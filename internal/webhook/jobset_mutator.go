/*
Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// JobSetMutator handles JobSet suspension and Pod-level TPU Slice mapping.
type JobSetMutator struct {
	client.Client
}

// SetupWithManager registers the mutator for Jobs.
func (w *JobSetMutator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &batchv1.Job{}).
		WithDefaulter(w).
		Complete()
}

// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;update;patch
// +kubebuilder:webhook:path=/mutate-batch-v1-job,mutating=true,failurePolicy=fail,sideEffects=None,groups=batch,resources=jobs,verbs=create,versions=v1,name=mjob.kb.io,admissionReviewVersions=v1

// Default applies mutations to Jobs (slice name and Pod template injection).
func (w *JobSetMutator) Default(ctx context.Context, obj *batchv1.Job) error {
	return w.defaultJob(ctx, obj)
}

func (w *JobSetMutator) defaultJob(ctx context.Context, job *batchv1.Job) error {
	logger := log.FromContext(ctx).WithValues("job", client.ObjectKeyFromObject(job))

	labels := job.GetLabels()
	// Identify the parent JobSet to retrieve necessary metadata.
	jobSetName := labels[jobSetNameLabel]
	if jobSetName == "" {
		logger.V(1).Info("Job is not part of a JobSet, skipping mutation")
		return nil
	}

	jobSet := &jobsetv1alpha2.JobSet{}
	if err := w.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: jobSetName}, jobSet); err != nil {
		// NOTE: We return nil to avoid blocking Job creation. The reconciler will handle slice creation.
		logger.Info("Parent JobSet not found, skipping Job mutation", "error", err)
		return nil
	}

	// Retrieve the slice-prefix from the parent JobSet.
	slicePrefix := jobSet.GetLabels()[slicePrefixLabel]
	if slicePrefix == "" {
		logger.V(1).Info("Parent JobSet missing slice-prefix label, skipping Job mutation")
		return nil
	}

	// Dynamic Mode: propagate accelerator & topology into Pod template nodeSelector if present on Job or parent JobSet
	sliceType := job.Labels[sliceTypeLabel]
	if sliceType == "" && jobSet != nil {
		sliceType = jobSet.Labels[sliceTypeLabel]
	}
	if sliceType == "" {
		sliceType = job.Spec.Template.Labels[sliceTypeLabel]
	}

	sliceTopology := job.Labels[sliceTopologyLabel]
	if sliceTopology == "" && jobSet != nil {
		sliceTopology = jobSet.Labels[sliceTopologyLabel]
	}
	if sliceTopology == "" {
		sliceTopology = job.Spec.Template.Labels[sliceTopologyLabel]
	}

	if sliceType != "" || sliceTopology != "" {
		if job.Spec.Template.Spec.NodeSelector == nil {
			job.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}
		if sliceType != "" && job.Spec.Template.Spec.NodeSelector[sliceTypeLabel] == "" {
			job.Spec.Template.Spec.NodeSelector[sliceTypeLabel] = sliceType
		}
		if sliceTopology != "" && job.Spec.Template.Spec.NodeSelector[sliceTopologyLabel] == "" {
			job.Spec.Template.Spec.NodeSelector[sliceTopologyLabel] = sliceTopology
		}
	}

	// Extract replicatedjob-name and job-index from the Job's labels.
	replicatedJobName := labels[replicatedJobNameLabel]
	jobIndex := labels[jobIndexLabel]
	if replicatedJobName == "" || jobIndex == "" {
		logger.V(1).Info("Job missing required JobSet metadata labels, skipping mutation")
		return nil
	}

	// Construct the unique TPU Slice name: <prefix>-<replicatedjob-name>-<index>.
	// Truncation ensures the total length remains within Kubernetes limits (63 chars).
	sliceName := fmt.Sprintf("%.35s-%.20s-%.6s", slicePrefix, replicatedJobName, jobIndex)

	// Inject the mapped TPU Slice name into the Job's labels and annotations.
	if job.Labels == nil {
		job.Labels = make(map[string]string)
	}
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}
	job.Labels[sliceNameLabel] = sliceName
	if sliceType != "" {
		job.Labels[sliceTypeLabel] = sliceType
	}
	if sliceTopology != "" {
		job.Labels[sliceTopologyLabel] = sliceTopology
	}
	job.Annotations[sliceNameAnnotation] = sliceName

	// Propagate slice labels to Pod template labels.
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = make(map[string]string)
	}
	job.Spec.Template.Labels[sliceNameLabel] = sliceName
	if sliceType != "" {
		job.Spec.Template.Labels[sliceTypeLabel] = sliceType
	}
	if sliceTopology != "" {
		job.Spec.Template.Labels[sliceTopologyLabel] = sliceTopology
	}

	// Propagate slice-name to Pod template annotations.
	if job.Spec.Template.Annotations == nil {
		job.Spec.Template.Annotations = make(map[string]string)
	}
	job.Spec.Template.Annotations[sliceNameAnnotation] = sliceName

	// Direct the Pod to the provisioned TPU Slice via nodeSelector ONLY if schedulerName != "slice-scheduler".
	if job.Spec.Template.Spec.SchedulerName != "slice-scheduler" {
		if job.Spec.Template.Spec.NodeSelector == nil {
			job.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}
		job.Spec.Template.Spec.NodeSelector[sliceNameLabel] = sliceName
	}

	logger.Info("Injected TPU Slice name into Job and its Pod template", "sliceName", sliceName)

	return nil
}
