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

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// JobSetValidator ensures JobSets opted-in for TPU provisioning provide all required metadata.
type JobSetValidator struct {
	client.Client
}

// SetupWebhookWithManager registers the validator with the Manager.
func (v *JobSetValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &jobsetv1alpha2.JobSet{}).
		WithValidator(v).
		Complete()
}

// +kubebuilder:webhook:path=/validate-jobset-x-k8s-io-v1alpha2-jobset,mutating=false,failurePolicy=fail,sideEffects=None,groups=jobset.x-k8s.io,resources=jobsets,verbs=create;update,versions=v1alpha2,name=vjobset.kb.io,admissionReviewVersions=v1

// ValidateCreate checks JobSet requirements on creation.
func (v *JobSetValidator) ValidateCreate(ctx context.Context, obj *jobsetv1alpha2.JobSet) (admission.Warnings, error) {
	return nil, v.validateJobSet(ctx, obj)
}

// ValidateUpdate checks JobSet requirements on update.
// It rejects changes to TPU-critical immutable labels on opted-in JobSets.
func (v *JobSetValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *jobsetv1alpha2.JobSet) (admission.Warnings, error) {
	if err := validateImmutableLabels(oldObj.GetLabels(), newObj.GetLabels()); err != nil {
		return nil, err
	}
	return nil, v.validateJobSet(ctx, newObj)
}

// ValidateDelete is a no-op for JobSet validation.
func (v *JobSetValidator) ValidateDelete(ctx context.Context, obj *jobsetv1alpha2.JobSet) (admission.Warnings, error) {
	return nil, nil
}

func (v *JobSetValidator) validateJobSet(ctx context.Context, jobSet *jobsetv1alpha2.JobSet) error {
	logger := log.FromContext(ctx).WithValues("jobset", client.ObjectKeyFromObject(jobSet))

	prefix := jobSet.GetLabels()[slicePrefixLabel]
	if prefix == "" {
		// JobSet is not opted-in for TPU auto-provisioning.
		return nil
	}

	logger.Info("Validating JobSet for TPU auto-provisioning configuration")

	// Ensure the requested slice-prefix is unique across the cluster.
	if err := v.validateSlicePrefixUniqueness(ctx, jobSet, prefix); err != nil {
		logger.Error(err, "Slice prefix collision detected")
		return err
	}

	for i, rj := range jobSet.Spec.ReplicatedJobs {
		rjName := rj.Name

		// Determine if this replicatedJob requests TPU resources.
		requestsTPU := false
		for _, c := range rj.Template.Spec.Template.Spec.Containers {
			if _, ok := c.Resources.Limits[corev1.ResourceName(tpuResourceName)]; ok {
				requestsTPU = true
				break
			}
		}

		if !requestsTPU {
			// Check initContainers as well.
			for _, c := range rj.Template.Spec.Template.Spec.InitContainers {
				if _, ok := c.Resources.Limits[corev1.ResourceName(tpuResourceName)]; ok {
					requestsTPU = true
					break
				}
			}
		}

		if requestsTPU {
			// ReplicatedJobs requesting TPUs MUST provide slice type and topology labels in the Pod template.
			labels := rj.Template.Labels

			_, hasType := labels[sliceTypeLabel]
			_, hasTopo := labels[sliceTopologyLabel]

			if !hasType || !hasTopo {
				return fmt.Errorf("replicatedJob[%d] (%s) requests TPUs but is missing required Job template labels: %s, %s", i, rjName, sliceTypeLabel, sliceTopologyLabel)
			}
		}
	}

	return nil
}

// validateSlicePrefixUniqueness checks that no other JobSet uses the same slice-prefix.
// NOTE: This is a best-effort check subject to TOCTOU races — two concurrent CREATE
// requests can both pass validation. The reconciler handles the resulting AlreadyExists
// error gracefully.
func (v *JobSetValidator) validateSlicePrefixUniqueness(ctx context.Context, jobSet *jobsetv1alpha2.JobSet, prefix string) error {
	jsList := &jobsetv1alpha2.JobSetList{}

	if err := v.List(ctx, jsList, client.MatchingLabels{slicePrefixLabel: prefix}); err != nil {
		return fmt.Errorf("failed to list JobSets: %v", err)
	}

	for _, existing := range jsList.Items {
		if existing.Name != jobSet.Name || existing.Namespace != jobSet.Namespace {
			return fmt.Errorf("slice-prefix %q is already used by JobSet %s/%s", prefix, existing.Namespace, existing.Name)
		}
	}

	return nil
}
