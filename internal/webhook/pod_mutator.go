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

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

// PodMutator assigns LWS leader Pods to the TPU Slice of their own group.
type PodMutator struct {
	client.Client
}

// SetupWebhookWithManager registers the mutator for Pods.
func (m *PodMutator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &corev1.Pod{}).
		WithDefaulter(m).
		Complete()
}

// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod.kb.io,admissionReviewVersions=v1

// Default sets the slice name on LWS leader Pods. One leader StatefulSet serves every
// group, so the name in its template is only correct for group 0; the Pod is the
// first object that knows which group it belongs to.
func (m *PodMutator) Default(ctx context.Context, pod *corev1.Pod) error {
	if pod.Labels[labels.LWSNameLabel] == "" || pod.Labels[slicePrefixLabel] == "" {
		return nil
	}
	// Worker Pods inherit the correct per-group name from their worker StatefulSet.
	if utils.GetLWSRole(pod.Annotations) != utils.LWSLeaderRole {
		return nil
	}

	sliceName := utils.LeaderPodSliceName(pod)
	if sliceName == "" {
		return nil
	}

	// Overwrite rather than skip when present: the value inherited from the leader template is stale for groups > 0.
	pod.Labels[sliceNameLabel] = sliceName
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[sliceNameAnnotation] = sliceName

	// Mirror the StatefulSet mutator: only Pods not scheduled by slice-scheduler are pinned via nodeSelector.
	if pod.Spec.SchedulerName != "slice-scheduler" {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		pod.Spec.NodeSelector[sliceNameLabel] = sliceName
	}

	log.FromContext(ctx).V(1).Info("Assigned LWS leader Pod to its group's TPU Slice", "pod", pod.Name, "sliceName", sliceName)
	return nil
}
