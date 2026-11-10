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

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

// StatefulSetMutator handles StatefulSet TPU Slice mapping.
type StatefulSetMutator struct {
	client.Client
}

// SetupWebhookWithManager registers the mutator for StatefulSets.
func (m *StatefulSetMutator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &appsv1.StatefulSet{}).
		WithDefaulter(m).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-apps-v1-statefulset,mutating=true,failurePolicy=fail,sideEffects=None,groups=apps,resources=statefulsets,verbs=create;update,versions=v1,name=mstatefulset.kb.io,admissionReviewVersions=v1

// Default applies mutations to StatefulSets (slice name propagation to Pod template).
func (m *StatefulSetMutator) Default(ctx context.Context, sts *appsv1.StatefulSet) error {

	logger := log.FromContext(ctx).WithValues("statefulset", client.ObjectKeyFromObject(sts))

	sliceName, sliceType, sliceTopology, err := utils.SliceRequirements(sts)
	if err != nil {
		logger.Error(err, "invalid TPU Slice requirements, rejecting admission")
		return err
	}

	// Skip slice-name mutation for non-LeaderExcluded LWS Leader StatefulSets
	// if sliceNameLabel was ALREADY stamped by <= 0.6.0 to avoid triggering
	// rolling update cascades, while still keeping type/topology nodeSelectors
	// in sync if a leaderTemplate rollout updated them via Server-Side Apply.
	if isLegacyStampedLWSLeader(sts) {
		propagateSliceTypeAndTopologyToNodeSelector(sts, sliceType, sliceTopology)
		return nil
	}

	// Skip mutation if already present to prevent triggering endless controller-revision-hash updates
	if statefulSetAlreadyMutated(sts, sliceName, sliceType, sliceTopology) {
		return nil
	}

	// Automatically propagate accelerator & topology into Pod template nodeSelector if present on workload
	propagateSliceTypeAndTopologyToNodeSelector(sts, sliceType, sliceTopology)

	if sliceName == "" {
		if utils.LWSName(sts) != "" {
			delete(sts.Labels, sliceNameLabel)
			delete(sts.Labels, sliceTypeLabel)
			delete(sts.Labels, sliceTopologyLabel)
		}
		return nil
	}

	// Propagate slice labels to top-level StatefulSet labels.
	if sts.Labels == nil {
		sts.Labels = make(map[string]string)
	}
	sts.Labels[sliceNameLabel] = sliceName
	if sliceType != "" {
		sts.Labels[sliceTypeLabel] = sliceType
	}
	if sliceTopology != "" {
		sts.Labels[sliceTopologyLabel] = sliceTopology
	}

	// Propagate slice name annotation to Pod template annotations.
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations[sliceNameAnnotation] = sliceName

	// Propagate slice labels to Pod template labels.
	if sts.Spec.Template.Labels == nil {
		sts.Spec.Template.Labels = make(map[string]string)
	}
	sts.Spec.Template.Labels[sliceNameLabel] = sliceName
	if sliceType != "" {
		sts.Spec.Template.Labels[sliceTypeLabel] = sliceType
	}
	if sliceTopology != "" {
		sts.Spec.Template.Labels[sliceTopologyLabel] = sliceTopology
	}

	// Direct the Pod to the provisioned TPU Slice via nodeSelector ONLY if schedulerName != "slice-scheduler".
	if sts.Spec.Template.Spec.SchedulerName != "slice-scheduler" {
		if sts.Spec.Template.Spec.NodeSelector == nil {
			sts.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}
		sts.Spec.Template.Spec.NodeSelector[sliceNameLabel] = sliceName
		logger.Info("Injected TPU Slice nodeSelector into StatefulSet Pod template for non-slice-scheduler workload", "sliceName", sliceName, "schedulerName", sts.Spec.Template.Spec.SchedulerName)
	} else {
		logger.Info("Skipped TPU Slice nodeSelector injection for slice-scheduler workload", "sliceName", sliceName)
	}

	return nil
}

func isLegacyStampedLWSLeader(sts *appsv1.StatefulSet) bool {
	return utils.LWSName(sts) != "" &&
		utils.LWSRole(sts) == utils.LWSLeaderRole &&
		sts.Spec.Template.Labels != nil &&
		sts.Spec.Template.Labels[sliceNameLabel] != "" &&
		sts.Spec.Template.Annotations[labels.LWSSubgroupPolicyTypeAnnotation] != utils.LWSLeaderExcludedPolicy
}

func statefulSetAlreadyMutated(sts *appsv1.StatefulSet, sliceName, sliceType, sliceTopology string) bool {
	if sliceName == "" && utils.LWSName(sts) != "" &&
		(sts.Labels[sliceNameLabel] != "" || sts.Labels[sliceTypeLabel] != "" || sts.Labels[sliceTopologyLabel] != "") {
		return false
	}
	hasType := sliceType == "" || (sts.Spec.Template.Spec.NodeSelector != nil && sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel] == sliceType)
	hasTopology := sliceTopology == "" || (sts.Spec.Template.Spec.NodeSelector != nil && sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel] == sliceTopology)
	hasName := sliceName == "" || (sts.Spec.Template.Labels != nil && sts.Spec.Template.Labels[sliceNameLabel] == sliceName && sts.Spec.Template.Annotations != nil && sts.Spec.Template.Annotations[sliceNameAnnotation] == sliceName)
	return hasType && hasTopology && hasName
}

func propagateSliceTypeAndTopologyToNodeSelector(sts *appsv1.StatefulSet, sliceType, sliceTopology string) {
	if sliceType == "" && sliceTopology == "" {
		return
	}
	if sts.Spec.Template.Spec.NodeSelector == nil {
		sts.Spec.Template.Spec.NodeSelector = make(map[string]string)
	}
	overwriteLWS := utils.LWSName(sts) != "" && sts.Spec.Template.Labels[slicePrefixLabel] != ""
	if sliceType != "" && (sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel] == "" || overwriteLWS) {
		sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel] = sliceType
	}
	if sliceTopology != "" && (sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel] == "" || overwriteLWS) {
		sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel] = sliceTopology
	}
}
