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
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

// StatefulSetValidator validates workloads (currently StatefulSet)
type StatefulSetValidator struct {
	client.Client
}

// SetupWebhookWithManager sets up the webhook with the Manager.
func (v *StatefulSetValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &appsv1.StatefulSet{}).
		WithValidator(v).
		Complete()
}

// +kubebuilder:webhook:path=/validate-apps-v1-statefulset,mutating=false,failurePolicy=fail,sideEffects=None,groups=apps,resources=statefulsets,verbs=create;update,versions=v1,name=vworkload.kb.io,admissionReviewVersions=v1

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type
func (v *StatefulSetValidator) ValidateCreate(ctx context.Context, obj *appsv1.StatefulSet) (admission.Warnings, error) {
	listExisting := v.lazyStatefulSetLister(ctx)
	if err := v.validateSliceName(ctx, obj, listExisting); err != nil {
		return nil, err
	}
	return nil, v.validateSlicePrefix(oldNil(), obj, listExisting)
}

func oldNil() *appsv1.StatefulSet { return nil }

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type.
// It rejects changes to TPU-critical immutable labels on opted-in StatefulSets and
// validates only Slice names newly introduced by the update, so pre-existing state
// never blocks unrelated updates such as finalizer removal.
func (v *StatefulSetValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *appsv1.StatefulSet) (admission.Warnings, error) {
	// Never block updates on objects being deleted (e.g. finalizer removal).
	if !newObj.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	// LeaderWorkerSet rewrites its StatefulSet templates during rolling updates
	// (including size and topology changes), StatefulSetMutator mirrors those
	// labels onto top-level StatefulSet labels for workers and LeaderExcluded
	// leaders, and StatefulSetReconciler recreates Slices on type/topology
	// changes. Only relax type/topology immutability when oldObj and newObj
	// belong to the same slice-prefix-managed LeaderWorkerSet; even then,
	// slice-prefix remains immutable and opted-in type/topology labels cannot
	// be removed.
	isLWSSlicePrefix := sameLWS(oldObj, newObj) && (oldObj.Spec.Template.Labels[slicePrefixLabel] != "" || oldObj.GetLabels()[slicePrefixLabel] != "")
	if !isLWSSlicePrefix {
		if err := validateImmutableLabels(oldObj.GetLabels(), newObj.GetLabels()); err != nil {
			return nil, err
		}
		if err := validateImmutableLabels(oldObj.Spec.Template.Labels, newObj.Spec.Template.Labels); err != nil {
			return nil, err
		}
	} else {
		if oldPfx := oldObj.GetLabels()[slicePrefixLabel]; oldPfx != "" && oldPfx != newObj.GetLabels()[slicePrefixLabel] {
			return nil, fmt.Errorf("label %q is immutable once set (was %q, got %q)", slicePrefixLabel, oldPfx, newObj.GetLabels()[slicePrefixLabel])
		}
		if oldPfx := oldObj.Spec.Template.Labels[slicePrefixLabel]; oldPfx != "" && oldPfx != newObj.Spec.Template.Labels[slicePrefixLabel] {
			return nil, fmt.Errorf("label %q is immutable once set (was %q, got %q)", slicePrefixLabel, oldPfx, newObj.Spec.Template.Labels[slicePrefixLabel])
		}
		for _, key := range []string{sliceTypeLabel, sliceTopologyLabel} {
			if oldVal := oldObj.Spec.Template.Labels[key]; oldVal != "" && newObj.Spec.Template.Labels[key] == "" {
				return nil, fmt.Errorf("label %q cannot be removed once set (was %q)", key, oldVal)
			}
			if oldVal := oldObj.GetLabels()[key]; oldVal != "" && newObj.GetLabels()[key] == "" && newObj.Spec.Template.Labels[key] == "" {
				return nil, fmt.Errorf("label %q cannot be removed once set (was %q)", key, oldVal)
			}
		}
	}

	listExisting := v.lazyStatefulSetLister(ctx)
	if effectiveSliceName(oldObj) != effectiveSliceName(newObj) {
		if err := v.validateSliceName(ctx, newObj, listExisting); err != nil {
			return nil, err
		}
	}
	if err := v.validateSlicePrefix(oldObj, newObj, listExisting); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type
func (v *StatefulSetValidator) ValidateDelete(ctx context.Context, obj *appsv1.StatefulSet) (admission.Warnings, error) {
	return nil, nil
}

func (v *StatefulSetValidator) lazyStatefulSetLister(ctx context.Context) func() ([]appsv1.StatefulSet, error) {
	var (
		loaded bool
		items  []appsv1.StatefulSet
		err    error
	)
	return func() ([]appsv1.StatefulSet, error) {
		if loaded {
			return items, err
		}
		loaded = true
		stsList := &appsv1.StatefulSetList{}
		if listErr := v.List(ctx, stsList, client.UnsafeDisableDeepCopy); listErr != nil {
			err = fmt.Errorf("listing StatefulSets: %w", listErr)
			return nil, err
		}
		items = stsList.Items
		return items, nil
	}
}

func sameLWS(a, b *appsv1.StatefulSet) bool {
	if a.Namespace != b.Namespace {
		return false
	}
	lwsA := utils.LWSName(a)
	return lwsA != "" && lwsA == utils.LWSName(b)
}

func effectiveSliceName(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return ""
	}
	if sName := sts.GetLabels()[sliceNameLabel]; sName != "" {
		return sName
	}
	if sts.Spec.Template.Labels[slicePrefixLabel] == "" {
		return sts.Spec.Template.Labels[sliceNameLabel]
	}
	return ""
}

func claimedSliceNames(sts *appsv1.StatefulSet) ([]string, error) {
	spec, err := utils.ResolveStatefulSetSlices(sts)
	if err != nil {
		return nil, err
	}
	claimed := slices.Clone(spec.DesiredNames)
	prefix := sts.Spec.Template.Labels[slicePrefixLabel]
	if utils.LWSName(sts) != "" && utils.LWSRole(sts) == utils.LWSLeaderRole && sts.GetLabels()[labels.LWSGroupIndexLabel] == "" && prefix != "" {
		for _, sName := range utils.LWSLeaderSliceNames(sts, prefix) {
			if !slices.Contains(claimed, sName) {
				claimed = append(claimed, sName)
			}
		}
	}
	return claimed, nil
}

// validateSliceName ensures no other StatefulSet claims the same slice-name.
// NOTE: This does NOT check Jobs or JobSets — cross-resource slice-name collisions
// are possible. The Slice CR itself is cluster-scoped with a unique name, so the
// reconciler's Create call will return AlreadyExists if there's a collision.
func (v *StatefulSetValidator) validateSliceName(ctx context.Context, obj *appsv1.StatefulSet, listExisting func() ([]appsv1.StatefulSet, error)) error {
	logger := log.FromContext(ctx)
	sliceName := effectiveSliceName(obj)
	if sliceName == "" {
		return nil
	}

	logger.Info("Validating slice-name label uniqueness", "name", obj.GetName(), "sliceName", sliceName)

	existingItems, err := listExisting()
	if err != nil {
		return err
	}

	for i := range existingItems {
		existing := &existingItems[i]
		if existing.Name == obj.GetName() && existing.Namespace == obj.GetNamespace() {
			continue
		}
		if sameLWS(obj, existing) {
			continue
		}
		if effectiveSliceName(existing) == sliceName {
			return fmt.Errorf("slice-name %q is already used by StatefulSet %s/%s", sliceName, existing.Namespace, existing.Name)
		}
		existingNames, err := claimedSliceNames(existing)
		if err == nil && slices.Contains(existingNames, sliceName) {
			return fmt.Errorf("slice-name %q is already used by StatefulSet %s/%s", sliceName, existing.Namespace, existing.Name)
		}
	}

	return nil
}

// validateSlicePrefix ensures that LWS StatefulSets from distinct LeaderWorkerSets
// do not claim the same slice-prefix (nor collide with Slice names derived by
// existing StatefulSets), and that derived Slice names fit RFC 1123 limits.
// On update (oldObj != nil), it ratchets: the prefix uniqueness check runs only
// when the prefix or owning LeaderWorkerSet changes, and name checks run only
// for Slice names newly introduced by the update (e.g. on scale-up).
func (v *StatefulSetValidator) validateSlicePrefix(oldObj, obj *appsv1.StatefulSet, listExisting func() ([]appsv1.StatefulSet, error)) error {
	prefix := obj.Spec.Template.Labels[slicePrefixLabel]
	if prefix == "" {
		return nil
	}

	claimedNames, err := claimedSliceNames(obj)
	if err != nil {
		return err
	}

	checkPrefix := true
	newNames := claimedNames
	if oldObj != nil {
		checkPrefix = oldObj.Spec.Template.Labels[slicePrefixLabel] != prefix || utils.LWSName(oldObj) != utils.LWSName(obj)
		if oldNames, oldErr := claimedSliceNames(oldObj); oldErr == nil {
			newNames = make([]string, 0, len(claimedNames))
			for _, sName := range claimedNames {
				if !slices.Contains(oldNames, sName) {
					newNames = append(newNames, sName)
				}
			}
		}
	}
	if !checkPrefix && len(newNames) == 0 {
		return nil
	}

	if checkPrefix && len(newNames) == 0 {
		if err := validateDerivedSliceName(prefix + "-0"); err != nil {
			return err
		}
	}
	for _, sName := range newNames {
		if err := validateDerivedSliceName(sName); err != nil {
			return err
		}
	}

	existingItems, err := listExisting()
	if err != nil {
		return err
	}

	for i := range existingItems {
		existing := &existingItems[i]
		if existing.Name == obj.GetName() && existing.Namespace == obj.GetNamespace() {
			continue
		}
		if sameLWS(obj, existing) {
			continue
		}
		if checkPrefix && existing.Spec.Template.Labels[slicePrefixLabel] == prefix {
			return fmt.Errorf("slice-prefix %q is already used by StatefulSet %s/%s", prefix, existing.Namespace, existing.Name)
		}
		existingClaimed, err := claimedSliceNames(existing)
		for _, sName := range newNames {
			if effectiveSliceName(existing) == sName || (err == nil && slices.Contains(existingClaimed, sName)) {
				return fmt.Errorf("derived slice-name %q is already used by StatefulSet %s/%s", sName, existing.Namespace, existing.Name)
			}
		}
	}

	return nil
}

func validateDerivedSliceName(sName string) error {
	if len(sName) > validation.DNS1123LabelMaxLength {
		return fmt.Errorf("derived slice name %q exceeds %d characters", sName, validation.DNS1123LabelMaxLength)
	}
	if errs := validation.IsDNS1123Label(sName); len(errs) > 0 {
		return fmt.Errorf("derived slice name %q is not a valid RFC 1123 label: %s", sName, strings.Join(errs, "; "))
	}
	return nil
}
