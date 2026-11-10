// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

const (
	// Kueue Workload condition types
	WorkloadEvicted       = "Evicted"
	WorkloadPreempted     = "Preempted"
	WorkloadAdmitted      = "Admitted"
	WorkloadQuotaReserved = "QuotaReserved"

	// Kinds
	KindStatefulSet     = "StatefulSet"
	KindPod             = "Pod"
	KindLeaderWorkerSet = "LeaderWorkerSet"
	KindJob             = "Job"
	KindJobSet          = "JobSet"
)

var (
	WorkloadGroupVersion = schema.GroupVersion{Group: "kueue.x-k8s.io", Version: "v1beta2"}
)

// Workload is a lightweight, trimmed representation of kueue.x-k8s.io/v1beta2 Workload CRD.
// By omitting the Spec field, unmarshaling and caching consume minimal Informer memory (~150 bytes per CR).
type Workload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Status            WorkloadStatus `json:"status,omitempty"`
}

func (w *Workload) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := new(Workload)
	w.DeepCopyInto(out)
	return out
}

func (w *Workload) DeepCopyInto(out *Workload) {
	*out = *w
	out.TypeMeta = w.TypeMeta
	w.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	w.Status.DeepCopyInto(&out.Status)
}

type WorkloadStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func (in *WorkloadStatus) DeepCopyInto(out *WorkloadStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

type WorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workload `json:"items"`
}

func (l *WorkloadList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := new(WorkloadList)
	l.DeepCopyInto(out)
	return out
}

func (l *WorkloadList) DeepCopyInto(out *WorkloadList) {
	*out = *l
	out.TypeMeta = l.TypeMeta
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	if l.Items != nil {
		in, out := &l.Items, &out.Items
		*out = make([]Workload, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func AddWorkloadToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(WorkloadGroupVersion, &Workload{}, &WorkloadList{})
	metav1.AddToGroupVersion(s, WorkloadGroupVersion)
	return nil
}

// IsWorkloadAdmitted checks if the Kueue Workload is Admitted or QuotaReserved, and not Evicted/Preempted.
func IsWorkloadAdmitted(wl *Workload) bool {
	if wl == nil {
		return false
	}
	if meta.IsStatusConditionTrue(wl.Status.Conditions, WorkloadEvicted) ||
		meta.IsStatusConditionTrue(wl.Status.Conditions, WorkloadPreempted) {
		return false
	}
	return meta.IsStatusConditionTrue(wl.Status.Conditions, WorkloadAdmitted) ||
		meta.IsStatusConditionTrue(wl.Status.Conditions, WorkloadQuotaReserved)
}

func WorkloadAdmissionPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return false },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldWl, ok1 := e.ObjectOld.(*Workload)
			newWl, ok2 := e.ObjectNew.(*Workload)
			if !ok1 || !ok2 {
				return false
			}
			return IsWorkloadAdmitted(oldWl) != IsWorkloadAdmitted(newWl)
		},
	}
}

// isOwner checks if the given owner reference matches the parentName.
// If the owner kind is "Pod", we assume StatefulSet ordinal naming convention (<sts-name>-<ordinal>).
// Truncating the last hyphen is safe because StatefulSet ordinals are always at the end, even if the
// StatefulSet name itself contains hyphens and numbers (e.g., "my-sts-2" pod "my-sts-2-0" is correctly
// truncated to "my-sts-2").
func isOwner(owner metav1.OwnerReference, parentName string) bool {
	ownerName := owner.Name
	if owner.Kind == KindPod {
		if idx := strings.LastIndex(ownerName, "-"); idx > 0 {
			ownerName = ownerName[:idx]
		}
	}
	return ownerName == parentName
}

// isOwnerForKinds reports whether owner matches parentName and (when non-empty)
// one of allowedKinds.
func isOwnerForKinds(owner metav1.OwnerReference, parentName string, allowedKinds ...string) bool {
	if len(allowedKinds) > 0 && !slices.Contains(allowedKinds, owner.Kind) {
		return false
	}
	return isOwner(owner, parentName)
}

// IsWorkloadAdmittedForParent checks if any Kueue Workload owned by parentName
// (restricted to allowedKinds when provided) is Admitted.
// If queueName is non-empty, we expect a Workload CR to be created by Kueue. If no Workload CR
// is found yet, we return false to wait for its creation and avoid the timing window race.
func IsWorkloadAdmittedForParent(ctx context.Context, c client.Client, ns string, parentName string, queueName string, allowedKinds ...string) (bool, error) {
	if queueName == "" {
		return true, nil
	}
	var wlList WorkloadList
	if err := c.List(ctx, &wlList, client.InNamespace(ns)); err != nil {
		if meta.IsNoMatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("listing Kueue Workloads in namespace %s: %w", ns, err)
	}
	for _, wl := range wlList.Items {
		for _, owner := range wl.OwnerReferences {
			if isOwnerForKinds(owner, parentName, allowedKinds...) && IsWorkloadAdmitted(&wl) {
				return true, nil
			}
		}
	}
	return false, nil
}

// lwsWorkloadGroupIndex extracts the LWS group index from a Kueue Workload's
// component-workload-index annotation or from its generated name
// ("leaderworkerset-<lwsName>-<groupIndex>-<hash>").
func lwsWorkloadGroupIndex(wl *Workload, lwsName string) (int, bool) {
	if idxStr := wl.Annotations[labels.KueueComponentWorkloadIndexAnnotation]; idxStr != "" {
		idx, err := strconv.Atoi(idxStr)
		return idx, err == nil && idx >= 0
	}
	if lwsName == "" {
		return 0, false
	}
	rest, ok := strings.CutPrefix(wl.Name, "leaderworkerset-"+lwsName+"-")
	if !ok {
		return 0, false
	}
	hashSep := strings.LastIndex(rest, "-")
	if hashSep <= 0 {
		return 0, false
	}
	idxStr := rest[:hashSep]
	if idxStr == "" || (len(idxStr) > 1 && idxStr[0] == '0') {
		return 0, false
	}
	for i := range len(idxStr) {
		if idxStr[i] < '0' || idxStr[i] > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(idxStr)
	return idx, err == nil && idx >= 0
}

// AdmittedLWSGroups evaluates Kueue admission independently for LWS replica
// groups [0, numGroups) in a single WorkloadList lookup. When Kueue creates
// per-group Workloads (via component-workload-index annotation, LWS-owned
// indexed Workload name, or Pod-owned Workload), each group's admission state
// is returned independently so unadmitted or evicted replicas neither block nor
// over-provision other replicas. If only a single unindexed parent Workload
// exists, all groups inherit that parent Workload's admission state.
func AdmittedLWSGroups(ctx context.Context, c client.Client, ns, lwsName, stsName string, numGroups int, queueName string) ([]bool, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	admitted := make([]bool, numGroups)
	if queueName == "" {
		for i := range admitted {
			admitted[i] = true
		}
		return admitted, nil
	}

	var wlList WorkloadList
	if err := c.List(ctx, &wlList, client.InNamespace(ns)); err != nil {
		if meta.IsNoMatchError(err) {
			for i := range admitted {
				admitted[i] = true
			}
			return admitted, nil
		}
		return nil, fmt.Errorf("listing Kueue Workloads in namespace %s: %w", ns, err)
	}

	hasIndexedWorkloads := false
	unindexedWorkloads := 0
	unindexedAdmitted := false

	for i := range wlList.Items {
		indexed, unindexed, wlAdmitted := recordLWSWorkloadGroupAdmission(&wlList.Items[i], admitted, lwsName, stsName)
		hasIndexedWorkloads = hasIndexedWorkloads || indexed
		if unindexed {
			unindexedWorkloads++
		}
		unindexedAdmitted = unindexedAdmitted || wlAdmitted
	}

	if !hasIndexedWorkloads && unindexedWorkloads == 1 && unindexedAdmitted {
		for i := range admitted {
			admitted[i] = true
		}
	}
	return admitted, nil
}

func recordLWSWorkloadGroupAdmission(wl *Workload, admitted []bool, lwsName, stsName string) (hasIndexed, foundUnindexed, unindexedAdmitted bool) {
	numGroups := len(admitted)
	for _, owner := range wl.OwnerReferences {
		if grpIdx, matchedPod := podOwnedLWSGroupIndex(owner, lwsName, stsName); matchedPod {
			hasIndexed = true
			if grpIdx >= 0 && grpIdx < numGroups {
				admitted[grpIdx] = admitted[grpIdx] || IsWorkloadAdmitted(wl)
			}
			continue
		}
		if (owner.Kind == KindLeaderWorkerSet && lwsName != "" && owner.Name == lwsName) ||
			(owner.Kind == KindStatefulSet && stsName != "" && owner.Name == stsName) {
			if idx, ok := lwsWorkloadGroupIndex(wl, lwsName); ok {
				hasIndexed = true
				if idx < numGroups {
					admitted[idx] = admitted[idx] || IsWorkloadAdmitted(wl)
				}
				continue
			}
			foundUnindexed = true
			if IsWorkloadAdmitted(wl) {
				unindexedAdmitted = true
			}
		}
	}
	return hasIndexed, foundUnindexed, unindexedAdmitted
}

func podOwnedLWSGroupIndex(owner metav1.OwnerReference, lwsName, stsName string) (int, bool) {
	if owner.Kind != KindPod {
		return -1, false
	}
	if stsName != "" && stsName != lwsName && isOwner(owner, stsName) {
		if podOrd, ok := ParseTrailingIndex(owner.Name); ok && owner.Name == stsName+"-"+podOrd {
			if grpStr, ok := ParseTrailingIndex(stsName); ok {
				if grpIdx, err := strconv.Atoi(grpStr); err == nil && grpIdx >= 0 {
					return grpIdx, true
				}
			}
		}
		return -1, true
	}
	if isOwner(owner, lwsName) {
		if idxStr, ok := ParseTrailingIndex(owner.Name); ok {
			if idx, err := strconv.Atoi(idxStr); err == nil && idx >= 0 && owner.Name == fmt.Sprintf("%s-%d", lwsName, idx) {
				return idx, true
			}
		}
		return -1, true
	}
	return -1, false
}

// IsLWSGroupWorkloadAdmitted reports whether a single LWS replica group's Kueue
// Workload is admitted.
func IsLWSGroupWorkloadAdmitted(ctx context.Context, c client.Client, ns, lwsName, stsName string, groupIndex int, queueName string) (bool, error) {
	if groupIndex < 0 {
		return false, nil
	}
	groups, err := AdmittedLWSGroups(ctx, c, ns, lwsName, stsName, groupIndex+1, queueName)
	if err != nil {
		return false, err
	}
	return groups[groupIndex], nil
}

// IsForeignSliceOwner reports whether slice carries non-empty owner metadata
// pointing to a workload different from (ownerNamespace, ownerName, ownerKind).
func IsForeignSliceOwner(slice *slicev1beta1.Slice, ownerNamespace, ownerName, ownerKind string) bool {
	if slice == nil {
		return false
	}
	if k := slice.Labels[labels.SliceOwnerKindLabel]; k != "" && ownerKind != "" && k != ownerKind {
		return true
	}
	if n := slice.Labels[labels.SliceOwnerNameLabel]; n != "" && ownerName != "" && n != ownerName {
		return true
	}
	if ns := slice.Labels[labels.SliceOwnerNamespaceLabel]; ns != "" && ownerNamespace != "" && ns != ownerNamespace {
		return true
	}
	return false
}

// IsSliceOwnedBy reports whether slice is positively tagged as owned by
// (ownerNamespace, ownerName, ownerKind), allowing legacy empty namespace/kind
// labels on slice when slice-owner-name matches.
func IsSliceOwnedBy(slice *slicev1beta1.Slice, ownerNamespace, ownerName, ownerKind string) bool {
	if slice == nil || ownerName == "" {
		return false
	}
	if slice.Labels[labels.SliceOwnerNameLabel] != ownerName {
		return false
	}
	if ns := slice.Labels[labels.SliceOwnerNamespaceLabel]; ns != "" && ns != ownerNamespace {
		return false
	}
	if k := slice.Labels[labels.SliceOwnerKindLabel]; k != "" && k != ownerKind {
		return false
	}
	return true
}

// CleanupUnadmittedSlice deletes the Slice CR associated with sliceName if it is not admitted.
func CleanupUnadmittedSlice(ctx context.Context, c client.Client, sliceName string) (ctrl.Result, error) {
	return CleanupUnadmittedSliceForOwner(ctx, c, sliceName, "", "", "")
}

// CleanupUnadmittedSliceForOwner deletes the Slice CR associated with sliceName
// when unadmitted, skipping deletion if the Slice is owned by a different workload.
func CleanupUnadmittedSliceForOwner(ctx context.Context, c client.Client, sliceName, ownerNamespace, ownerName, ownerKind string) (ctrl.Result, error) {
	if sliceName == "" {
		return ctrl.Result{}, nil
	}
	slice := &slicev1beta1.Slice{}
	if err := c.Get(ctx, client.ObjectKey{Name: sliceName}, slice); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching Slice %s for cleanup: %w", sliceName, err)
	}
	if IsForeignSliceOwner(slice, ownerNamespace, ownerName, ownerKind) {
		return ctrl.Result{}, nil
	}
	if slice.DeletionTimestamp.IsZero() {
		if err := c.Delete(ctx, slice); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting Slice %s for cleanup: %w", sliceName, err)
		}
	}
	return ctrl.Result{}, nil
}
