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
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

func newKueueTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sc := runtime.NewScheme()
	if err := AddWorkloadToScheme(sc); err != nil {
		t.Fatalf("AddWorkloadToScheme() = %v, want nil", err)
	}
	if err := slicev1beta1.AddToScheme(sc); err != nil {
		t.Fatalf("slicev1beta1.AddToScheme() = %v, want nil", err)
	}
	return sc
}

func TestIsWorkloadAdmitted(t *testing.T) {
	tests := []struct {
		name string
		wl   *Workload
		want bool
	}{
		{
			name: "nil_workload",
			wl:   nil,
			want: false,
		},
		{
			name: "admitted_true",
			wl: &Workload{
				Status: WorkloadStatus{
					Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}},
				},
			},
			want: true,
		},
		{
			name: "quota_reserved_true",
			wl: &Workload{
				Status: WorkloadStatus{
					Conditions: []metav1.Condition{{Type: WorkloadQuotaReserved, Status: metav1.ConditionTrue}},
				},
			},
			want: true,
		},
		{
			name: "evicted_overrides_admitted",
			wl: &Workload{
				Status: WorkloadStatus{
					Conditions: []metav1.Condition{
						{Type: WorkloadEvicted, Status: metav1.ConditionTrue},
						{Type: WorkloadAdmitted, Status: metav1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "preempted_false",
			wl: &Workload{
				Status: WorkloadStatus{
					Conditions: []metav1.Condition{{Type: WorkloadPreempted, Status: metav1.ConditionTrue}},
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsWorkloadAdmitted(tc.wl); got != tc.want {
				t.Errorf("IsWorkloadAdmitted(%v) = %v, want %v", tc.wl, got, tc.want)
			}
		})
	}
}

func TestWorkloadAdmissionPredicate(t *testing.T) {
	pred := WorkloadAdmissionPredicate()
	if got := pred.Create(event.CreateEvent{}); got {
		t.Errorf("pred.Create(empty) = %v, want false", got)
	}
	if got := pred.Delete(event.DeleteEvent{}); got {
		t.Errorf("pred.Delete(empty) = %v, want false", got)
	}
	if got := pred.Generic(event.GenericEvent{}); got {
		t.Errorf("pred.Generic(empty) = %v, want false", got)
	}

	oldWl := &Workload{Status: WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionFalse}}}}
	newWl := &Workload{Status: WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}}}
	if got := pred.Update(event.UpdateEvent{ObjectOld: oldWl, ObjectNew: newWl}); !got {
		t.Errorf("pred.Update(admitted false -> true) = %v, want true", got)
	}
	if got := pred.Update(event.UpdateEvent{ObjectOld: oldWl, ObjectNew: oldWl}); got {
		t.Errorf("pred.Update(unchanged admission) = %v, want false", got)
	}
	if got := pred.Update(event.UpdateEvent{ObjectOld: nil, ObjectNew: newWl}); got {
		t.Errorf("pred.Update(nil ObjectOld) = %v, want false", got)
	}
}

func TestWorkloadDeepCopy(t *testing.T) {
	var nilWl *Workload
	if got := nilWl.DeepCopyObject(); got != nil {
		t.Errorf("(*Workload)(nil).DeepCopyObject() = %v, want nil", got)
	}
	var nilList *WorkloadList
	if got := nilList.DeepCopyObject(); got != nil {
		t.Errorf("(*WorkloadList)(nil).DeepCopyObject() = %v, want nil", got)
	}

	list := &WorkloadList{
		Items: []Workload{{
			ObjectMeta: metav1.ObjectMeta{Name: "wl-0", Namespace: "default"},
			Status:     WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}},
		}},
	}
	copied, ok := list.DeepCopyObject().(*WorkloadList)
	if !ok || len(copied.Items) != 1 || copied.Items[0].Name != "wl-0" {
		t.Fatalf("list.DeepCopyObject() = %+v, want *WorkloadList with 1 item named wl-0", copied)
	}
	copied.Items[0].Status.Conditions[0].Status = metav1.ConditionFalse
	if got := list.Items[0].Status.Conditions[0].Status; got != metav1.ConditionTrue {
		t.Errorf("list.Items[0].Status.Conditions[0].Status after mutating DeepCopyObject() = %v, want %v", got, metav1.ConditionTrue)
	}
}

func TestAdmittedLWSGroups(t *testing.T) {
	sc := newKueueTestScheme(t)

	admittedCond := []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}
	evictedCond := []metav1.Condition{
		{Type: WorkloadAdmitted, Status: metav1.ConditionTrue},
		{Type: WorkloadEvicted, Status: metav1.ConditionTrue},
	}

	tests := []struct {
		name      string
		lwsName   string
		stsName   string
		numGroups int
		queueName string
		workloads []client.Object
		want      []bool
	}{
		{
			name:      "zero_groups_returns_nil",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 0,
			queueName: "q",
			want:      nil,
		},
		{
			name:      "empty_queue_admits_all_groups",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 3,
			queueName: "",
			want:      []bool{true, true, true},
		},
		{
			name:      "no_workloads_yet_holds_all_groups",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 2,
			queueName: "q",
			want:      []bool{false, false},
		},
		{
			name:      "generated_lws_workload_name_admits_only_matching_group",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "leaderworkerset-my-lws-0-abcd1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "my-lws"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{true, false},
		},
		{
			name:      "numeric_suffixed_lws_name_unindexed_parent_admits_all_groups",
			lwsName:   "llama-3",
			stsName:   "llama-3",
			numGroups: 4,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "leaderworkerset-llama-3-abcd1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "llama-3"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{true, true, true, true},
		},
		{
			name:      "numeric_suffixed_lws_name_indexed_workload_admits_matching_group",
			lwsName:   "llama-3",
			stsName:   "llama-3",
			numGroups: 4,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "leaderworkerset-llama-3-2-abcd1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "llama-3"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{false, false, true, false},
		},
		{
			name:      "component_workload_index_annotation_admits_matching_group",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "custom-wl-name",
						Namespace:       "default",
						Annotations:     map[string]string{labels.KueueComponentWorkloadIndexAnnotation: "1"},
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "my-lws"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{false, true},
		},
		{
			name:      "pod_owned_workloads_handle_partial_eviction",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-my-lws-0-hash",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindPod, Name: "my-lws-0"}},
					},
					Status: WorkloadStatus{Conditions: evictedCond},
				},
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-my-lws-1-hash",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindPod, Name: "my-lws-1"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{false, true},
		},
		{
			name:      "worker_pod_owned_workload_maps_to_worker_statefulset_group_index",
			lwsName:   "my-lws",
			stsName:   "my-lws-0",
			numGroups: 1,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-my-lws-0-1-hash",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindPod, Name: "my-lws-0-1"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{true},
		},
		{
			name:      "unindexed_parent_workload_fallback_admits_all_groups",
			lwsName:   "my-lws",
			stsName:   "my-lws",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lws-parent-wl",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "my-lws"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{true, true},
		},
		{
			name:      "multiple_unindexed_short_named_workloads_do_not_over_admit_all_groups",
			lwsName:   "very-long-leaderworkerset-name-whose-index-gets-truncated",
			stsName:   "very-long-leaderworkerset-name-whose-index-gets-truncated",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "leaderworkerset-very-long-leaderworkerset-name-whose-hash0",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "very-long-leaderworkerset-name-whose-index-gets-truncated"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "leaderworkerset-very-long-leaderworkerset-name-whose-hash1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "very-long-leaderworkerset-name-whose-index-gets-truncated"}},
					},
				},
			},
			want: []bool{false, false},
		},
		{
			name:      "foreign_kind_owner_with_same_name_does_not_admit_lws",
			lwsName:   "my-lws",
			stsName:   "my-lws-0",
			numGroups: 2,
			queueName: "q",
			workloads: []client.Object{
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "job-my-lws-abcd1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindJob, Name: "my-lws"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
				&Workload{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "jobset-my-lws-0-abcd1",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{Kind: KindJobSet, Name: "my-lws-0"}},
					},
					Status: WorkloadStatus{Conditions: admittedCond},
				},
			},
			want: []bool{false, false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(sc).WithObjects(tc.workloads...).Build()
			got, err := AdmittedLWSGroups(t.Context(), c, "default", tc.lwsName, tc.stsName, tc.numGroups, tc.queueName)
			if err != nil {
				t.Fatalf("AdmittedLWSGroups(%q, %q, %d, %q) error = %v, want nil", tc.lwsName, tc.stsName, tc.numGroups, tc.queueName, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("AdmittedLWSGroups(%q, %q, %d, %q) = %v, want %v", tc.lwsName, tc.stsName, tc.numGroups, tc.queueName, got, tc.want)
			}
		})
	}
}

func TestIsLWSGroupWorkloadAdmitted(t *testing.T) {
	sc := newKueueTestScheme(t)
	wlGroup0 := &Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "leaderworkerset-my-lws-0-abcd1",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "my-lws"}},
		},
		Status: WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}},
	}
	wlWorkerGroup2 := &Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pod-my-lws-2-1-abcd1",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: KindPod, Name: "my-lws-2-1"}},
		},
		Status: WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}},
	}
	c := fake.NewClientBuilder().WithScheme(sc).WithObjects(wlGroup0, wlWorkerGroup2).Build()

	tests := []struct {
		name       string
		stsName    string
		groupIndex int
		want       bool
	}{
		{name: "negative_group_index", stsName: "my-lws-0", groupIndex: -1, want: false},
		{name: "admitted_group_0", stsName: "my-lws-0", groupIndex: 0, want: true},
		{name: "unadmitted_group_1", stsName: "my-lws-1", groupIndex: 1, want: false},
		{name: "admitted_worker_pod_group_2", stsName: "my-lws-2", groupIndex: 2, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := IsLWSGroupWorkloadAdmitted(t.Context(), c, "default", "my-lws", tc.stsName, tc.groupIndex, "q")
			if err != nil {
				t.Fatalf("IsLWSGroupWorkloadAdmitted(%q, %d) error = %v, want nil", tc.stsName, tc.groupIndex, err)
			}
			if got != tc.want {
				t.Errorf("IsLWSGroupWorkloadAdmitted(%q, %d) = %v, want %v", tc.stsName, tc.groupIndex, got, tc.want)
			}
		})
	}
}

func TestIsWorkloadAdmittedForParentKindFiltering(t *testing.T) {
	sc := newKueueTestScheme(t)
	admittedJobSetWl := &Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "jobset-train-abcd1",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: KindJobSet, Name: "train"}},
		},
		Status: WorkloadStatus{Conditions: []metav1.Condition{{Type: WorkloadAdmitted, Status: metav1.ConditionTrue}}},
	}
	c := fake.NewClientBuilder().WithScheme(sc).WithObjects(admittedJobSetWl).Build()
	ctx := t.Context()

	admittedLWS, err := IsWorkloadAdmittedForParent(ctx, c, "default", "train", "q", KindLeaderWorkerSet, KindStatefulSet, KindPod)
	if err != nil {
		t.Fatalf("IsWorkloadAdmittedForParent(LWS kinds) error = %v, want nil", err)
	}
	if admittedLWS {
		t.Errorf("IsWorkloadAdmittedForParent(LWS kinds) = true for JobSet-owned Workload, want false")
	}

	admittedJobSet, err := IsWorkloadAdmittedForParent(ctx, c, "default", "train", "q", KindJobSet)
	if err != nil {
		t.Fatalf("IsWorkloadAdmittedForParent(KindJobSet) error = %v, want nil", err)
	}
	if !admittedJobSet {
		t.Errorf("IsWorkloadAdmittedForParent(KindJobSet) = false for JobSet-owned Workload, want true")
	}
}

func TestIsForeignSliceOwner(t *testing.T) {
	ownedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "llama-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel:      "llama",
				labels.SliceOwnerNamespaceLabel: "ns-a",
				labels.SliceOwnerKindLabel:      KindStatefulSet,
			},
		},
	}
	legacyOwnedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel: "llama",
			},
		},
	}
	unlabeledSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabeled-0"},
	}

	cases := []struct {
		name            string
		slice           *slicev1beta1.Slice
		ns, owner, kind string
		want            bool
	}{
		{name: "nil_slice", slice: nil, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "nil_labels", slice: unlabeledSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "matching_owner", slice: ownedSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "legacy_without_namespace_or_kind", slice: legacyOwnedSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "legacy_different_name", slice: legacyOwnedSlice, ns: "ns-a", owner: "other", kind: KindStatefulSet, want: true},
		{name: "different_namespace", slice: ownedSlice, ns: "ns-b", owner: "llama", kind: KindStatefulSet, want: true},
		{name: "different_name", slice: ownedSlice, ns: "ns-a", owner: "other", kind: KindStatefulSet, want: true},
		{name: "different_kind", slice: ownedSlice, ns: "ns-a", owner: "llama", kind: KindJob, want: true},
		{name: "empty_caller_kind_checks_namespace_and_name", slice: ownedSlice, ns: "ns-b", owner: "llama", kind: "", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsForeignSliceOwner(tc.slice, tc.ns, tc.owner, tc.kind); got != tc.want {
				t.Errorf("IsForeignSliceOwner(%v, %q, %q, %q) = %v, want %v", tc.slice, tc.ns, tc.owner, tc.kind, got, tc.want)
			}
		})
	}
}

func TestIsSliceOwnedBy(t *testing.T) {
	ownedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "llama-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel:      "llama",
				labels.SliceOwnerNamespaceLabel: "ns-a",
				labels.SliceOwnerKindLabel:      KindStatefulSet,
			},
		},
	}
	legacyOwnedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel: "llama",
			},
		},
	}
	unlabeledSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabeled-0"},
	}

	cases := []struct {
		name            string
		slice           *slicev1beta1.Slice
		ns, owner, kind string
		want            bool
	}{
		{name: "matching_modern_slice", slice: ownedSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: true},
		{name: "legacy_without_namespace_or_kind", slice: legacyOwnedSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: true},
		{name: "different_namespace", slice: ownedSlice, ns: "ns-b", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "different_kind", slice: ownedSlice, ns: "ns-a", owner: "llama", kind: KindJob, want: false},
		{name: "empty_caller_namespace_does_not_wildcard_stamped_slice", slice: ownedSlice, ns: "", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "empty_caller_kind_does_not_wildcard_stamped_slice", slice: ownedSlice, ns: "ns-a", owner: "llama", kind: "", want: false},
		{name: "nil_labels", slice: unlabeledSlice, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "nil_slice", slice: nil, ns: "ns-a", owner: "llama", kind: KindStatefulSet, want: false},
		{name: "empty_owner_name", slice: ownedSlice, ns: "ns-a", owner: "", kind: KindStatefulSet, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSliceOwnedBy(tc.slice, tc.ns, tc.owner, tc.kind); got != tc.want {
				t.Errorf("IsSliceOwnedBy(%v, %q, %q, %q) = %v, want %v", tc.slice, tc.ns, tc.owner, tc.kind, got, tc.want)
			}
		})
	}
}

func TestCleanupUnadmittedSliceForOwner(t *testing.T) {
	sc := newKueueTestScheme(t)
	ownedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "llama-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel:      "llama",
				labels.SliceOwnerNamespaceLabel: "ns-a",
				labels.SliceOwnerKindLabel:      KindStatefulSet,
			},
		},
	}
	legacyOwnedSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-0",
			Labels: map[string]string{
				labels.SliceOwnerNameLabel: "llama",
			},
		},
	}

	ctx := t.Context()
	c := fake.NewClientBuilder().WithScheme(sc).WithObjects(ownedSlice, legacyOwnedSlice).Build()

	// Foreign caller in ns-b must NOT delete ns-a's Slice.
	if _, err := CleanupUnadmittedSliceForOwner(ctx, c, "llama-0", "ns-b", "llama", KindStatefulSet); err != nil {
		t.Fatalf("CleanupUnadmittedSliceForOwner(\"llama-0\", \"ns-b\", \"llama\", %q) error = %v, want nil", KindStatefulSet, err)
	}
	if err := c.Get(ctx, client.ObjectKey{Name: "llama-0"}, &slicev1beta1.Slice{}); err != nil {
		t.Fatalf("c.Get(\"llama-0\") after foreign CleanupUnadmittedSliceForOwner() = %v, want nil", err)
	}

	// True owner in ns-a deletes its unadmitted Slice cleanly.
	if _, err := CleanupUnadmittedSliceForOwner(ctx, c, "llama-0", "ns-a", "llama", KindStatefulSet); err != nil {
		t.Fatalf("CleanupUnadmittedSliceForOwner(\"llama-0\", \"ns-a\", \"llama\", %q) error = %v, want nil", KindStatefulSet, err)
	}
	if err := c.Get(ctx, client.ObjectKey{Name: "llama-0"}, &slicev1beta1.Slice{}); !apierrors.IsNotFound(err) {
		t.Errorf("c.Get(\"llama-0\") after owner CleanupUnadmittedSliceForOwner() = %v, want NotFound", err)
	}

	// CleanupUnadmittedSlice deletes legacy Slice and tolerates empty/missing names.
	if _, err := CleanupUnadmittedSlice(ctx, c, ""); err != nil {
		t.Errorf("CleanupUnadmittedSlice(\"\") error = %v, want nil", err)
	}
	if _, err := CleanupUnadmittedSlice(ctx, c, "legacy-0"); err != nil {
		t.Errorf("CleanupUnadmittedSlice(\"legacy-0\") error = %v, want nil", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Name: "legacy-0"}, &slicev1beta1.Slice{}); !apierrors.IsNotFound(err) {
		t.Errorf("c.Get(\"legacy-0\") after CleanupUnadmittedSlice() = %v, want NotFound", err)
	}
}
