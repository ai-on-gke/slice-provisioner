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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

func TestParseTrailingIndex(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantIdx string
		wantOk  bool
	}{
		{
			name:    "valid trailing index",
			input:   "my-lws-worker-3",
			wantIdx: "3",
			wantOk:  true,
		},
		{
			name:    "no trailing index",
			input:   "my-lws-worker",
			wantIdx: "",
			wantOk:  false,
		},
		{
			name:    "ends with hyphen only",
			input:   "my-lws-worker-",
			wantIdx: "",
			wantOk:  false,
		},
		{
			name:    "alphabetic suffix",
			input:   "my-lws-worker-a",
			wantIdx: "",
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotOk := ParseTrailingIndex(tt.input)
			if gotOk != tt.wantOk || gotIdx != tt.wantIdx {
				t.Errorf("ParseTrailingIndex(%q) = (%q, %t), want (%q, %t)", tt.input, gotIdx, gotOk, tt.wantIdx, tt.wantOk)
			}
		})
	}
}

func TestSliceRequirements(t *testing.T) {
	tests := []struct {
		name         string
		obj          client.Object
		wantName     string
		wantType     string
		wantTopology string
		wantErr      bool
	}{
		{
			name:         "Nil object",
			obj:          nil,
			wantName:     "",
			wantType:     "",
			wantTopology: "",
			wantErr:      false,
		},
		{
			name: "Object with direct labels",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						labels.SliceNameLabel:     "direct-name",
						labels.SliceTypeLabel:     "tpu7x",
						labels.SliceTopologyLabel: "2x2x2",
					},
				},
			},
			wantName:     "direct-name",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet Single-slice LWS Worker",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws-worker-0",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.LWSGroupIndexLabel: "0",
						labels.LWSRoleLabel:       "worker",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-0",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet Single-slice LWS Leader (returns sliceName)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
						labels.LWSRoleLabel: "leader",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-0",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet multi-host LWS Leader (no sliceName; named per Pod)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "2",
							},
						},
					},
				},
			},
			wantName:     "",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			// size=1 LWS Leader has no single sliceName; Slices are named per Pod and provisioned per replica.
			name: "statefulset_single_host_lws_leader",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x1",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
						},
					},
				},
			},
			wantName:     "",
			wantType:     "tpu7x",
			wantTopology: "2x2x1",
			wantErr:      false,
		},
		{
			// Upgraded 0.6.0 shared LWS Leader ignores stale top-level labels when template rolls out unstamped.
			name: "statefulset_upgraded_060_shared_lws_leader_unstamped_template_rollout",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "legacy-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "legacy-lws",
						labels.SliceNameLabel:     "legacy-lws-0",
						labels.SliceTypeLabel:     "v6e",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "legacy-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
						},
					},
				},
			},
			wantName:     "",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			// LWS worker StatefulSet template labels take precedence over stale mutator-stamped top-level labels on rollout.
			name: "statefulset_lws_worker_template_rollout_overrides_stale_top_level_labels",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws-1",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.LWSRoleLabel:       LWSWorkerRole,
						labels.LWSGroupIndexLabel: "1",
						labels.SliceNameLabel:     "old-prefix-1",
						labels.SliceTypeLabel:     "v6e",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-1",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			// Single-host LWS Leader with explicit group-index returns derived sliceName.
			name: "statefulset_single_host_lws_leader_with_group_index",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.LWSGroupIndexLabel: "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x1",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-0",
			wantType:     "tpu7x",
			wantTopology: "2x2x1",
			wantErr:      false,
		},
		{
			name: "StatefulSet multi-host Leader Inclusion Leader (no sliceName)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation:               "4",
								labels.LWSSubgroupSizeAnnotation:       "2",
								labels.LWSSubgroupPolicyTypeAnnotation: "LeaderIncluded",
							},
						},
					},
				},
			},
			wantName:     "",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet multi-host Leader Excluded Leader (keeps dedicated sliceName)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation:               "4",
								labels.LWSSubgroupSizeAnnotation:       "2",
								labels.LWSSubgroupPolicyTypeAnnotation: "LeaderExcluded",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-leader",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet Case B: Leader Excluded Leader",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
						labels.LWSRoleLabel: "leader",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSubgroupSizeAnnotation:       "1",
								labels.LWSSubgroupPolicyTypeAnnotation: "LeaderExcluded",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-leader",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet Case C: Leader Inclusion Leader (returns sliceName)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
						labels.LWSRoleLabel: "leader",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSubgroupSizeAnnotation:       "1",
								labels.LWSSubgroupPolicyTypeAnnotation: "LeaderIncluded",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-0",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet Subgrouped Worker with trailing index",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws-worker-3",
					Labels: map[string]string{
						labels.LWSNameLabel: "my-lws",
						labels.LWSRoleLabel: "worker",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSubgroupSizeAnnotation: "1",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-3",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet with direct SliceNameLabel on template labels",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-sts",
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SliceNameLabel:     "direct-template-name",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "direct-template-name",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "StatefulSet with prefix but no LWS name (bare STS)",
			obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bare-sts",
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "",
			wantType:     "",
			wantTopology: "",
			wantErr:      true,
		},
		{
			name: "Job with replicated job name and job index",
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-job",
					Labels: map[string]string{
						labels.JobSetNameLabel:        "my-jobset",
						labels.JobIndexLabel:          "1",
						labels.ReplicatedJobNameLabel: "my-rep-job",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-my-rep-job-1",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "Job with job index only",
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-job",
					Labels: map[string]string{
						labels.JobSetNameLabel: "my-jobset",
						labels.JobIndexLabel:   "2",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix-2",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "Job with prefix only",
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-job",
					Labels: map[string]string{
						labels.JobSetNameLabel: "my-jobset",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-prefix",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "my-prefix",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
		{
			name: "Job with direct slice name label on template",
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-job",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.SliceNameLabel:     "direct-job-name",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			wantName:     "direct-job-name",
			wantType:     "tpu7x",
			wantTopology: "2x2x2",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotType, gotTopology, err := SliceRequirements(tt.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("SliceRequirements() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotName != tt.wantName || gotType != tt.wantType || gotTopology != tt.wantTopology {
				t.Errorf("SliceRequirements() = (%q, %q, %q), want (%q, %q, %q)",
					gotName, gotType, gotTopology, tt.wantName, tt.wantType, tt.wantTopology)
			}
		})
	}
}

func TestLWSName(t *testing.T) {
	tests := []struct {
		name string
		sts  *appsv1.StatefulSet
		want string
	}{
		{
			name: "nil_statefulset",
			sts:  nil,
			want: "",
		},
		{
			name: "top_level_label",
			sts:  &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LWSNameLabel: "lws-top"}}},
			want: "lws-top",
		},
		{
			name: "template_label",
			sts: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LWSNameLabel: "lws-tpl"}},
					},
				},
			},
			want: "lws-tpl",
		},
		{
			name: "owner_reference",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "lws-owner"}},
				},
			},
			want: "lws-owner",
		},
		{
			name: "top_level_label_precedence_over_template_and_owner",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels:          map[string]string{labels.LWSNameLabel: "lws-top"},
					OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "lws-owner"}},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LWSNameLabel: "lws-tpl"}},
					},
				},
			},
			want: "lws-top",
		},
		{
			name: "template_label_precedence_over_owner",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{Kind: KindLeaderWorkerSet, Name: "lws-owner"}},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labels.LWSNameLabel: "lws-tpl"}},
					},
				},
			},
			want: "lws-tpl",
		},
		{
			name: "non_lws_owner_reference",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "other-owner"}},
				},
			},
			want: "",
		},
		{
			name: "plain_statefulset",
			sts:  &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "plain"}},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LWSName(tc.sts); got != tc.want {
				t.Errorf("LWSName(%v) = %q, want %q", tc.sts, got, tc.want)
			}
		})
	}
}

func TestLWSRole(t *testing.T) {
	tests := []struct {
		name string
		sts  *appsv1.StatefulSet
		want string
	}{
		{
			name: "nil_statefulset_defaults_to_leader",
			sts:  nil,
			want: LWSLeaderRole,
		},
		{
			name: "top_level_worker_role_label_without_leader_name_annotation",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{labels.LWSRoleLabel: LWSWorkerRole},
				},
			},
			want: LWSWorkerRole,
		},
		{
			name: "template_leader_name_annotation_identifies_worker",
			sts: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSLeaderNameAnnotation: "my-lws-0"},
						},
					},
				},
			},
			want: LWSWorkerRole,
		},
		{
			name: "top_level_role_label_takes_precedence",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{labels.LWSRoleLabel: LWSLeaderRole},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSLeaderNameAnnotation: "unexpected"},
						},
					},
				},
			},
			want: LWSLeaderRole,
		},
		{
			name: "defaults_to_leader_when_unset",
			sts:  &appsv1.StatefulSet{},
			want: LWSLeaderRole,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LWSRole(tc.sts); got != tc.want {
				t.Errorf("LWSRole(%v) = %q, want %q", tc.sts, got, tc.want)
			}
		})
	}
}

func TestResolveStatefulSetSlices(t *testing.T) {
	zero := int32(0)
	two := int32(2)

	tests := []struct {
		name    string
		sts     *appsv1.StatefulSet
		want    StatefulSetSliceSpec
		wantErr bool
	}{
		{
			name: "nil_statefulset",
			sts:  nil,
			want: StatefulSetSliceSpec{},
		},
		{
			name: "lws_size1_leader_with_slice_prefix",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "my-lws",
					Labels: map[string]string{labels.LWSNameLabel: "my-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"my-lws-0", "my-lws-1"},
				Prefix:       "my-lws",
				Type:         "tpu7x",
				Topology:     "2x2x1",
				HasIntent:    true,
			},
		},
		{
			name: "upgraded_060_lws_size1_leader_with_top_level_slice_name",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "legacy-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "legacy-lws",
						labels.SliceNameLabel:     "legacy-lws-0",
						labels.SliceTypeLabel:     "tpu7x",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								labels.SlicePrefixLabel: "legacy-lws",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"legacy-lws-0", "legacy-lws-1"},
				Prefix:       "legacy-lws",
				Type:         "tpu7x",
				Topology:     "2x2x1",
				HasIntent:    true,
			},
		},
		{
			name: "lws_size1_leader_template_slice_labels_take_precedence_over_stale_top_level_labels",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "legacy-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "legacy-lws",
						labels.SliceTypeLabel:     "v6e",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "legacy-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"legacy-lws-0", "legacy-lws-1"},
				Prefix:       "legacy-lws",
				Type:         "tpu7x",
				Topology:     "2x2x2",
				HasIntent:    true,
			},
		},
		{
			name: "lws_leader_excluded_rollout_overrides_stale_top_level_labels",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.SliceNameLabel:     "my-lws-leader",
						labels.SliceTypeLabel:     "v6e",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								labels.LWSSizeAnnotation:               "4",
								labels.LWSSubgroupSizeAnnotation:       "2",
								labels.LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
							},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"my-lws-leader"},
				Type:         "tpu7x",
				Topology:     "2x2x2",
				HasIntent:    true,
			},
		},
		{
			name: "lws_size1_leader_scaled_to_zero_retains_intent",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "my-lws",
					Labels: map[string]string{labels.LWSNameLabel: "my-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &zero,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: nil,
				Prefix:       "my-lws",
				Type:         "tpu7x",
				Topology:     "2x2x1",
				HasIntent:    true,
			},
		},
		{
			name: "lws_size2_shared_leader_is_skipped",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "multi-host-lws",
					Labels: map[string]string{labels.LWSNameLabel: "multi-host-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "2"},
							Labels:      map[string]string{labels.SlicePrefixLabel: "multi-host-lws"},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{SkipSharedLeader: true},
		},
		{
			name: "lws_leader_excluded_policy_gets_dedicated_leader_slice",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "my-lws",
					Labels: map[string]string{labels.LWSNameLabel: "my-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								labels.LWSSizeAnnotation:               "4",
								labels.LWSSubgroupSizeAnnotation:       "2",
								labels.LWSSubgroupPolicyTypeAnnotation: LWSLeaderExcludedPolicy,
							},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"my-lws-leader"},
				Type:         "tpu7x",
				Topology:     "2x2x2",
				HasIntent:    true,
			},
		},
		{
			name: "lws_worker_with_role_label_only_provisions_group_slice",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws-worker-2",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.LWSRoleLabel:       LWSWorkerRole,
						labels.LWSGroupIndexLabel: "2",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "2"},
							Labels: map[string]string{
								labels.SlicePrefixLabel:   "my-lws",
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x2",
							},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{
				DesiredNames: []string{"my-lws-2"},
				Type:         "tpu7x",
				Topology:     "2x2x2",
				HasIntent:    true,
			},
		},
		{
			name: "lws_size1_leader_without_slice_prefix_has_no_intent",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "my-lws",
					Labels: map[string]string{labels.LWSNameLabel: "my-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{HasIntent: false},
		},
		{
			name: "lws_opted_out_template_ignores_stale_mutator_stamped_top_level_labels",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-lws",
					Labels: map[string]string{
						labels.LWSNameLabel:       "my-lws",
						labels.SliceNameLabel:     "my-lws-leader",
						labels.SliceTypeLabel:     "tpu7x",
						labels.SliceTopologyLabel: "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
						},
					},
				},
			},
			want: StatefulSetSliceSpec{HasIntent: false},
		},
		{
			name: "bare_statefulset_with_slice_prefix_returns_error",
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "bare-sts"},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{labels.SlicePrefixLabel: "invalid-prefix"},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveStatefulSetSlices(tc.sts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveStatefulSetSlices() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !got.Equal(tc.want) {
				t.Errorf("ResolveStatefulSetSlices() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLWSLeaderSliceNames(t *testing.T) {
	negOne := int32(-1)
	zero := int32(0)
	three := int32(3)
	tests := []struct {
		name      string
		sts       *appsv1.StatefulSet
		prefix    string
		wantNames []string
	}{
		{
			name:      "nil_statefulset",
			sts:       nil,
			prefix:    "p",
			wantNames: nil,
		},
		{
			name:      "empty_prefix",
			sts:       &appsv1.StatefulSet{},
			prefix:    "",
			wantNames: nil,
		},
		{
			name:      "negative_replicas",
			sts:       &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &negOne}},
			prefix:    "p",
			wantNames: nil,
		},
		{
			name:      "nil_replicas_defaults_to_1",
			sts:       &appsv1.StatefulSet{},
			prefix:    "p",
			wantNames: []string{"p-0"},
		},
		{
			name:      "zero_replicas",
			sts:       &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &zero}},
			prefix:    "p",
			wantNames: nil,
		},
		{
			name:      "three_replicas",
			sts:       &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &three}},
			prefix:    "srv",
			wantNames: []string{"srv-0", "srv-1", "srv-2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotNames := LWSLeaderSliceNames(tc.sts, tc.prefix)
			if !slices.Equal(gotNames, tc.wantNames) {
				t.Errorf("LWSLeaderSliceNames(%v, %q) = %v, want %v", tc.sts, tc.prefix, gotNames, tc.wantNames)
			}
		})
	}
}

func TestMatchesIndexedSlicePrefix(t *testing.T) {
	tests := []struct {
		name      string
		sliceName string
		prefix    string
		wantOK    bool
	}{
		{name: "zero_ordinal", sliceName: "llama-0", prefix: "llama", wantOK: true},
		{name: "multi_digit_ordinal", sliceName: "llama-12", prefix: "llama", wantOK: true},
		{name: "sibling_hyphenated_prefix", sliceName: "llama-70b-0", prefix: "llama", wantOK: false},
		{name: "leading_zero_ordinal", sliceName: "llama-01", prefix: "llama", wantOK: false},
		{name: "leader_suffix", sliceName: "llama-leader", prefix: "llama", wantOK: false},
		{name: "plus_signed_suffix", sliceName: "llama-+1", prefix: "llama", wantOK: false},
		{name: "minus_signed_suffix", sliceName: "llama--1", prefix: "llama", wantOK: false},
		{name: "integer_overflow_suffix", sliceName: "llama-999999999999999999999999999", prefix: "llama", wantOK: false},
		{name: "trailing_hyphen_only", sliceName: "llama-", prefix: "llama", wantOK: false},
		{name: "exact_prefix_without_ordinal", sliceName: "llama", prefix: "llama", wantOK: false},
		{name: "empty_prefix", sliceName: "llama-0", prefix: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if gotOK := MatchesIndexedSlicePrefix(tc.sliceName, tc.prefix); gotOK != tc.wantOK {
				t.Errorf("MatchesIndexedSlicePrefix(%q, %q) = %v, want %v",
					tc.sliceName, tc.prefix, gotOK, tc.wantOK)
			}
		})
	}
}
