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
	"fmt"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

// LeaderWorkerSet role and subgroup policy constants.
const (
	LWSLeaderRole           = "leader"
	LWSWorkerRole           = "worker"
	LWSLeaderExcludedPolicy = "LeaderExcluded"
)

// GetLWSRole returns the LWS role (leader or worker) based on pod template annotations.
// In LeaderWorkerSet, Worker StatefulSet templates contain the "leaderworkerset.sigs.k8s.io/leader-name" annotation.
func GetLWSRole(annotations map[string]string) string {
	if annotations != nil && annotations[labels.LWSLeaderNameAnnotation] != "" {
		return LWSWorkerRole
	}
	return LWSLeaderRole
}

// LWSRole returns the LWS role for sts, preferring the top-level LWS role label
// and falling back to pod template annotations.
func LWSRole(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return LWSLeaderRole
	}
	if role := sts.Labels[labels.LWSRoleLabel]; role != "" {
		return role
	}
	return GetLWSRole(sts.Spec.Template.Annotations)
}

// LWSName returns the owning LeaderWorkerSet name for sts, checking top-level
// labels, pod template labels, and OwnerReferences.
func LWSName(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return ""
	}
	if name := sts.Labels[labels.LWSNameLabel]; name != "" {
		return name
	}
	if name := sts.Spec.Template.Labels[labels.LWSNameLabel]; name != "" {
		return name
	}
	for _, owner := range sts.OwnerReferences {
		if owner.Kind == KindLeaderWorkerSet {
			return owner.Name
		}
	}
	return ""
}

func isLeaderExcluded(annotations map[string]string) bool {
	return annotations[labels.LWSSubgroupSizeAnnotation] != "" &&
		annotations[labels.LWSSubgroupPolicyTypeAnnotation] == LWSLeaderExcludedPolicy
}

// StatefulSetSliceSpec describes the TPU Slice CRs managed by a StatefulSet.
type StatefulSetSliceSpec struct {
	DesiredNames     []string
	Prefix           string
	Type             string
	Topology         string
	HasIntent        bool
	SkipSharedLeader bool
}

// Equal reports whether s and other represent identical Slice provisioning requirements.
func (s StatefulSetSliceSpec) Equal(other StatefulSetSliceSpec) bool {
	return s.HasIntent == other.HasIntent &&
		s.SkipSharedLeader == other.SkipSharedLeader &&
		s.Prefix == other.Prefix &&
		s.Type == other.Type &&
		s.Topology == other.Topology &&
		slices.Equal(s.DesiredNames, other.DesiredNames)
}

// ResolveStatefulSetSlices resolves the authoritative TPU Slice provisioning
// specification for sts across standalone StatefulSets, LWS worker StatefulSets,
// LeaderExcluded LWS leaders, multi-host (size > 1) shared LWS leaders, and
// single-host (size == 1) per-replica LWS leaders.
func ResolveStatefulSetSlices(sts *appsv1.StatefulSet) (StatefulSetSliceSpec, error) {
	if sts == nil {
		return StatefulSetSliceSpec{}, nil
	}
	if LWSName(sts) != "" && LWSRole(sts) == LWSLeaderRole && !isLeaderExcluded(sts.Spec.Template.Annotations) {
		sizeStr := sts.Spec.Template.Annotations[labels.LWSSizeAnnotation]
		hasGroupIndex := sts.Labels[labels.LWSGroupIndexLabel] != ""
		if sizeStr != "" && sizeStr != "1" {
			return StatefulSetSliceSpec{SkipSharedLeader: true}, nil
		}
		prefix := sts.Spec.Template.Labels[labels.SlicePrefixLabel]
		if sizeStr == "1" && !hasGroupIndex && prefix != "" {
			// The Pod template is authoritative for shared leaders: LeaderWorkerSet
			// rewrites it on rolling updates, whereas top-level slice labels may be
			// stale leftovers stamped by older slice-provisioner releases.
			sliceType := sts.Spec.Template.Labels[labels.SliceTypeLabel]
			if sliceType == "" {
				sliceType = sts.Labels[labels.SliceTypeLabel]
			}
			sliceTopology := sts.Spec.Template.Labels[labels.SliceTopologyLabel]
			if sliceTopology == "" {
				sliceTopology = sts.Labels[labels.SliceTopologyLabel]
			}
			return StatefulSetSliceSpec{
				DesiredNames: LWSLeaderSliceNames(sts, prefix),
				Prefix:       prefix,
				Type:         sliceType,
				Topology:     sliceTopology,
				HasIntent:    true,
			}, nil
		}
	}

	sliceName, sliceType, sliceTopology, err := SliceRequirements(sts)
	if err != nil || sliceName == "" {
		return StatefulSetSliceSpec{
			Type:      sliceType,
			Topology:  sliceTopology,
			HasIntent: false,
		}, err
	}
	return StatefulSetSliceSpec{
		DesiredNames: []string{sliceName},
		Type:         sliceType,
		Topology:     sliceTopology,
		HasIntent:    true,
	}, nil
}

// LWSLeaderSliceNames returns the ordered Slice CR names (<prefix>-<ordinal>)
// desired for an LWS size=1 leader StatefulSet.
func LWSLeaderSliceNames(sts *appsv1.StatefulSet, prefix string) []string {
	if sts == nil || prefix == "" {
		return nil
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas <= 0 {
		return nil
	}
	names := make([]string, int(replicas))
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return names
}

// ParseIndexedSliceOrdinal parses sliceName of the exact form
// "<prefix>-<non-negative-int>" without leading zeros and returns the ordinal.
func ParseIndexedSliceOrdinal(sliceName, prefix string) (int, bool) {
	if prefix == "" {
		return 0, false
	}
	suffix, ok := strings.CutPrefix(sliceName, prefix+"-")
	if !ok || suffix == "" || (len(suffix) > 1 && suffix[0] == '0') {
		return 0, false
	}
	for i := range len(suffix) {
		if suffix[i] < '0' || suffix[i] > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(suffix)
	return idx, err == nil && idx >= 0
}

// MatchesIndexedSlicePrefix reports whether sliceName has the exact form
// "<prefix>-<non-negative-int>" without leading zeros.
func MatchesIndexedSlicePrefix(sliceName, prefix string) bool {
	_, ok := ParseIndexedSliceOrdinal(sliceName, prefix)
	return ok
}

// SliceRequirements extracts or derives TPU Slice name, type, and topology from an object.
// For workloads under PodTemplate inheritance (like LeaderWorkerSet and JobSet), derives dynamic intent.
func SliceRequirements(obj client.Object) (string, string, string, error) {
	if obj == nil {
		return "", "", "", nil
	}
	lbls := obj.GetLabels()
	sliceName, sliceType, sliceTopology := lbls[labels.SliceNameLabel], lbls[labels.SliceTypeLabel], lbls[labels.SliceTopologyLabel]

	if sts, ok := obj.(*appsv1.StatefulSet); ok {
		sliceName, sliceType, sliceTopology = applyLWSStatefulSetTemplateOverrides(sts, sliceName, sliceType, sliceTopology)
	}

	// 1. First, check if all three required labels are present on the top-level object.
	if sliceName != "" && sliceType != "" && sliceTopology != "" {
		return sliceName, sliceType, sliceTopology, nil
	}

	// 2. If not all are present, check the Pod Template of specific object types.
	switch o := obj.(type) {
	case *appsv1.StatefulSet:
		return statefulSetTemplateSliceRequirements(o, lbls, sliceName, sliceType, sliceTopology)

	case *batchv1.Job:
		templateLabels := o.Spec.Template.Labels
		if templateLabels == nil {
			return sliceName, sliceType, sliceTopology, nil
		}

		if sliceName == "" {
			if prefix := templateLabels[labels.SlicePrefixLabel]; prefix != "" {
				// If prefix is set but it is not managed by JobSet, it is invalid.
				if lbls[labels.JobSetNameLabel] == "" {
					return "", "", "", fmt.Errorf("slice-prefix was specified but the Job is not managed by JobSet")
				}

				if jobIndex := lbls[labels.JobIndexLabel]; jobIndex != "" {
					if rJobName := lbls[labels.ReplicatedJobNameLabel]; rJobName != "" {
						// Format and truncate components (prefix <= 35 chars, replicatedJobName <= 20 chars, jobIndex <= 6 chars)
						// to ensure the derived Slice name does not exceed the Kubernetes 63-character RFC 1123 limit.
						sliceName = fmt.Sprintf("%.35s-%.20s-%.6s", prefix, rJobName, jobIndex)
					} else {
						sliceName = fmt.Sprintf("%s-%s", prefix, jobIndex)
					}
				} else {
					sliceName = prefix
				}
			} else {
				sliceName = templateLabels[labels.SliceNameLabel]
			}
		}
		if sliceType == "" {
			sliceType = templateLabels[labels.SliceTypeLabel]
		}
		if sliceTopology == "" {
			sliceTopology = templateLabels[labels.SliceTopologyLabel]
		}
	}

	return sliceName, sliceType, sliceTopology, nil
}

func applyLWSStatefulSetTemplateOverrides(sts *appsv1.StatefulSet, sliceName, sliceType, sliceTopology string) (string, string, string) {
	if LWSName(sts) == "" {
		return sliceName, sliceType, sliceTopology
	}
	if sts.Spec.Template.Labels[labels.SlicePrefixLabel] == "" {
		if sts.Spec.Template.Labels[labels.SliceNameLabel] == "" {
			// For LWS-managed StatefulSets, user-authored TPU labels live on the
			// Pod template; if neither slice-prefix nor slice-name is present on
			// the template, top-level labels are stale leftovers from an earlier
			// mutator-stamped configuration and must not resurrect phantom Slices.
			return "", "", ""
		}
		return sliceName, sliceType, sliceTopology
	}
	// Upgraded <= 0.6.0 LWS leader StatefulSets carry slice-name on both
	// top-level metadata and the Pod template until a rolling update
	// rewrites the template. Preserve that stamped sliceName only while a
	// non-LeaderExcluded leader template still carries slice-name; otherwise
	// re-derive sliceName and prefer Pod template type/topology over stale
	// mutator-stamped top-level labels.
	if LWSRole(sts) != LWSLeaderRole || isLeaderExcluded(sts.Spec.Template.Annotations) || sts.Spec.Template.Labels[labels.SliceNameLabel] == "" {
		sliceName = ""
	}
	if t := sts.Spec.Template.Labels[labels.SliceTypeLabel]; t != "" {
		sliceType = t
	}
	if topo := sts.Spec.Template.Labels[labels.SliceTopologyLabel]; topo != "" {
		sliceTopology = topo
	}
	return sliceName, sliceType, sliceTopology
}

func statefulSetTemplateSliceRequirements(sts *appsv1.StatefulSet, lbls map[string]string, sliceName, sliceType, sliceTopology string) (string, string, string, error) {
	templateLabels := sts.Spec.Template.Labels
	if templateLabels == nil {
		return sliceName, sliceType, sliceTopology, nil
	}

	if sliceName == "" {
		if prefix := templateLabels[labels.SlicePrefixLabel]; prefix != "" {
			if LWSName(sts) == "" {
				return "", "", "", fmt.Errorf("slice-prefix was specified but the StatefulSet is not managed by LeaderWorkerSet")
			}
			sliceName = deriveLWSStatefulSetSliceName(sts, lbls, prefix)
		} else {
			sliceName = templateLabels[labels.SliceNameLabel]
		}
	}
	if sliceType == "" {
		sliceType = templateLabels[labels.SliceTypeLabel]
	}
	if sliceTopology == "" {
		sliceTopology = templateLabels[labels.SliceTopologyLabel]
	}
	return sliceName, sliceType, sliceTopology, nil
}

func deriveLWSStatefulSetSliceName(sts *appsv1.StatefulSet, lbls map[string]string, prefix string) string {
	role := LWSRole(sts)
	subgroupPolicyType := sts.Spec.Template.Annotations[labels.LWSSubgroupPolicyTypeAnnotation]
	subgroupSizeStr := sts.Spec.Template.Annotations[labels.LWSSubgroupSizeAnnotation]
	sizeStr := sts.Spec.Template.Annotations[labels.LWSSizeAnnotation]
	leaderExcluded := isLeaderExcluded(sts.Spec.Template.Annotations)

	switch {
	case role == LWSLeaderRole && sizeStr != "" && lbls[labels.LWSGroupIndexLabel] == "" && !leaderExcluded:
		// LWS creates one leader StatefulSet shared across all replica groups
		// (both size > 1 and size == 1). No single slice name in its template
		// is valid for every group; PodMutator names each leader Pod from its
		// group index instead, and StatefulSetReconciler provisions per-replica
		// Slices when size == 1.
		return ""
	case subgroupSizeStr == "":
		groupIndex := lbls[labels.LWSGroupIndexLabel]
		if groupIndex == "" {
			groupIndex = "0"
		}
		return fmt.Sprintf("%s-%s", prefix, groupIndex)
	case role == LWSLeaderRole && subgroupPolicyType == LWSLeaderExcludedPolicy:
		return fmt.Sprintf("%s-leader", prefix)
	case role == LWSLeaderRole:
		groupIndex := lbls[labels.LWSGroupIndexLabel]
		if groupIndex == "" {
			groupIndex = "0"
		}
		return fmt.Sprintf("%s-%s", prefix, groupIndex)
	default:
		if groupIndex, ok := ParseTrailingIndex(sts.Name); ok {
			return fmt.Sprintf("%s-%s", prefix, groupIndex)
		}
		return prefix
	}
}

// LeaderPodSliceName resolves the TPU Slice name for an LWS leader Pod.
// All groups share one leader StatefulSet, so its template cannot carry a per-group
// slice name; the group index is taken from the Pod instead. The LWS group-index
// label is preferred, falling back to the Pod's StatefulSet ordinal (which LWS uses
// as the group index) so that admission does not depend on webhook ordering.
func LeaderPodSliceName(pod *corev1.Pod) string {
	prefix := pod.Labels[labels.SlicePrefixLabel]
	if prefix == "" {
		return ""
	}
	if isLeaderExcluded(pod.Annotations) {
		return fmt.Sprintf("%s-leader", prefix)
	}
	groupIndex := pod.Labels[labels.LWSGroupIndexLabel]
	if groupIndex == "" {
		var ok bool
		if groupIndex, ok = ParseTrailingIndex(pod.Name); !ok {
			groupIndex = "0"
		}
	}
	return fmt.Sprintf("%s-%s", prefix, groupIndex)
}

// ParseTrailingIndex extracts the trailing index from a resource name (e.g. "my-sts-3" -> "3", true).
func ParseTrailingIndex(name string) (string, bool) {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '-' {
			if i < len(name)-1 {
				return name[i+1:], true
			}
			break
		}
		if name[i] < '0' || name[i] > '9' {
			break
		}
	}
	return "", false
}
