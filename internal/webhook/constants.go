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
	"fmt"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

// Re-export shared constants as package-level aliases for internal use.
const (
	sliceNameLabel         = labels.SliceNameLabel
	sliceNameAnnotation    = labels.SliceNameAnnotation
	sliceTypeLabel         = labels.SliceTypeLabel
	sliceTopologyLabel     = labels.SliceTopologyLabel
	slicePrefixLabel       = labels.SlicePrefixLabel
	tpuResourceName        = labels.TPUResourceName
	jobIndexLabel          = labels.JobIndexLabel
	replicatedJobNameLabel = labels.ReplicatedJobNameLabel
	jobSetNameLabel        = labels.JobSetNameLabel
)

// immutableTPULabels are labels that cannot be changed once set on an opted-in
// workload, because they drive Slice CR creation. Changing them after
// provisioning would corrupt the slice mapping.
var immutableTPULabels = []string{
	slicePrefixLabel,
	sliceTypeLabel,
	sliceTopologyLabel,
}

// validateImmutableLabels rejects label mutations for TPU-critical fields.
// It only enforces immutability when the old object was opted-in (i.e. at least
// one of the immutable labels was set).
func validateImmutableLabels(oldLabels, newLabels map[string]string) error {
	for _, key := range immutableTPULabels {
		oldVal := oldLabels[key]
		newVal := newLabels[key]
		if oldVal != "" && oldVal != newVal {
			return fmt.Errorf("label %q is immutable once set (was %q, got %q)", key, oldVal, newVal)
		}
	}
	return nil
}
