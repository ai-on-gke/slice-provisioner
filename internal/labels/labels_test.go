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

package labels

import (
	"strings"
	"testing"
)

func TestLabelConstants(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		expectedPrefix string
	}{
		{
			name:           "SliceNameLabel",
			value:          SliceNameLabel,
			expectedPrefix: "cloud.google.com/",
		},
		{
			name:           "SliceNameAnnotation",
			value:          SliceNameAnnotation,
			expectedPrefix: "cloud.google.com/",
		},
		{
			name:           "SliceTypeLabel",
			value:          SliceTypeLabel,
			expectedPrefix: "cloud.google.com/",
		},
		{
			name:           "SliceTopologyLabel",
			value:          SliceTopologyLabel,
			expectedPrefix: "cloud.google.com/",
		},
		{
			name:           "SlicePrefixLabel",
			value:          SlicePrefixLabel,
			expectedPrefix: "cloud.google.com/",
		},
		{
			name:           "TPUResourceName",
			value:          TPUResourceName,
			expectedPrefix: "google.com/",
		},
		{
			name:           "JobIndexLabel",
			value:          JobIndexLabel,
			expectedPrefix: "jobset.sigs.k8s.io/",
		},
		{
			name:           "ReplicatedJobNameLabel",
			value:          ReplicatedJobNameLabel,
			expectedPrefix: "jobset.sigs.k8s.io/",
		},
		{
			name:           "JobSetNameLabel",
			value:          JobSetNameLabel,
			expectedPrefix: "jobset.sigs.k8s.io/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value == "" {
				t.Errorf("%s should not be empty", tt.name)
			}
			if !strings.HasPrefix(tt.value, tt.expectedPrefix) {
				t.Errorf("%s = %q, want prefix %q", tt.name, tt.value, tt.expectedPrefix)
			}
		})
	}
}

func TestLabelConstantValues(t *testing.T) {
	// Verify exact values to catch accidental renames.
	if SliceNameLabel != "cloud.google.com/gke-tpu-slice" {
		t.Errorf("SliceNameLabel = %q, want %q", SliceNameLabel, "cloud.google.com/gke-tpu-slice")
	}
	if SliceTypeLabel != "cloud.google.com/gke-tpu-accelerator" {
		t.Errorf("SliceTypeLabel = %q, want %q", SliceTypeLabel, "cloud.google.com/gke-tpu-accelerator")
	}
	if SliceTopologyLabel != "cloud.google.com/gke-tpu-topology" {
		t.Errorf("SliceTopologyLabel = %q, want %q", SliceTopologyLabel, "cloud.google.com/gke-tpu-topology")
	}
	if SlicePrefixLabel != "cloud.google.com/slice-prefix" {
		t.Errorf("SlicePrefixLabel = %q, want %q", SlicePrefixLabel, "cloud.google.com/slice-prefix")
	}
	if TPUResourceName != "google.com/tpu" {
		t.Errorf("TPUResourceName = %q, want %q", TPUResourceName, "google.com/tpu")
	}
}
