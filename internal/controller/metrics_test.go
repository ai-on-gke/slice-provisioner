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

package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReconciliationErrorsTotal(t *testing.T) {
	metric := ReconciliationErrorsTotal.WithLabelValues("test_error")
	before := testutil.ToFloat64(metric)
	metric.Inc()
	if expected := before + 1.0; testutil.ToFloat64(metric) != expected {
		t.Errorf("Expected %f, got %f", expected, testutil.ToFloat64(metric))
	}
}

func TestSliceOperationsTotal(t *testing.T) {
	metric := SliceOperationsTotal.WithLabelValues("Create", "Success")
	before := testutil.ToFloat64(metric)
	metric.Inc()
	if expected := before + 1.0; testutil.ToFloat64(metric) != expected {
		t.Errorf("Expected %f, got %f", expected, testutil.ToFloat64(metric))
	}
}

func TestSliceCreationProcessingMilliseconds(t *testing.T) {
	// Histogram observation doesn't panic
	SliceCreationProcessingMilliseconds.Observe(500)

	// We can verify the count increased
	if count := testutil.CollectAndCount(SliceCreationProcessingMilliseconds); count == 0 {
		t.Error("Expected metric to be collected")
	}
}
