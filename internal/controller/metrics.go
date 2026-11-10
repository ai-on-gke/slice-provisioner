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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// Metrics for reconciler
	ReconciliationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_provisioner_reconciliation_errors_total",
			Help: "Total number of slice provisioner reconciliation errors",
		},
		[]string{"error_type"},
	)
	SliceOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_provisioner_slice_operations_total",
			Help: "Total number of slice operations (creation/deletion)",
		},
		[]string{"operation", "status"},
	)
	SliceCreationProcessingMilliseconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "slice_provisioner_slice_creation_processing_milliseconds",
			Help:    "Wall-clock time from workload creation to Slice CR creation, includes controller startup and queueing delays (milliseconds)",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		},
	)
	SliceDeletionStuckTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slice_provisioner_slice_deletion_stuck_total",
			Help: "Total number of times a Slice deletion was detected as stuck (exceeds 5m threshold, likely blocked by external finalizer)",
		},
		[]string{"slice"},
	)
)

func init() {
	metrics.Registry.MustRegister(ReconciliationErrorsTotal)
	metrics.Registry.MustRegister(SliceOperationsTotal)
	metrics.Registry.MustRegister(SliceCreationProcessingMilliseconds)
	metrics.Registry.MustRegister(SliceDeletionStuckTotal)
}
