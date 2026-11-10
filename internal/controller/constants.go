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

// NOTE: This project uses multiple domain prefixes for historical reasons:
// - cloud.google.com/    — workload labels, annotations, finalizer
// - accelerator.gke.io/  — Slice CRD API group
// - partition.tpu.google.com/ — PartitionTree CRD API group
// - slice.gke.io/        — retry-on-failure annotation
// TODO: Consolidate to a single domain prefix in a future API version.
package controller

import (
	"time"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

const (
	sliceProvisionerFinalizer = "cloud.google.com/delete-slice-cr"
	slicePriorityAnnotation   = "cloud.google.com/slice-priority"
	managedByAnnotation       = "cloud.google.com/managed-by"
	// retryOnFailureAnnotation tells the slice-controller to retry when a Slice
	// enters SliceCreationFailed. This is critical for the preemption flow:
	// when the scheduler preempts slice-A and assigns the partition to slice-B,
	// GCE may not have fully released slice-A's capacity yet, causing slice-B's
	// configureAcceleratorTopology call to fail. With this annotation, the
	// slice-controller will retry instead of leaving the Slice in a failed state.
	retryOnFailureAnnotation = "slice.gke.io/retry-on-failure"
	sliceSchedulerName       = "slice-scheduler"

	// topologyChangeRequeueInterval is the requeue delay after deleting a Slice
	// for topology change, giving the API server time to process the deletion.
	topologyChangeRequeueInterval = 10 * time.Second

	// Owner labels on Slice CRs for orphan traceability and debugging.
	sliceOwnerNameLabel      = labels.SliceOwnerNameLabel
	sliceOwnerNamespaceLabel = labels.SliceOwnerNamespaceLabel
	sliceOwnerKindLabel      = labels.SliceOwnerKindLabel
)

// Re-export shared constants as package-level aliases for internal use.
const (
	sliceNameLabel         = labels.SliceNameLabel
	sliceNameAnnotation    = labels.SliceNameAnnotation
	sliceTypeLabel         = labels.SliceTypeLabel
	sliceTopologyLabel     = labels.SliceTopologyLabel
	slicePrefixLabel       = labels.SlicePrefixLabel
	tpuResourceName        = labels.TPUResourceName
	lwsNameLabel           = labels.LWSNameLabel
	lwsGroupIndexLabel     = labels.LWSGroupIndexLabel
	jobIndexLabel          = labels.JobIndexLabel
	replicatedJobNameLabel = labels.ReplicatedJobNameLabel
	jobSetNameLabel        = labels.JobSetNameLabel
)
