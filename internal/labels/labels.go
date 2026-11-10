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

// Package labels provides shared label, annotation, and resource constants
// used by both the controller and webhook packages.
package labels

const (
	SliceNameLabel      = "cloud.google.com/gke-tpu-slice"
	SliceNameAnnotation = "cloud.google.com/gke-tpu-slice-name"
	SliceTypeLabel      = "cloud.google.com/gke-tpu-accelerator"
	SliceTopologyLabel  = "cloud.google.com/gke-tpu-topology"
	SlicePrefixLabel    = "cloud.google.com/slice-prefix"

	// Exclusive Topology Signals for Static Slicing Mode
	LWSExclusiveTopologyAnnotation    = "leaderworkerset.sigs.k8s.io/exclusive-topology"
	JobSetExclusiveTopologyAnnotation = "alpha.jobset.sigs.k8s.io/exclusive-topology"
	AcceleratorModeKey                = "cloud.google.com/gke-accelerator-topology-mode"
	ProvisionOnlyValue                = "PROVISION_ONLY"
	TPUResourceName                   = "google.com/tpu"

	// LeaderWorkerSet labels & annotations
	LWSNameLabel                    = "leaderworkerset.sigs.k8s.io/name"
	LWSLeaderNameAnnotation         = "leaderworkerset.sigs.k8s.io/leader-name"
	LWSSizeAnnotation               = "leaderworkerset.sigs.k8s.io/size"
	LWSGroupIndexLabel              = "leaderworkerset.sigs.k8s.io/group-index"
	LWSRoleLabel                    = "leaderworkerset.sigs.k8s.io/role"
	LWSSubgroupSizeAnnotation       = "leaderworkerset.sigs.k8s.io/subgroup-size"
	LWSSubgroupPolicyTypeAnnotation = "leaderworkerset.sigs.k8s.io/subgroup-policy-type"

	// Kueue labels & annotations
	KueueQueueNameLabel                   = "kueue.x-k8s.io/queue-name"
	KueueComponentWorkloadIndexAnnotation = "kueue.x-k8s.io/component-workload-index"

	// Slice ownership labels stamped on cluster-scoped Slice CRs
	SliceOwnerNameLabel      = "cloud.google.com/slice-owner-name"
	SliceOwnerNamespaceLabel = "cloud.google.com/slice-owner-namespace"
	SliceOwnerKindLabel      = "cloud.google.com/slice-owner-kind"

	// JobSet labels
	JobIndexLabel          = "jobset.sigs.k8s.io/job-index"
	ReplicatedJobNameLabel = "jobset.sigs.k8s.io/replicatedjob-name"
	JobSetNameLabel        = "jobset.sigs.k8s.io/jobset-name"
)
