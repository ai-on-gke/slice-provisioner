# Release Notes

## v0.7.0

- Slice Provisioner controller that creates and deletes `Slice` custom
  resources for Jobs (created by JobSets) and StatefulSets labeled
  `cloud.google.com/gke-tpu-slice`.
- Supports LeaderWorkerSet StatefulSets that use `cloud.google.com/slice-prefix`,
  with one `Slice` per replica group.
- Reads the slice name, accelerator type, and topology from workload labels,
  and the priority from the workload's `PriorityClassName`.
- Adds the `cloud.google.com/delete-slice-cr` finalizer so the Slice CR is
  deleted with its workload.
- Optional admission webhooks that validate TPU labels on JobSets and
  StatefulSets, prevent duplicate slice prefixes, and inject slice names into
  Jobs, StatefulSets, and Pods.
- Helm chart and kustomize deployment.
