# User Playbook: Slice Provisioner

A practical, debugging-focused guide for GKE customers using the
**Slice Provisioner**. This playbook focuses on what you can see
and act on with `kubectl` and standard monitoring tools.

---

## Table of Contents

1. [What is the Slice Provisioner?](#1-what-is-the-slice-provisioner)
2. [Quick Reference](#2-quick-reference)
3. [Lifecycle: how Slices are created and deleted](#3-lifecycle)
4. [Inspecting your Slices](#4-inspecting-your-slices)
5. [Triage decision tree](#5-triage-decision-tree)
6. [Troubleshooting scenarios](#6-troubleshooting-scenarios)
7. [Events & conditions cheatsheet](#7-events--conditions-cheatsheet)
8. [Metrics & monitoring](#8-metrics--monitoring)
9. [FAQ](#9-faq)

---

## 1. What is the Slice Provisioner?

The Slice Provisioner is a Kubernetes controller that **automates the
lifecycle of Slice CRs**. It watches for workloads (Jobs from JobSets,
StatefulSets) with TPU labels and creates corresponding `Slice` custom
resources. When a workload is deleted, the provisioner cleans up the
associated Slice CR.

**You get:**

- Automatic `Slice` CR creation when TPU workloads appear
- Priority propagation from PriorityClass to Slice CR
- Retry-on-failure annotation for preemption resilience
- Finalizer-based cleanup ensuring no orphaned Slices
- Webhook validation preventing misconfigured workloads

---

## 2. Quick Reference

### Key labels (on workloads)

| Label | Purpose | Example |
|-------|---------|---------|
| `cloud.google.com/gke-tpu-slice` | Slice name prefix | `my-training-worker-0` |
| `cloud.google.com/gke-tpu-accelerator` | Accelerator type | `tpu7x` |
| `cloud.google.com/gke-tpu-topology` | Topology shape | `4x4x4` |

### Key annotations (on Slice CRs)

| Annotation | Purpose | Value |
|------------|---------|-------|
| `cloud.google.com/managed-by` | Identifies scheduler ownership | `slice-scheduler` |
| `cloud.google.com/slice-priority` | Scheduling priority | `0` (highest) to `1000000000` |
| `cloud.google.com/workload-name` | Source workload name | JobSet/STS name |
| `cloud.google.com/num-slices` | Number of slices in group | `1`, `2`, etc. |
| `slice.gke.io/retry-on-failure` | Enable slice-controller retry | `true` |

### Quick checks

```bash
# List all Slice CRs
kubectl get slices -A

# Check provisioner controller status
kubectl get pods -n slice-provisioner-system

# View controller logs
kubectl logs -n slice-provisioner-system deploy/slice-provisioner-controller-manager -f

# Check webhook status
kubectl get validatingwebhookconfigurations | grep slice
kubectl get mutatingwebhookconfigurations | grep slice
```

---

## 3. Lifecycle

### Creation flow

```
1. User creates a JobSet with TPU labels
2. JobSet controller creates Jobs
3. Slice Provisioner sees the Job (via label watch)
4. Provisioner extracts: name, type, topology, priority
5. Provisioner creates a cluster-scoped Slice CR
6. Provisioner adds finalizer to the Job
7. Slice Scheduler assigns partitions to the Slice
8. Slice Controller forms the topology on GCE
```

### Deletion flow

```
1. User deletes the JobSet
2. JobSet controller deletes Jobs
3. Kubernetes blocks deletion (finalizer present)
4. Provisioner sees the Job entering deletion
5. Provisioner deletes the associated Slice CR
6. Provisioner removes the finalizer from the Job
7. Job is garbage collected
```

### Key behaviors

- **Finalizer:** `cloud.google.com/delete-slice-cr` is added to every
  workload that has a Slice CR. This ensures cleanup even if the
  provisioner restarts during deletion.
- **Retry annotation:** Every Slice CR gets
  `slice.gke.io/retry-on-failure: "true"`. During preemption, if
  GCE capacity isn't immediately available, the slice-controller will
  retry instead of leaving the Slice stuck in `SliceCreationFailed`.
- **Priority:** Resolved from the workload's `PriorityClassName` to an
  integer value. The scheduler uses this for preemption decisions.

---

## 4. Inspecting your Slices

### View all Slices

```bash
kubectl get slices -o wide
```

Output columns: NAME, TYPE, TOPOLOGY, PARTITION-IDS, CONDITIONS

### Detailed Slice inspection

```bash
kubectl describe slice <slice-name>
```

Key fields to check:
- `spec.partitionIds` — assigned partitions (empty = unscheduled)
- `status.conditions[type=Ready]` — current state
- `metadata.annotations` — priority, workload name, retry flag

### Find orphaned Slices

```bash
# Slices without a matching workload
kubectl get slices -o json | jq -r '.items[] | select(.metadata.annotations["cloud.google.com/workload-name"] != null) | .metadata.name'
```

---

## 5. Triage Decision Tree

```
Slice CR not created?
├── Check workload has required labels → Section 6.1
├── Check webhook is not blocking → Section 6.3
└── Check provisioner logs → Section 6.2

Slice stuck in SliceCreationFailed?
├── Check retry-on-failure annotation → Section 6.4
├── Check partition tree health → Section 6.5
└── Check scheduler logs → Section 6.6

Slice not getting partitions?
├── Check scheduler is running → Section 6.6
├── Check partition tree has available partitions → Section 6.5
└── Check priority vs other workloads → Section 6.7
```

---

## 6. Troubleshooting Scenarios

### 6.1 Slice CR not created

**Symptoms:** Workload is running but no Slice CR exists.

**Check:**
```bash
# Verify labels on the workload
kubectl get job <job-name> -o jsonpath='{.metadata.labels}' | jq .
```

Required labels:
- `cloud.google.com/gke-tpu-slice`
- `cloud.google.com/gke-tpu-accelerator`
- `cloud.google.com/gke-tpu-topology`

### 6.2 Provisioner not reconciling

**Symptoms:** Labels are correct but Slice CR still not created.

```bash
# Check controller logs
kubectl logs -n slice-provisioner-system deploy/slice-provisioner-controller-manager --since=5m | grep -i error

# Check if controller is leader
kubectl logs -n slice-provisioner-system deploy/slice-provisioner-controller-manager | grep "leader"
```

### 6.3 Webhook blocking operations

**Symptoms:** `admission webhook denied the request`

```bash
# Check webhook status
kubectl get endpoints -n slice-provisioner-system

# Emergency: disable webhook (skips validation)
kubectl delete validatingwebhookconfiguration slice-provisioner-validating-webhook
kubectl delete mutatingwebhookconfiguration slice-provisioner-mutating-webhook
```

### 6.4 Slice stuck in SliceCreationFailed

**Symptoms:** Slice has partitions but state is `SliceCreationFailed`.

```bash
# Check if retry annotation is set
kubectl get slice <name> -o jsonpath='{.metadata.annotations.slice\.gke\.io/retry-on-failure}'
# Should output: true

# Check slice-controller logs for retry progress
kubectl logs -n gke-managed-system deploy/slice-controller | grep <slice-name>
```

If retry annotation is missing, the Slice was not created by the
provisioner. Manually add it:

```bash
kubectl annotate slice <name> slice.gke.io/retry-on-failure=true
```

### 6.5 Partition tree unhealthy

```bash
# Check partition tree status
kubectl get partitiontrees -o wide

# Look for unhealthy/degraded partitions
kubectl get partitiontrees -o json | jq '.items[].status.root.state'
```

### 6.6 Scheduler not assigning partitions

```bash
# Check scheduler logs
kubectl logs -n slice-provisioner-system deploy/slice-scheduler | grep -i "schedule\|error"

# Verify managed-by annotation
kubectl get slice <name> -o jsonpath='{.metadata.annotations.cloud\.google\.com/managed-by}'
# Should output: slice-scheduler
```

### 6.7 Priority and preemption

```bash
# Check slice priority
kubectl get slice <name> -o jsonpath='{.metadata.annotations.cloud\.google\.com/slice-priority}'

# List all slices by priority
kubectl get slices -o json | jq -r '.items[] | [.metadata.name, .metadata.annotations["cloud.google.com/slice-priority"]] | @tsv' | sort -k2 -n
```

Lower number = higher priority. Priority `0` preempts everything.

---

## 7. Events & Conditions Cheatsheet

### Events emitted by the provisioner

| Event | Type | Meaning |
|-------|------|---------|
| `SliceCreated` | Normal | Slice CR successfully created |
| `SliceDeleted` | Normal | Slice CR successfully deleted |
| `FinalizerAdded` | Normal | Cleanup finalizer added to workload |
| `FinalizerRemoved` | Normal | Finalizer removed after Slice deletion |

### Slice status conditions

| Condition Reason | Meaning | Action |
|-----------------|---------|--------|
| `SliceNotCreated` | Initial state, waiting for partition assignment | Wait for scheduler |
| `ACTIVATING` | Partition assigned, topology being formed | Wait for slice-controller |
| `ACTIVE` | Topology formed, nodes ready | Workload can run |
| `SliceCreationFailed` | Formation failed (may be transient) | Check retry annotation |
| `FAILED` | Permanent failure | Check partition health |
| `DEACTIVATING` | Topology being torn down | Wait for completion |
| `INACTIVE` | Topology fully torn down | Slice can be rescheduled |

---

## 8. Metrics & Monitoring

The provisioner exposes Prometheus metrics on port `8443` (HTTPS) or
`8080` (HTTP).

### Key metrics

| Metric | Type | Description |
|--------|------|-------------|
| `controller_runtime_reconcile_total` | Counter | Total reconciliations by controller |
| `controller_runtime_reconcile_errors_total` | Counter | Failed reconciliations |
| `controller_runtime_reconcile_time_seconds` | Histogram | Reconciliation latency |
| `workqueue_depth` | Gauge | Items waiting to be reconciled |
| `workqueue_adds_total` | Counter | Total items added to work queue |

### Prometheus ServiceMonitor

Apply the ServiceMonitor to scrape metrics:

```bash
kubectl apply -f slice-provisioner-servicemonitor.yaml
```

---

## 9. FAQ

**Q: Can I run the provisioner alongside other TPU controllers?**
A: Yes. The provisioner only watches workloads with specific TPU labels
and uses a `managed-by` annotation to avoid conflicts.

**Q: What happens if the provisioner restarts?**
A: The controller is stateless — all state is in the Kubernetes API.
On restart, it re-reconciles all workloads and Slices. Finalizers
ensure no cleanup is missed.

**Q: What happens during preemption?**
A: When a higher-priority workload needs capacity, the scheduler
preempts lower-priority Slices. The `retry-on-failure` annotation
ensures that if the replacement Slice fails initially (GCE capacity
not released yet), the slice-controller retries automatically.

**Q: Can I manually create Slice CRs?**
A: Yes, but you must add the `retry-on-failure` annotation and
`managed-by` annotation manually. The provisioner won't manage
manually-created Slices.

**Q: How do I change a workload's priority?**
A: Update the workload's `PriorityClassName`. The provisioner will
not update existing Slice CRs — you need to delete and recreate
the workload.
