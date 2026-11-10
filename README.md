# Slice Provisioner

A Kubernetes controller that automates the lifecycle of `Slice` custom resources (CRs). It watches for workloads (Job and StatefulSet) with specific TPU labels and creates corresponding `Slice` CRs, tying the slice lifecycle to the workload's lifecycle. When a workload is deleted, the provisioner cleans up the associated Slice CR.

## How it works

```
User                       JobSet Controller          Slice Provisioner           Slice Scheduler
 │                              │                          │                          │
 ├─ kubectl apply JobSet ──────►│                          │                          │
 │                              ├─ creates Job ───────────►│                          │
 │                              │   (with TPU labels)      │                          │
 │                              │                          ├─ creates Slice CR ──────►│
 │                              │                          │   (name, type, topology, │
 │                              │                          │    priority)             │
 │                              │                          │                          ├─ assigns partition
 │                              │                          │                          │
 ├─ kubectl delete JobSet ─────►│                          │                          │
 │                              ├─ deletes Job ───────────►│                          │
 │                              │                          ├─ deletes Slice CR ──────►│
 │                              │                          │                          ├─ releases partition
```

1. The provisioner watches **Jobs** (created by JobSets) and **StatefulSets** with the label `cloud.google.com/gke-tpu-slice`.
2. When a matching workload appears, the provisioner extracts:
   - **Slice name** from `cloud.google.com/gke-tpu-slice` label
   - **Accelerator type** from `cloud.google.com/gke-tpu-accelerator` label
   - **Topology** from `cloud.google.com/gke-tpu-topology` label
   - **Priority** from the workload's `PriorityClassName` (resolved to integer value)
3. The provisioner creates a cluster-scoped `Slice` CR with the extracted metadata.
4. A **finalizer** (`cloud.google.com/delete-slice-cr`) is added to the workload, ensuring cleanup when the workload is deleted.
5. The **webhook** (optional) validates JobSets at admission time:
   - Rejects TPU workloads missing required labels
   - Prevents duplicate `slice-prefix` across JobSets
   - Enforces immutability of TPU labels after creation
   - Injects the slice name into Jobs and pod templates

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Slice Provisioner                            │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Job Reconciler    │  │ StatefulSet      │  │ Webhook       │  │
│  │                   │  │ Reconciler       │  │ (Validating + │  │
│  │ • Watch Jobs      │  │                  │  │  Mutating)    │  │
│  │ • Create Slice CR │  │ • Watch STS      │  │               │  │
│  │ • Manage finalizer│  │ • Create Slice CR│  │ • Label check │  │
│  │ • Priority lookup │  │ • Manage finalizer│ │ • Dedup check │  │
│  └──────────────────┘  └──────────────────┘  │ • Immutability│  │
│                                               │ • Inject slice│  │
│  ┌──────────────────┐  ┌──────────────────┐  │   name        │  │
│  │ Metrics          │  │ Leader Election  │  └───────────────┘  │
│  │ :8080            │  │ (coordination/v1)│                     │
│  └──────────────────┘  └──────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
         │                       │                      │
         ▼                       ▼                      ▼
    Slice CRD              PriorityClass          JobSet / Job
  (accelerator.gke.io)   (scheduling.k8s.io)     (batch/v1)
```

### Dependencies

The Slice Provisioner is one component in a multi-controller stack:

| Component | Purpose | Required? |
|-----------|---------|-----------|
| **PartitionTree** | Reflects GCE partition topology into CRs | Yes (scheduler depends on it) |
| **Slice Scheduler** | Assigns partitions to Slice CRs, binds pods | Yes |
| **Slice Provisioner** | Creates/deletes Slice CRs for workloads | Yes (this component) |
| **JobSet Controller** | Manages JobSet → Job lifecycle | Yes (for JobSet workloads) |
| **Slice Controller** | Calls GCE to configure accelerator topology | Yes (for real hardware) |

Install order: PartitionTree → Slice Scheduler → JobSet Controller → **Slice Provisioner**

## Prerequisites

- Go 1.24+ (for building from source)
- Docker 17.03+ (for building the manager image)
- kubectl v1.30+ configured against your target cluster
- Helm v3+ (for deployment)
- A GKE cluster with:
  - TPU node pools with `cloud.google.com/gke-accelerator-topology-mode: PROVISION_ONLY`
  - A **CPU node pool** with `cloud-platform` OAuth scope (for the controller)
  - The `Slice` CRD installed (from the slice-scheduler repo)
  - The PartitionTree and Slice Scheduler controllers deployed

> **Note:** The CPU node pool running the controller must have the `cloud-platform` OAuth scope. Without it, controllers that call GCE APIs will get 403 errors even with correct IAM permissions.

## Quick start

### Install into a cluster

**1. Build and push the image:**

```bash
export PROJECT=YOUR_PROJECT
export IMG=us-central1-docker.pkg.dev/${PROJECT}/YOUR_REPO/slice-provisioner:v0.7.0
make docker-build docker-push IMG=$IMG
```

> **Tip:** Ensure the Artifact Registry repository exists first:
> ```bash
> gcloud artifacts repositories create YOUR_REPO \
>   --repository-format=docker \
>   --location=us-central1 \
>   --project=$PROJECT
> ```

**2. Install the Slice CRD** (if not already installed by the slice-scheduler):

```bash
make install
```

This installs the `slices.accelerator.gke.io` CRD.

**3. Deploy via Helm:**

```bash
helm install slice-provisioner charts/slice-provisioner \
  --set image.repository=${IMG%:*} \
  --set image.tag=${IMG##*:} \
  --set "nodeSelector.cloud\.google\.com/gke-nodepool=cpu-pool" \
  --namespace slice-provisioner-system \
  --create-namespace
```

This creates:
- The **`slice-provisioner-system`** namespace
- A ServiceAccount with ClusterRole for managing Slices, Jobs, StatefulSets, and JobSets
- The **`slice-provisioner`** Deployment (the controller)
- Validating and Mutating webhooks for JobSet admission
- A self-signed certificate for the webhook TLS
- A metrics Service on port 8080

**Key Helm values:**

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` | `slice-provisioner` | Container image repository |
| `image.tag` | Chart `appVersion` | Image tag |
| `webhook.enabled` | `true` | Enable admission webhooks |
| `metrics.enabled` | `true` | Enable Prometheus metrics endpoint |
| `metrics.tls` | `false` | Enable TLS for metrics |
| `nodeSelector` | `{}` | Node selector for the controller pod |
| `debug.enabled` | `false` | Enable debug logging |
| `replicaCount` | `1` | Number of replicas (leader election handles HA) |

### Verify

```bash
# 1. Check the CRD is installed
kubectl get crd slices.accelerator.gke.io
# Expected: slices.accelerator.gke.io   <date>

# 2. Controller pod should be Running
kubectl -n slice-provisioner-system get pods
# Expected: slice-provisioner-...   1/1   Running

# 3. Check controller logs
kubectl -n slice-provisioner-system logs deployment/slice-provisioner --tail=10
# Expected: "Starting workers" for job and statefulset controllers

# 4. Verify webhook is ready
kubectl get endpointslice -l kubernetes.io/service-name=slice-provisioner-webhook-service \
  -n slice-provisioner-system
# Expected: endpoints with ready=true

# 5. Create a test workload and check Slice CR is created
kubectl apply -f tests/test-single-cube-jobset.yaml
sleep 30
kubectl get slices
# Expected: test-single-cube-worker-0   tpu7x   4x4x4   ACTIVE   <age>
kubectl delete jobset test-single-cube
```

### Upgrade

```bash
helm upgrade slice-provisioner charts/slice-provisioner \
  --set image.repository=${IMG%:*} \
  --set image.tag=${IMG##*:} \
  --set "nodeSelector.cloud\.google\.com/gke-nodepool=cpu-pool" \
  --namespace slice-provisioner-system
```

### Uninstall

```bash
# Remove the controller, RBAC, and webhooks
helm uninstall slice-provisioner --namespace slice-provisioner-system

# Remove the namespace
kubectl delete namespace slice-provisioner-system

# Remove CRDs (WARNING: this deletes all Slice CRs)
make uninstall
```

### Troubleshooting & Gotchas

#### Slice CR not created

If a workload is created but no Slice CR appears:

1. **Check the provisioner logs** for errors:
   ```bash
   kubectl -n slice-provisioner-system logs deployment/slice-provisioner --tail=20
   ```

2. **Verify labels** — the workload must have all three TPU labels:
   ```yaml
   labels:
     cloud.google.com/gke-tpu-slice: "my-slice-name"
     cloud.google.com/gke-tpu-accelerator: "tpu7x"
     cloud.google.com/gke-tpu-topology: "4x4x4"
   ```

3. **Check if the webhook is injecting the slice name** — the webhook mutates Jobs to add the `gke-tpu-slice` label from the parent JobSet.

#### RBAC errors (403 Forbidden)

If the controller logs show `slices.accelerator.gke.io is forbidden`:
- Verify the ClusterRole has the correct API group (`accelerator.gke.io`, not `slice.tpu.google.com`)
- Check with: `kubectl get clusterrole <release-name>-slice-provisioner -o yaml`

#### Webhook blocking operations

The validating webhook uses `failurePolicy: Fail`. If the provisioner is down, JobSet and Job creation/updates may be blocked. To temporarily bypass:
```bash
# Check webhook status
kubectl get validatingwebhookconfigurations | grep slice-provisioner

# Emergency: delete the webhook (JobSets won't be validated)
kubectl delete validatingwebhookconfiguration slice-provisioner-validating-webhook-configuration
```

#### Image pull issues

Ensure your cluster's node service account has read access to the Artifact Registry:
```bash
gcloud artifacts repositories add-iam-policy-binding YOUR_REPO \
  --location=us-central1 \
  --member="serviceAccount:YOUR_NODE_SA@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

#### NodeSelector mismatch

The controller uses a `nodeSelector` to run on CPU nodes. If the pod is Pending:
```bash
# Check what node pools exist
kubectl get nodes -L cloud.google.com/gke-nodepool
# Ensure the nodeSelector matches an actual pool name
```

## CRD reference

The provisioner creates `Slice` CRs defined by the slice-scheduler:

```yaml
apiVersion: accelerator.gke.io/v1beta1
kind: Slice                    # cluster-scoped
metadata:
  name: my-workload-worker-0   # derived from Job name
  annotations:
    cloud.google.com/slice-priority: "500"  # from PriorityClass
    cloud.google.com/managed-by: slice-scheduler
  finalizers:
    - cloud.google.com/delete-slice-cr
spec:
  type: tpu7x                  # from gke-tpu-accelerator label
  topology: "4x4x4"            # from gke-tpu-topology label
  partitionIds: []              # filled by scheduler
status:
  conditions:
    - type: Ready
      status: "True"
      reason: SliceReady
```

### Labels used

| Label | Applied to | Purpose |
|-------|-----------|---------|
| `cloud.google.com/gke-tpu-slice` | Job, Pod | Slice name identifier |
| `cloud.google.com/gke-tpu-accelerator` | Job, Pod | TPU type (e.g., `tpu7x`) |
| `cloud.google.com/gke-tpu-topology` | Job, Pod | Topology (e.g., `4x4x4`) |
| `cloud.google.com/slice-prefix` | JobSet | Links JobSet to its slice group |

## Configuration

The provisioner accepts the following command-line flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--leader-elect` | `true` | Enable leader election for HA |
| `--health-probe-bind-address` | `:8081` | Health probe listen address |
| `--metrics-bind-address` | `:8080` | Metrics endpoint listen address |
| `--enable-webhook` | `true` | Enable admission webhooks |
| `--enable-http2` | `false` | Enable HTTP/2 (disabled by default for security) |
| `--sync-period` | `5m` | Minimum interval for periodic cache resync to catch missed events |

## Development

### Run tests

```bash
# Unit + integration tests (uses envtest with real etcd + kube-apiserver)
make test

# Lint
make lint

# Build binary
make build
```

### Run locally against a cluster

```bash
# 1. Install CRDs
make install

# 2. Run the controller locally (uses your kubeconfig)
go run ./cmd --enable-webhook=false
```

> **Note:** Webhooks require TLS certificates, so they are disabled for local development. Use `--enable-webhook=false`.

### Project structure

```
slice-provisioner/
├── apis/                    # CRD type definitions
│   ├── partitiontree/       # PartitionTree types (imported)
│   └── slice/               # Slice types (v1beta1)
├── charts/                  # Helm chart
│   └── slice-provisioner/
├── cmd/                     # Manager entrypoint
│   └── main.go
├── config/                  # Kustomize base configs
│   ├── crd/                 # CRD manifests
│   ├── default/             # Default kustomization
│   ├── manager/             # Deployment spec
│   └── rbac/                # RBAC rules
├── internal/
│   ├── controller/          # Reconcilers (Job, StatefulSet)
│   ├── labels/              # Shared label constants
│   └── webhook/             # Admission webhooks
├── test/                    # E2E test infrastructure
└── vendor/                  # Go module vendor
```

### Key source files

| File | Purpose |
|------|---------|
| `internal/controller/jobset_reconciler.go` | Job → Slice lifecycle (main path) |
| `internal/controller/statefulset_reconciler.go` | StatefulSet → Slice lifecycle |
| `internal/controller/common.go` | Shared helpers (createSliceCR, reconcileFinalizer, priorityValue) |
| `internal/controller/constants.go` | Label and annotation constants |
| `internal/controller/metrics.go` | Prometheus metric definitions |
| `internal/webhook/jobset_webhook.go` | JobSet validating + mutating webhook |
| `internal/labels/labels.go` | Shared label constants (used by controller + webhook) |

## Metrics

The provisioner exposes Prometheus metrics on `:8080/metrics`:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slice_provisioner_reconciliation_errors_total` | Counter | `error_type` | Reconciliation errors by type |
| `slice_provisioner_slice_operations_total` | Counter | `operation`, `status` | Slice operations (Create/Delete) with result (Success/Error) |
| `slice_provisioner_slice_creation_processing_milliseconds` | Histogram | — | Time from workload creation to successful Slice creation |

### Prometheus monitoring

The Helm chart includes a ServiceMonitor template. Enable it via Helm values:

```bash
helm install slice-provisioner charts/slice-provisioner \
  --set metrics.enabled=true
```

## Reporting Security Issues

Eligibility for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security) is determined by the [Google Open Source Software Vulnerability Reward Program Rules](https://bughunters.google.com/about/rules/open-source/google-open-source-software-vulnerability-reward-program-rules).

## Contributing

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on setting up your development environment and running tests.

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## License

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