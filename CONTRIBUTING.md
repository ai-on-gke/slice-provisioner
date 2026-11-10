# How to Contribute to Slice Provisioner

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License Agreement (CLA). You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project. Head over to <https://cla.developers.google.com/> to see your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one (even if it was for a different project), you probably don't need to do it again.

## Community Guidelines

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Development Workflow

### Prerequisites

- **Go**: `v1.26.2+`
- **Docker**: `17.03+`
- **kubectl**: configured against your target cluster
- **Helm**: `v3+` (for deployment)
- **Kind**: for e2e tests (`go install sigs.k8s.io/kind@latest`)
- **controller-gen** / **kustomize** / **golangci-lint** / **addlicense**: Managed automatically via the Makefile tools directory (`bin/`)

### Getting Started

```bash
git clone https://github.com/ai-on-gke/slice-provisioner
cd slice-provisioner
make test
```

### Building and Testing

1. **Format and lint code**:
   ```bash
   make fmt
   make vet
   make lint
   ```

2. **Regenerate code and manifests** after changing API types, `+kubebuilder` RBAC markers or webhook markers:
   ```bash
   make manifests generate
   ```
   Never edit files under `config/crd/bases/`, `config/rbac/role.yaml`, `config/webhook/manifests.yaml`, or `zz_generated.deepcopy.go` by hand.

3. **Verify license headers**:
   Ensure all source files have appropriate Apache 2.0 copyright headers:
   ```bash
   make check-license
   ```
   To automatically add missing license headers:
   ```bash
   make addlicense
   ```

4. **Run unit and integration tests** (envtest with a real etcd and kube-apiserver):
   ```bash
   make test
   ```

5. **Run e2e tests** (creates a Kind cluster, deploys the controller, and tears the cluster down):
   ```bash
   make test-e2e
   ```

6. **Run presubmit checks**:
   Runs unit tests, linting, and license checks in a single pass:
   ```bash
   make presubmit
   ```

### Code Guidelines

- Follow the [Go style guide](https://google.github.io/styleguide/go/) and existing patterns. Controller and webhook tests use Ginkgo/Gomega with envtest; other unit tests use the standard `testing` package.
- Use the constants from `internal/labels/` for all label and annotation keys.
- Add unit tests for new reconciler logic.
- Keep webhook validation consistent with controller expectations.
- Use structured logging with `logger.Info()` / `logger.Error()`.
