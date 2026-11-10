//go:build e2e
// +build e2e

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/slice-provisioner/test/utils"
)

var (
	// Optional Environment Variables:
	// - CERT_MANAGER_INSTALL_SKIP=true: Skips CertManager installation during test setup.
	// - USE_EXISTING_CLUSTER=true: Skips building/deploying to Kind and runs against an existing cluster.
	// These variables are useful if CertManager or a target cluster is already provisioned,
	// avoiding re-installation and conflicts.
	skipCertManagerInstall = os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true"
	useExistingCluster     = os.Getenv("USE_EXISTING_CLUSTER") == "true"
	// isCertManagerAlreadyInstalled will be set true when CertManager CRDs be found on the cluster
	isCertManagerAlreadyInstalled = false

	// projectImage is the name of the image which will be build and loaded
	// with the code source changes to be tested.
	projectImage = "example.com/slice-provisioner:v0.0.1"
)

// TestE2E runs the end-to-end (e2e) test suite for the project. These tests execute in an isolated,
// temporary environment to validate project changes with the purpose of being used in CI jobs.
// The default setup requires Kind, builds/loads the Manager Docker image locally, and installs
// CertManager.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting slice-provisioner integration test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	if useExistingCluster {
		By("using pre-provisioned cluster and controllers (USE_EXISTING_CLUSTER set)")
		return
	}
	By("building the manager(Operator) image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager(Operator) image")

	// TODO(user): If you want to change the e2e test vendor from Kind, ensure the image is
	// built and available before running the tests. Also, remove the following block.
	By("loading the manager(Operator) image on Kind")
	err = utils.LoadImageToKindClusterWithName(projectImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager(Operator) image into Kind")

	// The tests-e2e are intended to run on a temporary cluster that is created and destroyed for testing.
	// To prevent errors when tests run in environments with CertManager already installed,
	// we check for its presence before execution.
	// Setup CertManager before the suite if not skipped and if not already installed
	if !skipCertManagerInstall {
		By("checking if cert manager is installed already")
		isCertManagerAlreadyInstalled = utils.IsCertManagerCRDsInstalled()
		if !isCertManagerAlreadyInstalled {
			_, _ = fmt.Fprintf(GinkgoWriter, "Installing CertManager...\n")
			Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "WARNING: CertManager is already installed. Skipping installation...\n")
		}
	}

	// Deploy the controller so it is available for all test suites.
	// This must happen at suite level because Ginkgo may run Describe blocks
	// in any order, and all of them need the controller running.
	By("creating manager namespace")
	cmd = exec.Command("bash", "-c",
		fmt.Sprintf("kubectl create ns %s --dry-run=client -o yaml | kubectl apply -f -", namespace))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

	// The slice-controller (if running on a shared cluster) installs a
	// validating webhook that enforces topology allow-lists. This blocks the
	// provisioner from creating Slice CRs with test topologies. Delete it
	// so E2E tests exercise the provisioner in isolation.
	By("removing external validating webhooks that may block Slice creation")
	cmd = exec.Command("kubectl", "delete", "validatingwebhookconfiguration",
		"slice-controller-validating-webhook-configuration", "--ignore-not-found=true")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to remove slice-controller webhook")

	By("waiting for webhook certificate to be issued")
	cmd = exec.Command("kubectl", "wait", "certificate/slice-provisioner-serving-cert",
		"--for", "condition=Ready",
		"--namespace", namespace,
		"--timeout", "60s",
	)
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed waiting for webhook certificate")

	By("waiting for controller-manager deployment to be available")
	cmd = exec.Command("kubectl", "wait", "deployment/slice-provisioner-controller-manager",
		"--for", "condition=Available",
		"--namespace", namespace,
		"--timeout", "60s",
	)
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Controller-manager deployment not available")

	By("waiting for webhook endpoint to be ready")
	// The webhook server needs a few seconds after pod startup to begin serving.
	// Probe it with a dry-run StatefulSet create until it responds.
	Eventually(func() error {
		probe := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
		probe.Stdin = strings.NewReader(`apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: webhook-probe
  namespace: default
spec:
  replicas: 0
  selector:
    matchLabels:
      app: probe
  template:
    metadata:
      labels:
        app: probe
    spec:
      containers:
      - name: probe
        image: busybox
`)
		_, err := utils.Run(probe)
		return err
	}, 60*time.Second, 2*time.Second).Should(Succeed(), "Webhook endpoint not ready")
})

var _ = AfterSuite(func() {
	// Best-effort diagnostic dump: if anything failed, pull the
	// controller-manager pod logs so the CI log shows exactly what the
	// controller did. Cheap no-op on green runs and safe on existing clusters.
	if CurrentSpecReport().Failed() {
		By("dumping controller-manager logs (some spec failed)")
		dumpControllerLogs()
	}

	if useExistingCluster {
		return
	}

	// Undeploy the controller-manager and clean up.
	By("cleaning up the curl pod for metrics")
	cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found=true")
	_, _ = utils.Run(cmd)

	By("undeploying the controller-manager")
	cmd = exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	By("removing manager namespace")
	cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found=true")
	_, _ = utils.Run(cmd)

	// Teardown CertManager after the suite if not skipped and if it was not already installed
	if !skipCertManagerInstall && !isCertManagerAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Uninstalling CertManager...\n")
		utils.UninstallCertManager()
	}
})

// dumpControllerLogs streams the current controller-manager pod logs to
// GinkgoWriter so a CI failure includes them without having to re-run
// kubectl logs by hand.
func dumpControllerLogs() {
	const namespace = "slice-provisioner-system"
	out, err := utils.Run(exec.Command("kubectl", "-n", namespace,
		"get", "pods", "-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}"))
	if err != nil || out == "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "dumpControllerLogs: no controller pod: %v\n", err)
		return
	}
	logs, err := utils.Run(exec.Command("kubectl", "-n", namespace,
		"logs", out, "--tail=500"))
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "dumpControllerLogs: kubectl logs: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "=== controller-manager logs (%s) ===\n%s\n=== end logs ===\n",
		out, logs)
}
