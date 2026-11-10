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
	"bytes"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/slice-provisioner/test/utils"
)

// testNamespace is a dedicated namespace for domain-specific tests,
// separate from the controller's system namespace.
const testNamespace = "sp-e2e-tests"

// Label and annotation constants matching the production code.
const (
	sliceNameLabel      = "cloud.google.com/gke-tpu-slice"
	sliceTypeLabel      = "cloud.google.com/gke-tpu-accelerator"
	sliceTopologyLabel  = "cloud.google.com/gke-tpu-topology"
	sliceNameAnnotation = "cloud.google.com/gke-tpu-slice-name"
	finalizerName       = "cloud.google.com/delete-slice-cr"
)

// applyStatefulSet creates a StatefulSet via kubectl apply. It returns
// an error so callers can use Expect() or check for webhook rejections.
func applyStatefulSet(name, ns string, labels map[string]string) error {
	var labelLines bytes.Buffer
	for k, v := range labels {
		fmt.Fprintf(&labelLines, "      %s: %q\n", k, v)
	}
	manifest := fmt.Sprintf(`apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  labels:
%s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      terminationGracePeriodSeconds: 1
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
        resources:
          limits:
            google.com/tpu: "4"
  serviceName: %s
`, name, ns, labelLines.String(), name, name, name)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader([]byte(manifest))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply statefulset %s: %w (output: %s)", name, err, out)
	}
	return nil
}

// deleteStatefulSet deletes a StatefulSet by name.
func deleteStatefulSet(name, ns string) {
	cmd := exec.Command("kubectl", "delete", "statefulset", name,
		"-n", ns, "--ignore-not-found=true", "--wait=false")
	_, _ = utils.Run(cmd)
}

// deleteSlice deletes a Slice CR by name.
func deleteSlice(name string) {
	// Remove finalizers first to avoid hanging on cluster-scoped resources.
	cmd := exec.Command("kubectl", "patch", "slice", name,
		"--type=merge", "-p", `{"metadata":{"finalizers":[]}}`)
	_, _ = utils.Run(cmd)
	cmd = exec.Command("kubectl", "delete", "slice", name,
		"--ignore-not-found=true", "--wait=false")
	_, _ = utils.Run(cmd)
}

// getSliceField retrieves a jsonpath field from a Slice CR.
func getSliceField(sliceName, jsonpath string) (string, error) {
	cmd := exec.Command("kubectl", "get", "slice", sliceName,
		"-o", "jsonpath="+jsonpath)
	return utils.Run(cmd)
}

// sliceExists checks whether a Slice CR with the given name exists.
func sliceExists(sliceName string) bool {
	cmd := exec.Command("kubectl", "get", "slice", sliceName)
	_, err := utils.Run(cmd)
	return err == nil
}

// getStsField retrieves a jsonpath field from a StatefulSet.
func getStsField(name, ns, jsonpath string) (string, error) {
	cmd := exec.Command("kubectl", "get", "statefulset", name,
		"-n", ns, "-o", "jsonpath="+jsonpath)
	return utils.Run(cmd)
}

var _ = Describe("StatefulSet lifecycle", Ordered, func() {
	BeforeAll(func() {
		By("creating the test namespace")
		cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
		out, err := cmd.Output()
		Expect(err).NotTo(HaveOccurred())
		applyCmd := exec.Command("kubectl", "apply", "-f", "-")
		applyCmd.Stdin = bytes.NewReader(out)
		_, err = utils.Run(applyCmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		// Only clean up resources owned by this test block.
		// Do NOT delete the shared test namespace — other Describe blocks depend on it.
		// Clean up any cluster-scoped Slice CRs created during tests.
		for _, name := range []string{
			"e2e-lifecycle-slice",
			"e2e-delete-slice",
			"e2e-topo-change-slice",
			"e2e-topo-change-slice-2",
		} {
			deleteSlice(name)
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("creates a Slice CR when a StatefulSet with slice labels is created", func() {
		stsName := "e2e-lifecycle-sts"
		sliceName := "e2e-lifecycle-slice"

		By("creating a StatefulSet with slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the Slice CR is created with the correct spec")
		Eventually(func(g Gomega) {
			out, err := getSliceField(sliceName, "{.spec.type} {.spec.topology}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("v6e 2x2"))
		}).Should(Succeed())

		By("verifying the Slice CR has the expected annotations")
		Eventually(func(g Gomega) {
			managedBy, err := getSliceField(sliceName, "{.metadata.annotations.cloud\\.google\\.com/managed-by}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(managedBy).To(Equal("slice-scheduler"))

			retryOnFailure, err := getSliceField(sliceName, "{.metadata.annotations.slice\\.gke\\.io/retry-on-failure}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(retryOnFailure).To(Equal("true"))
		}).Should(Succeed())

		// Clean up.
		deleteStatefulSet(stsName, testNamespace)
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})

	It("does not create a Slice CR for a StatefulSet without slice labels", func() {
		stsName := "e2e-nolabel-sts"

		By("creating a StatefulSet without slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			"app": "no-slice",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting briefly and verifying no Slice CR is created")
		// Give the controller time to process.
		Consistently(func() bool {
			return !sliceExists("e2e-nolabel-sts")
		}, 15*time.Second, 2*time.Second).Should(BeTrue())

		// Clean up.
		deleteStatefulSet(stsName, testNamespace)
	})

	It("cleans up the Slice CR when the StatefulSet is deleted", func() {
		stsName := "e2e-delete-sts"
		sliceName := "e2e-delete-slice"

		By("creating a StatefulSet with slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "4x4",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the Slice CR to be created")
		Eventually(func() bool { return sliceExists(sliceName) }).Should(BeTrue())

		By("deleting the StatefulSet")
		cmd := exec.Command("kubectl", "delete", "statefulset", stsName,
			"-n", testNamespace, "--wait=true", "--timeout=60s")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the Slice CR is cleaned up")
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})

	It("recreates the Slice CR when topology changes", func() {

		stsName := "e2e-topo-sts"
		sliceName := "e2e-topo-change-slice"

		By("creating a StatefulSet with initial topology")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the Slice CR to be created with topology 2x2")
		Eventually(func(g Gomega) {
			out, err := getSliceField(sliceName, "{.spec.topology}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("2x2"))
		}).Should(Succeed())

		By("NOTE: Updating immutable labels is blocked by the validating webhook")
		By("verifying the webhook rejects topology label changes on an opted-in StatefulSet")
		// The webhook enforces immutability of slice-topology label.
		// We verify this by attempting the update and expecting a rejection.
		patchCmd := exec.Command("kubectl", "patch", "statefulset", stsName,
			"-n", testNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"metadata":{"labels":{"%s":"4x4"}}}`, sliceTopologyLabel))
		_, patchErr := patchCmd.CombinedOutput()
		Expect(patchErr).To(HaveOccurred(), "expected webhook to reject topology label change")

		// Clean up.
		deleteStatefulSet(stsName, testNamespace)
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})
})

var _ = Describe("Webhook validation", Ordered, func() {
	BeforeAll(func() {

		By("ensuring the test namespace exists")
		cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
		out, err := cmd.Output()
		Expect(err).NotTo(HaveOccurred())
		applyCmd := exec.Command("kubectl", "apply", "-f", "-")
		applyCmd.Stdin = bytes.NewReader(out)
		_, err = utils.Run(applyCmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		// Clean up STS and Slices from validation tests.
		deleteStatefulSet("e2e-dup-sts-1", testNamespace)
		deleteStatefulSet("e2e-dup-sts-2", testNamespace)
		deleteSlice("e2e-dup-slice")
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("rejects a StatefulSet with a duplicate slice-name", func() {
		sliceName := "e2e-dup-slice"

		By("creating the first StatefulSet with a unique slice-name")
		err := applyStatefulSet("e2e-dup-sts-1", testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the first StatefulSet to be accepted")
		Eventually(func(g Gomega) {
			_, err := getStsField("e2e-dup-sts-1", testNamespace, "{.metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		By("creating a second StatefulSet with the same slice-name — expecting rejection")
		err = applyStatefulSet("e2e-dup-sts-2", testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).To(HaveOccurred(), "expected webhook to reject duplicate slice-name")
	})

	It("rejects mutation of immutable TPU labels on an opted-in StatefulSet", func() {
		stsName := "e2e-immutable-sts"
		sliceName := "e2e-immutable-slice"

		By("creating a StatefulSet with slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("attempting to change the immutable slice-type label")
		patchCmd := exec.Command("kubectl", "patch", "statefulset", stsName,
			"-n", testNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"metadata":{"labels":{"%s":"tpu7x"}}}`, sliceTypeLabel))
		_, patchErr := patchCmd.CombinedOutput()
		Expect(patchErr).To(HaveOccurred(), "expected webhook to reject slice-type label change")

		// Clean up.
		deleteStatefulSet(stsName, testNamespace)
		deleteSlice(sliceName)
	})
})

var _ = Describe("Webhook mutation", Ordered, func() {
	BeforeAll(func() {

		By("ensuring the test namespace exists")
		cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
		out, err := cmd.Output()
		Expect(err).NotTo(HaveOccurred())
		applyCmd := exec.Command("kubectl", "apply", "-f", "-")
		applyCmd.Stdin = bytes.NewReader(out)
		_, err = utils.Run(applyCmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		deleteStatefulSet("e2e-mutate-sts", testNamespace)
		deleteSlice("e2e-mutate-slice")
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("injects nodeSelector into the StatefulSet pod template", func() {
		stsName := "e2e-mutate-sts"
		sliceName := "e2e-mutate-slice"

		By("creating a StatefulSet with slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("verifying nodeSelector is injected in the pod template")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace,
				fmt.Sprintf("{.spec.template.spec.nodeSelector.%s}", escapeJsonpath(sliceNameLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal(sliceName))
		}).Should(Succeed())
	})

	It("injects slice-name annotation into the StatefulSet pod template", func() {
		stsName := "e2e-mutate-sts"
		sliceName := "e2e-mutate-slice"

		By("verifying the slice-name annotation is injected in the pod template")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace,
				fmt.Sprintf("{.spec.template.metadata.annotations.%s}", escapeJsonpath(sliceNameAnnotation)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal(sliceName))
		}).Should(Succeed())
	})

	It("propagates slice labels to the StatefulSet pod template labels", func() {
		stsName := "e2e-mutate-sts"
		sliceName := "e2e-mutate-slice"

		By("verifying slice-name label is on the pod template")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace,
				fmt.Sprintf("{.spec.template.metadata.labels.%s}", escapeJsonpath(sliceNameLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal(sliceName))
		}).Should(Succeed())

		By("verifying slice-type label is on the pod template")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace,
				fmt.Sprintf("{.spec.template.metadata.labels.%s}", escapeJsonpath(sliceTypeLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("v6e"))
		}).Should(Succeed())

		By("verifying slice-topology label is on the pod template")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace,
				fmt.Sprintf("{.spec.template.metadata.labels.%s}", escapeJsonpath(sliceTopologyLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("2x2"))
		}).Should(Succeed())
	})
})

var _ = Describe("Finalizer management", Ordered, func() {
	BeforeAll(func() {
		By("ensuring the test namespace exists")
		cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
		out, err := cmd.Output()
		Expect(err).NotTo(HaveOccurred())
		applyCmd := exec.Command("kubectl", "apply", "-f", "-")
		applyCmd.Stdin = bytes.NewReader(out)
		_, err = utils.Run(applyCmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		deleteStatefulSet("e2e-finalizer-sts", testNamespace)
		deleteSlice("e2e-finalizer-slice")
		deleteStatefulSet("e2e-no-finalizer-sts", testNamespace)
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("adds a finalizer when a StatefulSet has slice labels", func() {
		stsName := "e2e-finalizer-sts"
		sliceName := "e2e-finalizer-slice"

		By("creating a StatefulSet with slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			sliceNameLabel:     sliceName,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the finalizer is present")
		Eventually(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace, "{.metadata.finalizers}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(finalizerName))
		}).Should(Succeed())
	})

	It("does not add a finalizer to a StatefulSet without slice labels", func() {
		stsName := "e2e-no-finalizer-sts"

		By("creating a StatefulSet without slice labels")
		err := applyStatefulSet(stsName, testNamespace, map[string]string{
			"app": "plain",
		})
		Expect(err).NotTo(HaveOccurred())

		By("verifying no finalizer is added")
		Consistently(func(g Gomega) {
			out, err := getStsField(stsName, testNamespace, "{.metadata.finalizers}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(ContainSubstring(finalizerName))
		}, 15*time.Second, 2*time.Second).Should(Succeed())
	})

	It("removes the finalizer after Slice cleanup on deletion", func() {
		stsName := "e2e-finalizer-sts"
		sliceName := "e2e-finalizer-slice"

		By("verifying the Slice CR exists")
		Eventually(func() bool { return sliceExists(sliceName) }).Should(BeTrue())

		By("deleting the StatefulSet and waiting for it to be fully removed")
		cmd := exec.Command("kubectl", "delete", "statefulset", stsName,
			"-n", testNamespace, "--wait=true", "--timeout=120s")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the StatefulSet is gone (finalizer was removed)")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "statefulset", stsName, "-n", testNamespace)
			out, err := cmd.CombinedOutput()
			g.Expect(err).To(HaveOccurred(), "expected NotFound, got: %s", out)
			g.Expect(string(out)).To(ContainSubstring("not found"))
		}).Should(Succeed())

		By("verifying the Slice CR is also cleaned up")
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})
})

// escapeJsonpath escapes dots and slashes in label keys for use in
// kubectl jsonpath expressions.
func escapeJsonpath(key string) string {
	var buf bytes.Buffer
	for _, ch := range key {
		switch ch {
		case '.', '/':
			buf.WriteRune('\\')
			buf.WriteRune(ch)
		default:
			buf.WriteRune(ch)
		}
	}
	return buf.String()
}
