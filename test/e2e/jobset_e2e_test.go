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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ai-on-gke/slice-provisioner/test/utils"
)

// JobSet-specific label constants not already defined in slice_provisioner_test.go.
// The production values come from internal/labels/labels.go.
const (
	slicePrefixLabel       = "cloud.google.com/slice-prefix"
	jobSetNameLabelE2E     = "jobset.sigs.k8s.io/jobset-name"
	replicatedJobNameLabel = "jobset.sigs.k8s.io/replicatedjob-name"
	jobIndexLabel          = "jobset.sigs.k8s.io/job-index"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

// applyJobSet creates a JobSet via kubectl apply. The JobSet includes a single
// replicatedJob ("worker") with TPU resources. Slice type and topology labels
// are placed on the Job template metadata (required by the validating webhook).
// It returns an error so callers can use Expect() or check for webhook rejections.
func applyJobSet(name, ns string, labels map[string]string) error {
	var labelLines bytes.Buffer
	for k, v := range labels {
		fmt.Fprintf(&labelLines, "      %s: %q\n", k, v)
	}
	manifest := fmt.Sprintf(`apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: %s
  namespace: %s
  labels:
%s
spec:
  replicatedJobs:
  - name: worker
    replicas: 1
    template:
      metadata:
        labels:
          %s: "v6e"
          %s: "2x2"
      spec:
        parallelism: 1
        completions: 1
        template:
          spec:
            terminationGracePeriodSeconds: 1
            containers:
            - name: pause
              image: registry.k8s.io/pause:3.10
              resources:
                limits:
                  google.com/tpu: "4"
            restartPolicy: Never
`, name, ns, labelLines.String(), sliceTypeLabel, sliceTopologyLabel)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader([]byte(manifest))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply jobset %s: %w (output: %s)", name, err, out)
	}
	return nil
}

// applyJobSetNoTPU creates a JobSet without TPU resources or slice labels —
// used to test that plain JobSets are not provisioned.
func applyJobSetNoTPU(name, ns string, labels map[string]string) error {
	var labelLines bytes.Buffer
	for k, v := range labels {
		fmt.Fprintf(&labelLines, "      %s: %q\n", k, v)
	}
	manifest := fmt.Sprintf(`apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: %s
  namespace: %s
  labels:
%s
spec:
  replicatedJobs:
  - name: worker
    replicas: 1
    template:
      spec:
        parallelism: 1
        completions: 1
        template:
          spec:
            terminationGracePeriodSeconds: 1
            containers:
            - name: pause
              image: registry.k8s.io/pause:3.10
            restartPolicy: Never
`, name, ns, labelLines.String())

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader([]byte(manifest))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply jobset %s: %w (output: %s)", name, err, out)
	}
	return nil
}

// deleteJobSet deletes a JobSet by name.
func deleteJobSet(name, ns string) {
	cmd := exec.Command("kubectl", "delete", "jobset", name,
		"-n", ns, "--ignore-not-found=true", "--wait=false")
	_, _ = utils.Run(cmd)
}

// getJobSetField retrieves a jsonpath field from a JobSet.
func getJobSetField(name, ns, jsonpath string) (string, error) {
	cmd := exec.Command("kubectl", "get", "jobset", name,
		"-n", ns, "-o", "jsonpath="+jsonpath)
	return utils.Run(cmd)
}

// getJobField retrieves a jsonpath field from a Job.
func getJobField(name, ns, jsonpath string) (string, error) {
	cmd := exec.Command("kubectl", "get", "job", name,
		"-n", ns, "-o", "jsonpath="+jsonpath)
	return utils.Run(cmd)
}

// listJobsForJobSet returns the names of Jobs owned by a given JobSet.
// kubectl jsonpath {.items[*].metadata.name} returns space-separated names
// on a single line, so we use strings.Fields() for correct parsing.
func listJobsForJobSet(jobSetName, ns string) ([]string, error) {
	cmd := exec.Command("kubectl", "get", "jobs",
		"-n", ns,
		"-l", fmt.Sprintf("%s=%s", jobSetNameLabelE2E, jobSetName),
		"-o", "jsonpath={.items[*].metadata.name}")
	out, err := utils.Run(cmd)
	if err != nil {
		return nil, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return nil, nil
	}
	return strings.Fields(out), nil
}

// expectedSliceName computes the slice name produced by the mutating webhook.
// It mirrors the truncation logic in jobset_mutator.go:
//
//	fmt.Sprintf("%.35s-%.20s-%.6s", slicePrefix, replicatedJobName, jobIndex)
func expectedSliceName(prefix, replicatedJobName, jobIndex string) string {
	return fmt.Sprintf("%.35s-%.20s-%.6s", prefix, replicatedJobName, jobIndex)
}

// deleteJobsForJobSet deletes all Jobs belonging to a JobSet, removing
// finalizers first to avoid them hanging in Terminating state.
func deleteJobsForJobSet(jobSetName, ns string) {
	cmd := exec.Command("kubectl", "get", "jobs",
		"-n", ns,
		"-l", fmt.Sprintf("%s=%s", jobSetNameLabelE2E, jobSetName),
		"-o", "jsonpath={.items[*].metadata.name}")
	out, _ := utils.Run(cmd)
	out = strings.TrimSpace(out)
	if out == "" {
		return
	}
	for _, jobName := range strings.Fields(out) {
		// Remove finalizer first to prevent the Job from being stuck.
		patchCmd := exec.Command("kubectl", "patch", "job", jobName,
			"-n", ns, "--type=merge",
			"-p", `{"metadata":{"finalizers":[]}}`)
		_, _ = utils.Run(patchCmd)
		delCmd := exec.Command("kubectl", "delete", "job", jobName,
			"-n", ns, "--ignore-not-found=true", "--wait=false")
		_, _ = utils.Run(delCmd)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test Suites
// ─────────────────────────────────────────────────────────────────────────────

var _ = Describe("JobSet lifecycle", Ordered, func() {
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
		// Clean up JobSets, child Jobs, and Slices created during tests.
		for _, jsName := range []string{
			"e2e-js-lifecycle",
			"e2e-js-nolabel",
			"e2e-js-delete",
		} {
			deleteJobsForJobSet(jsName, testNamespace)
			deleteJobSet(jsName, testNamespace)
		}
		for _, sliceName := range []string{
			expectedSliceName("e2e-js-lc", "worker", "0"),
			expectedSliceName("e2e-js-del", "worker", "0"),
		} {
			deleteSlice(sliceName)
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("creates Slice CRs when a JobSet with slice labels is created", func() {
		jsName := "e2e-js-lifecycle"
		prefix := "e2e-js-lc"
		sliceName := expectedSliceName(prefix, "worker", "0")

		By("creating a JobSet with slice labels")
		err := applyJobSet(jsName, testNamespace, map[string]string{
			slicePrefixLabel:   prefix,
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
		deleteJobsForJobSet(jsName, testNamespace)
		deleteJobSet(jsName, testNamespace)
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})

	It("does not create Slice CRs for a JobSet without slice labels", func() {
		jsName := "e2e-js-nolabel"

		By("creating a JobSet without slice labels")
		err := applyJobSetNoTPU(jsName, testNamespace, map[string]string{
			"app": "no-slice",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting briefly and verifying no Slice CR is created")
		// No slice-prefix means the mutator won't inject a slice name, so
		// no specific slice name to look for. Just verify none exist
		// with our prefix by checking all slice names.
		Consistently(func() bool {
			cmd := exec.Command("kubectl", "get", "slices",
				"-o", "jsonpath={.items[*].metadata.name}")
			out, _ := utils.Run(cmd)
			out = strings.TrimSpace(out)
			if out == "" {
				return true
			}
			// jsonpath returns space-separated names on a single line.
			for _, name := range strings.Fields(out) {
				if strings.HasPrefix(name, "e2e-js-nol") {
					return false
				}
			}
			return true
		}, 15*time.Second, 2*time.Second).Should(BeTrue())

		// Clean up.
		deleteJobSet(jsName, testNamespace)
	})

	It("cleans up Slice CRs when the JobSet is deleted", func() {
		jsName := "e2e-js-delete"
		prefix := "e2e-js-del"
		sliceName := expectedSliceName(prefix, "worker", "0")

		By("creating a JobSet with slice labels")
		err := applyJobSet(jsName, testNamespace, map[string]string{
			slicePrefixLabel:   prefix,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the Slice CR to be created")
		Eventually(func() bool { return sliceExists(sliceName) }).Should(BeTrue())

		By("deleting the JobSet")
		cmd := exec.Command("kubectl", "delete", "jobset", jsName,
			"-n", testNamespace, "--wait=true", "--timeout=120s")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the Slice CR is cleaned up")
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})
})

var _ = Describe("JobSet webhook validation", Ordered, func() {
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
		// Clean up JobSets and Slices from validation tests.
		for _, jsName := range []string{
			"e2e-js-dup-1",
			"e2e-js-dup-2",
			"e2e-js-immut",
		} {
			deleteJobsForJobSet(jsName, testNamespace)
			deleteJobSet(jsName, testNamespace)
		}
		deleteSlice(expectedSliceName("e2e-js-dup", "worker", "0"))
		deleteSlice(expectedSliceName("e2e-js-imm", "worker", "0"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("rejects a JobSet with duplicate slice-prefix", func() {
		prefix := "e2e-js-dup"

		By("creating the first JobSet with a unique slice-prefix")
		err := applyJobSet("e2e-js-dup-1", testNamespace, map[string]string{
			slicePrefixLabel:   prefix,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the first JobSet to be accepted")
		Eventually(func(g Gomega) {
			_, err := getJobSetField("e2e-js-dup-1", testNamespace, "{.metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		By("creating a second JobSet with the same slice-prefix — expecting rejection")
		err = applyJobSet("e2e-js-dup-2", testNamespace, map[string]string{
			slicePrefixLabel:   prefix,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).To(HaveOccurred(), "expected webhook to reject duplicate slice-prefix")
	})

	It("rejects mutation of immutable TPU labels on an opted-in JobSet", func() {
		jsName := "e2e-js-immut"

		By("creating a JobSet with slice labels")
		err := applyJobSet(jsName, testNamespace, map[string]string{
			slicePrefixLabel:   "e2e-js-imm",
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("attempting to change the immutable slice-type label")
		patchCmd := exec.Command("kubectl", "patch", "jobset", jsName,
			"-n", testNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"metadata":{"labels":{"%s":"tpu7x"}}}`, sliceTypeLabel))
		_, patchErr := patchCmd.CombinedOutput()
		Expect(patchErr).To(HaveOccurred(), "expected webhook to reject slice-type label change")
	})
})

var _ = Describe("JobSet finalizer management", Ordered, func() {
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
		for _, jsName := range []string{
			"e2e-js-finalizer",
		} {
			deleteJobsForJobSet(jsName, testNamespace)
			deleteJobSet(jsName, testNamespace)
		}
		deleteSlice(expectedSliceName("e2e-js-fin", "worker", "0"))
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("adds a finalizer when a Job has slice labels", func() {
		jsName := "e2e-js-finalizer"
		prefix := "e2e-js-fin"

		By("creating a JobSet with slice labels")
		err := applyJobSet(jsName, testNamespace, map[string]string{
			slicePrefixLabel:   prefix,
			sliceTypeLabel:     "v6e",
			sliceTopologyLabel: "2x2",
		})
		Expect(err).NotTo(HaveOccurred())

		By("waiting for child Jobs to appear")
		var jobNames []string
		Eventually(func(g Gomega) {
			names, err := listJobsForJobSet(jsName, testNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(names).NotTo(BeEmpty())
			jobNames = names
		}).Should(Succeed())

		By("verifying the finalizer is present on the child Job")
		Eventually(func(g Gomega) {
			out, err := getJobField(jobNames[0], testNamespace, "{.metadata.finalizers}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(finalizerName))
		}).Should(Succeed())
	})

	It("removes the finalizer after Slice cleanup on deletion", func() {
		jsName := "e2e-js-finalizer"
		prefix := "e2e-js-fin"
		sliceName := expectedSliceName(prefix, "worker", "0")

		By("verifying the Slice CR exists")
		Eventually(func() bool { return sliceExists(sliceName) }).Should(BeTrue())

		By("deleting the JobSet and waiting for it to be fully removed")
		cmd := exec.Command("kubectl", "delete", "jobset", jsName,
			"-n", testNamespace, "--wait=true", "--timeout=120s")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the JobSet is gone (child Job finalizers were removed)")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "jobset", jsName, "-n", testNamespace)
			out, err := cmd.CombinedOutput()
			g.Expect(err).To(HaveOccurred(), "expected NotFound, got: %s", out)
			g.Expect(string(out)).To(ContainSubstring("not found"))
		}).Should(Succeed())

		By("verifying the Slice CR is also cleaned up")
		Eventually(func() bool { return !sliceExists(sliceName) }, 2*time.Minute, time.Second).Should(BeTrue())
	})
})
