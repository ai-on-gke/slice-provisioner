//go:build e2e
// +build e2e

/*
Copyright 2026.

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

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/test/utils"
)

const (
	lwsVersion         = "v0.7.0"
	lwsManifestURL     = "https://github.com/kubernetes-sigs/lws/releases/download/" + lwsVersion + "/manifests.yaml"
	lwsSecondNamespace = "sp-e2e-lws-other"
)

type lwsSpec struct {
	name           string
	namespace      string
	prefix         string
	topology       string
	maxUnavailable string
	replicas       int
	size           int
	partition      *int
}

func intPtr(v int) *int {
	return &v
}

func installLWSController() error {
	cmd := exec.Command("kubectl", "apply", "--server-side", "-f", lwsManifestURL)
	if _, err := utils.Run(cmd); err != nil {
		return err
	}
	waitCmd := exec.Command("kubectl", "wait", "deployment/lws-controller-manager",
		"--for", "condition=Available",
		"--namespace", "lws-system",
		"--timeout", "3m",
	)
	_, err := utils.Run(waitCmd)
	return err
}

func uninstallLWSController() {
	cmd := exec.Command("kubectl", "delete", "-f", lwsManifestURL, "--ignore-not-found=true", "--timeout=60s")
	_, _ = utils.Run(cmd)
}

func applyLWS(spec lwsSpec) error {
	const sliceType = "tpu7x"
	ns := spec.namespace
	if ns == "" {
		ns = testNamespace
	}
	topo := spec.topology
	if topo == "" {
		topo = "2x2x1"
	}
	maxUnavailable := spec.maxUnavailable
	if maxUnavailable == "" {
		maxUnavailable = "100%"
	}
	var partitionLine string
	if spec.partition != nil {
		partitionLine = fmt.Sprintf("\n      partition: %d", *spec.partition)
	}

	manifest := fmt.Sprintf(`apiVersion: leaderworkerset.x-k8s.io/v1
kind: LeaderWorkerSet
metadata:
  name: %s
  namespace: %s
spec:
  replicas: %d
  rolloutStrategy:
    type: RollingUpdate
    rollingUpdateConfiguration:
      maxUnavailable: %s%s
  leaderWorkerTemplate:
    size: %d
    leaderTemplate:
      metadata:
        labels:
          %s: %q
          %s: %q
          %s: %q
      spec:
        terminationGracePeriodSeconds: 1
        containers:
        - name: leader
          image: registry.k8s.io/pause:3.10
          resources:
            limits:
              google.com/tpu: "4"
    workerTemplate:
      metadata:
        labels:
          %s: %q
          %s: %q
          %s: %q
      spec:
        terminationGracePeriodSeconds: 1
        containers:
        - name: worker
          image: registry.k8s.io/pause:3.10
          resources:
            limits:
              google.com/tpu: "4"
`, spec.name, ns, spec.replicas, maxUnavailable, partitionLine, spec.size,
		labels.SlicePrefixLabel, spec.prefix, labels.SliceTypeLabel, sliceType, labels.SliceTopologyLabel, topo,
		labels.SlicePrefixLabel, spec.prefix, labels.SliceTypeLabel, sliceType, labels.SliceTopologyLabel, topo)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader([]byte(manifest))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply lws %s/%s: %w (output: %s)", ns, spec.name, err, out)
	}
	return nil
}

func deleteLWS(name, ns string) {
	cmd := exec.Command("kubectl", "delete", "leaderworkerset", name,
		"-n", ns, "--ignore-not-found=true", "--timeout=30s")
	_, _ = utils.Run(cmd)
}

func cleanupStatefulSet(name, ns string) {
	patchCmd := exec.Command("kubectl", "patch", "statefulset", name,
		"-n", ns, "--type=merge", "-p", `{"metadata":{"finalizers":[]}}`)
	_, _ = utils.Run(patchCmd)
	delCmd := exec.Command("kubectl", "delete", "statefulset", name,
		"-n", ns, "--ignore-not-found=true", "--timeout=30s")
	_, _ = utils.Run(delCmd)
}

func cleanupLWS(name, ns string) {
	selector := labels.LWSNameLabel + "=" + name
	listCmd := exec.Command("kubectl", "get", "statefulset", "-n", ns,
		"-l", selector, "-o", "jsonpath={.items[*].metadata.name}")
	if out, err := utils.Run(listCmd); err == nil {
		for _, stsName := range strings.Fields(out) {
			patchCmd := exec.Command("kubectl", "patch", "statefulset", stsName,
				"-n", ns, "--type=merge", "-p", `{"metadata":{"finalizers":[]}}`)
			_, _ = utils.Run(patchCmd)
		}
	}
	delLWSCmd := exec.Command("kubectl", "delete", "leaderworkerset", name,
		"-n", ns, "--ignore-not-found=true", "--timeout=30s")
	_, _ = utils.Run(delLWSCmd)

	if out, err := utils.Run(listCmd); err == nil {
		for _, stsName := range strings.Fields(out) {
			cleanupStatefulSet(stsName, ns)
		}
	}
}

func getPodField(name, ns, jsonpath string) (string, error) {
	cmd := exec.Command("kubectl", "get", "pod", name,
		"-n", ns, "-o", "jsonpath="+jsonpath)
	return utils.Run(cmd)
}

func ensureE2ENamespace(ns string) {
	cmd := exec.Command("kubectl", "create", "ns", ns, "--dry-run=client", "-o", "yaml")
	out, err := cmd.Output()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	applyCmd := exec.Command("kubectl", "apply", "-f", "-")
	applyCmd.Stdin = bytes.NewReader(out)
	_, err = utils.Run(applyCmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

var _ = Describe("LeaderWorkerSet size=1 lifecycle, rollouts, and validation", Ordered, func() {
	var lwsPreInstalled bool
	scale61Prefix := "s" + strings.Repeat("p", 60)

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(35 * time.Second)
		SetDefaultEventuallyPollingInterval(500 * time.Millisecond)
		DeferCleanup(func() {
			SetDefaultEventuallyTimeout(2 * time.Minute)
			SetDefaultEventuallyPollingInterval(time.Second)
		})
		ensureE2ENamespace(testNamespace)
		ensureE2ENamespace(lwsSecondNamespace)

		By("checking if LeaderWorkerSet CRD is already installed")
		crdCmd := exec.Command("kubectl", "get", "crd", "leaderworkersets.leaderworkerset.x-k8s.io")
		if _, err := utils.Run(crdCmd); err == nil {
			lwsPreInstalled = true
		} else {
			By("installing upstream LeaderWorkerSet controller " + lwsVersion)
			Expect(installLWSController()).To(Succeed())
		}

		By("waiting for LeaderWorkerSet webhook to accept dry-run requests")
		Eventually(func() error {
			probe := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
			probe.Stdin = strings.NewReader(`apiVersion: leaderworkerset.x-k8s.io/v1
kind: LeaderWorkerSet
metadata:
  name: lws-webhook-probe
  namespace: default
spec:
  replicas: 1
  leaderWorkerTemplate:
    size: 1
    workerTemplate:
      spec:
        containers:
        - name: c
          image: registry.k8s.io/pause:3.10
`)
			_, err := utils.Run(probe)
			return err
		}, 90*time.Second, 2*time.Second).Should(Succeed(), "LWS webhook not ready")
	})

	AfterAll(func() {
		cleanupLWS("e2e-lws-size1", testNamespace)
		cleanupLWS("e2e-lws-dup-a", testNamespace)
		cleanupStatefulSet("e2e-lws-dup-b", lwsSecondNamespace)
		cleanupStatefulSet("e2e-lws-size2-leader", lwsSecondNamespace)
		cleanupStatefulSet("e2e-standalone-dup", lwsSecondNamespace)
		cleanupStatefulSet("e2e-lws-scale63", lwsSecondNamespace)
		for i := range 4 {
			deleteSlice(fmt.Sprintf("e2e-lws-s1-%d", i))
		}
		deleteSlice("e2e-lws-dup-0")
		for i := range 11 {
			deleteSlice(fmt.Sprintf("%s-%d", scale61Prefix, i))
		}
		delNS := exec.Command("kubectl", "delete", "ns", lwsSecondNamespace, "--ignore-not-found=true", "--wait=false")
		_, _ = utils.Run(delNS)
		if !lwsPreInstalled {
			uninstallLWSController()
		}
	})

	It("provisions per-replica Slices, mutates leader Pods, scales cleanly, and hands Slices across rollouts", func() {
		const (
			lwsName = "e2e-lws-size1"
			prefix  = "e2e-lws-s1"
		)

		By("creating a real LeaderWorkerSet with size=1 and replicas=3")
		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x1",
			replicas: 3,
			size:     1,
		})).To(Succeed())

		By("verifying LWS creates a shared leader StatefulSet without stamping <prefix>-0 on it or its template")
		Eventually(func(g Gomega) {
			replicas, err := getStsField(lwsName, testNamespace, "{.spec.replicas}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(replicas).To(Equal("3"))

			topSlice, err := getStsField(lwsName, testNamespace,
				fmt.Sprintf("{.metadata.labels.%s}", escapeJsonpath(labels.SliceNameLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(topSlice).To(BeEmpty())

			tmplSlice, err := getStsField(lwsName, testNamespace,
				fmt.Sprintf("{.spec.template.metadata.labels.%s}", escapeJsonpath(labels.SliceNameLabel)))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(tmplSlice).To(BeEmpty())
		}).Should(Succeed())

		By("verifying all 3 per-replica Slice CRs are provisioned with namespace-scoped owner labels")
		rvBySlice := make(map[string]string, 3)
		for i := range 3 {
			sName := fmt.Sprintf("%s-%d", prefix, i)
			Eventually(func(g Gomega) {
				specOut, err := getSliceField(sName, "{.spec.type} {.spec.topology}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(specOut).To(Equal("tpu7x 2x2x1"))

				ownerOut, err := getSliceField(sName, fmt.Sprintf(
					"{.metadata.labels.%s}/{.metadata.labels.%s}/{.metadata.labels.%s}",
					escapeJsonpath(labels.SliceOwnerNamespaceLabel),
					escapeJsonpath(labels.SliceOwnerNameLabel),
					escapeJsonpath(labels.SliceOwnerKindLabel),
				))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ownerOut).To(Equal(fmt.Sprintf("%s/%s/StatefulSet", testNamespace, lwsName)))

				rv, err := getSliceField(sName, "{.metadata.resourceVersion}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rv).NotTo(BeEmpty())
				rvBySlice[sName] = rv
			}).Should(Succeed())
		}

		By("verifying PodMutator stamps label, annotation, and nodeSelector on each leader Pod")
		podMutatedJSONPath := fmt.Sprintf(
			"{.metadata.labels.%s}/{.metadata.annotations.%s}/{.spec.nodeSelector.%s}",
			escapeJsonpath(labels.SliceNameLabel),
			escapeJsonpath(labels.SliceNameAnnotation),
			escapeJsonpath(labels.SliceNameLabel),
		)
		for i := range 3 {
			podName := fmt.Sprintf("%s-%d", lwsName, i)
			wantSlice := fmt.Sprintf("%s-%d", prefix, i)
			wantMutated := fmt.Sprintf("%s/%s/%s", wantSlice, wantSlice, wantSlice)
			Eventually(func(g Gomega) {
				got, err := getPodField(podName, testNamespace, podMutatedJSONPath)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(wantMutated))
			}).Should(Succeed())
		}

		By("scaling LWS replicas 3 -> 4 and verifying Slice-3 is added without churning Slices 0..2")
		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x1",
			replicas: 4,
			size:     1,
		})).To(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(sliceExists(prefix + "-3")).To(BeTrue())
		}).Should(Succeed())
		for i := range 3 {
			sName := fmt.Sprintf("%s-%d", prefix, i)
			rv, err := getSliceField(sName, "{.metadata.resourceVersion}")
			Expect(err).NotTo(HaveOccurred())
			Expect(rv).To(Equal(rvBySlice[sName]), "existing Slice %s should not be churned on scale-up", sName)
		}

		By("scaling LWS replicas 4 -> 2 and verifying Slices 2 and 3 are deleted while Slices 0 and 1 remain")
		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x1",
			replicas: 2,
			size:     1,
		})).To(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(sliceExists(prefix + "-2")).To(BeFalse())
			g.Expect(sliceExists(prefix + "-3")).To(BeFalse())
			g.Expect(sliceExists(prefix + "-0")).To(BeTrue())
			g.Expect(sliceExists(prefix + "-1")).To(BeTrue())
		}).Should(Succeed())

		By("rolling out a template topology change (2x2x1 -> 2x2x2) via the webhook and verifying Slices recreate")
		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x2",
			replicas: 2,
			size:     1,
		})).To(Succeed())
		for i := range 2 {
			sName := fmt.Sprintf("%s-%d", prefix, i)
			Eventually(func(g Gomega) {
				topo, err := getSliceField(sName, "{.spec.topology}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(topo).To(Equal("2x2x2"))
			}).Should(Succeed())
		}

		By("rolling size 1 -> 2 with explicit partition=1: Slice-1 hands over while Slice-0 stays deterministically pinned")
		Expect(applyLWS(lwsSpec{
			name:      lwsName,
			prefix:    prefix,
			topology:  "2x2x2",
			replicas:  2,
			size:      2,
			partition: intPtr(1),
		})).To(Succeed())
		sName0 := prefix + "-0"
		sName1 := prefix + "-1"
		wantWorkerOwner0 := lwsName + "-0"
		wantWorkerOwner1 := lwsName + "-1"
		ownerJSONPath := fmt.Sprintf("{.metadata.labels.%s}", escapeJsonpath(labels.SliceOwnerNameLabel))
		Eventually(func(g Gomega) {
			owner1, err := getSliceField(sName1, ownerJSONPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(owner1).To(Equal(wantWorkerOwner1))

			owner0, err := getSliceField(sName0, ownerJSONPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(owner0).To(Equal(lwsName))
		}).Should(Succeed())

		By("scaling to replicas=1 with partition=0 so ordinal 0 completes handover to worker-0 and leader drops finalizer")
		Expect(applyLWS(lwsSpec{
			name:      lwsName,
			prefix:    prefix,
			topology:  "2x2x2",
			replicas:  1,
			size:      2,
			partition: intPtr(0),
		})).To(Succeed())
		Eventually(func() bool { return !sliceExists(sName1) }).Should(BeTrue())
		Eventually(func(g Gomega) {
			owner0, err := getSliceField(sName0, ownerJSONPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(owner0).To(Equal(wantWorkerOwner0))
		}).Should(Succeed())
		Eventually(func(g Gomega) {
			finalizers, err := getStsField(lwsName, testNamespace, "{.metadata.finalizers}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(finalizers).NotTo(ContainSubstring(finalizerName))
		}).Should(Succeed())

		By("rolling LWS size 2 -> 1 and scaling replicas 1 -> 2, verifying leader reclaims both Slices")
		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x2",
			replicas: 1,
			size:     1,
		})).To(Succeed())
		Eventually(func(g Gomega) {
			owner, err := getSliceField(sName0, ownerJSONPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(owner).To(Equal(lwsName))
		}).Should(Succeed())

		Expect(applyLWS(lwsSpec{
			name:     lwsName,
			prefix:   prefix,
			topology: "2x2x2",
			replicas: 2,
			size:     1,
		})).To(Succeed())
		for i := range 2 {
			sName := fmt.Sprintf("%s-%d", prefix, i)
			Eventually(func(g Gomega) {
				owner, err := getSliceField(sName, ownerJSONPath)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(owner).To(Equal(lwsName))
			}).Should(Succeed())
		}

		By("deleting the LeaderWorkerSet and verifying all Slice CRs are cleaned up")
		deleteLWS(lwsName, testNamespace)
		Eventually(func(g Gomega) {
			g.Expect(sliceExists(prefix + "-0")).To(BeFalse())
			g.Expect(sliceExists(prefix + "-1")).To(BeFalse())
		}).Should(Succeed())
	})

	It("rejects cross-namespace collisions and >63-char derived Slice names on StatefulSet create and scale-up", func() {
		By("creating LWS A in testNamespace claiming slice-prefix e2e-lws-dup")
		Expect(applyLWS(lwsSpec{
			name:     "e2e-lws-dup-a",
			prefix:   "e2e-lws-dup",
			topology: "2x2x1",
			replicas: 1,
			size:     1,
		})).To(Succeed())
		Eventually(func() bool { return sliceExists("e2e-lws-dup-0") }).Should(BeTrue())

		lwsStatefulSetManifest := func(name, prefix, sizeStr string, replicas int) string {
			return fmt.Sprintf(`apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  labels:
    %s: %s
spec:
  replicas: %d
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app: %s
  serviceName: %s
  template:
    metadata:
      annotations:
        %s: %q
      labels:
        app: %s
        %s: %s
        %s: tpu7x
        %s: 2x2x1
    spec:
      terminationGracePeriodSeconds: 1
      containers:
      - name: c
        image: registry.k8s.io/pause:3.10
`, name, lwsSecondNamespace, labels.LWSNameLabel, name, replicas, name, name,
				labels.LWSSizeAnnotation, sizeStr, name,
				labels.SlicePrefixLabel, prefix, labels.SliceTypeLabel, labels.SliceTopologyLabel)
		}

		By("applying colliding size=1 and size=2 LWS StatefulSets in another namespace and expecting rejection")
		for _, sizeStr := range []string{"1", "2"} {
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-dup-b", "e2e-lws-dup", sizeStr, 1))
			out, err := cmd.CombinedOutput()
			Expect(err).To(HaveOccurred())
			Expect(string(out)).To(ContainSubstring(`slice-prefix "e2e-lws-dup" is already used`))
		}

		By("applying a standalone StatefulSet claiming derived Slice e2e-lws-dup-0 and expecting rejection")
		err := applyStatefulSet("e2e-standalone-dup", lwsSecondNamespace, map[string]string{
			labels.SliceNameLabel:     "e2e-lws-dup-0",
			labels.SliceTypeLabel:     "tpu7x",
			labels.SliceTopologyLabel: "2x2x1",
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`slice-name "e2e-lws-dup-0" is already used by StatefulSet`))

		By("rejecting standalone StatefulSet collisions against an admitted size=2 SkipSharedLeader LWS leader")
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-size2-leader", "e2e-lws-s2", "2", 2))
		out, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "expected size=2 LWS leader to be accepted: %s", string(out))

		err = applyStatefulSet("e2e-standalone-dup", lwsSecondNamespace, map[string]string{
			labels.SliceNameLabel:     "e2e-lws-s2-1",
			labels.SliceTypeLabel:     "tpu7x",
			labels.SliceTopologyLabel: "2x2x1",
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`slice-name "e2e-lws-s2-1" is already used by StatefulSet`))
		cleanupStatefulSet("e2e-lws-size2-leader", lwsSecondNamespace)

		By("rejecting >63-char and non-DNS-1123 derived Slice names on LWS StatefulSet create")
		longPrefix := strings.Repeat("l", 62)
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-dup-b", longPrefix, "2", 1))
		out, err = cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(out)).To(ContainSubstring("exceeds 63 characters"))

		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-dup-b", "Invalid_Prefix", "2", 1))
		out, err = cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(out)).To(ContainSubstring("not a valid RFC 1123 label"))

		By("accepting a 61-char slice-prefix at replicas=9 (63 chars) and rejecting scale-up to replicas=11 (64 chars)")
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-scale63", scale61Prefix, "1", 9))
		out, err = cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "expected 61-char prefix at replicas=9 to be accepted: %s", string(out))

		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(lwsStatefulSetManifest("e2e-lws-scale63", scale61Prefix, "1", 11))
		out, err = cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(out)).To(ContainSubstring("exceeds 63 characters"))

		cleanupStatefulSet("e2e-lws-scale63", lwsSecondNamespace)
		for i := range 9 {
			deleteSlice(fmt.Sprintf("%s-%d", scale61Prefix, i))
		}
		deleteLWS("e2e-lws-dup-a", testNamespace)
		Eventually(func() bool { return !sliceExists("e2e-lws-dup-0") }).Should(BeTrue())
	})
})
