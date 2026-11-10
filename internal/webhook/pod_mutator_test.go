// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package webhook

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

var _ = Describe("Pod Mutator", func() {
	var (
		mutator *PodMutator
		ctx     = context.Background()
	)

	BeforeEach(func() {
		mutator = &PodMutator{Client: k8sClient}
	})

	// leaderPod builds a Pod as the LWS leader StatefulSet would create it, carrying
	// the template's stale group-0 slice name.
	leaderPod := func(name string, podLabels, annotations map[string]string) *corev1.Pod {
		lbls := map[string]string{
			labels.LWSNameLabel: "lws-jax-pd",
			slicePrefixLabel:    "lws-jax-pd",
			sliceNameLabel:      "lws-jax-pd-0",
		}
		for k, v := range podLabels {
			lbls[k] = v
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Labels:      lbls,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				SchedulerName: "slice-scheduler",
				Containers:    []corev1.Container{{Name: "test", Image: "test"}},
			},
		}
	}

	Context("Default", func() {
		It("should assign each leader Pod the slice of its own group", func() {
			for _, tc := range []struct{ name, groupIndex, want string }{
				{"lws-jax-pd-0", "0", "lws-jax-pd-0"},
				{"lws-jax-pd-1", "1", "lws-jax-pd-1"},
			} {
				pod := leaderPod(tc.name, map[string]string{labels.LWSGroupIndexLabel: tc.groupIndex}, nil)
				Expect(mutator.Default(ctx, pod)).To(Succeed())
				Expect(pod.Labels[sliceNameLabel]).To(Equal(tc.want))
				Expect(pod.Annotations[sliceNameAnnotation]).To(Equal(tc.want))
			}
		})

		It("should fall back to the Pod ordinal when the group-index label is not yet set", func() {
			pod := leaderPod("lws-jax-pd-3", nil, nil)
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Labels[sliceNameLabel]).To(Equal("lws-jax-pd-3"))
		})

		It("should not pin slice-scheduler Pods via nodeSelector", func() {
			pod := leaderPod("lws-jax-pd-1", nil, nil)
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Spec.NodeSelector).NotTo(HaveKey(sliceNameLabel))
		})

		It("should overwrite the inherited nodeSelector for Pods not using slice-scheduler", func() {
			pod := leaderPod("lws-jax-pd-1", nil, nil)
			pod.Spec.SchedulerName = ""
			pod.Spec.NodeSelector = map[string]string{sliceNameLabel: "lws-jax-pd-0"}
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Spec.NodeSelector[sliceNameLabel]).To(Equal("lws-jax-pd-1"))
		})

		It("should give a LeaderExcluded leader the dedicated leader slice", func() {
			pod := leaderPod("lws-jax-pd-1", nil, map[string]string{
				labels.LWSSubgroupSizeAnnotation:       "2",
				labels.LWSSubgroupPolicyTypeAnnotation: "LeaderExcluded",
			})
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Labels[sliceNameLabel]).To(Equal("lws-jax-pd-leader"))
		})

		It("should leave worker Pods unchanged", func() {
			pod := leaderPod("lws-jax-pd-1-1", map[string]string{labels.LWSGroupIndexLabel: "1"},
				map[string]string{labels.LWSLeaderNameAnnotation: "lws-jax-pd-1"})
			pod.Labels[sliceNameLabel] = "lws-jax-pd-1"
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Labels[sliceNameLabel]).To(Equal("lws-jax-pd-1"))
			Expect(pod.Annotations).NotTo(HaveKey(sliceNameAnnotation))
		})

		It("should leave Pods without slice-prefix unchanged", func() {
			pod := leaderPod("lws-jax-pd-1", nil, nil)
			delete(pod.Labels, slicePrefixLabel)
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Labels[sliceNameLabel]).To(Equal("lws-jax-pd-0"))
		})

		It("should leave Pods not managed by LWS unchanged", func() {
			pod := leaderPod("plain-1", nil, nil)
			delete(pod.Labels, labels.LWSNameLabel)
			Expect(mutator.Default(ctx, pod)).To(Succeed())
			Expect(pod.Labels[sliceNameLabel]).To(Equal("lws-jax-pd-0"))
		})
	})
})
