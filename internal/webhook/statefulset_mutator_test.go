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

package webhook

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

var _ = Describe("StatefulSet Mutator", func() {
	var (
		mutator *StatefulSetMutator
		ctx     = context.Background()
	)

	BeforeEach(func() {
		mutator = &StatefulSetMutator{
			Client: k8sClient,
		}
	})

	Context("Default", func() {
		It("should propagate slice labels to Pod template labels", func() {
			sliceName := "test-slice"
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel:     sliceName,
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app": "test",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "test",
									Image: "test",
								},
							},
						},
					},
				},
			}

			err := mutator.Default(ctx, sts)
			Expect(err).ToNot(HaveOccurred())

			Expect(sts.Spec.Template.Labels[sliceNameLabel]).To(Equal(sliceName))
			Expect(sts.Spec.Template.Labels[sliceTypeLabel]).To(Equal("v6e"))
			Expect(sts.Spec.Template.Labels[sliceTopologyLabel]).To(Equal("2x4"))
			Expect(sts.Spec.Template.Labels["app"]).To(Equal("test"))
		})

		It("should derive slice name from slice-prefix and group-index", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-k8s-0",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel:       "lws-k8s",
						labels.LWSGroupIndexLabel: "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "k8s-slice",
								sliceTypeLabel:     "v6e",
								sliceTopologyLabel: "2x2x1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}

			err := mutator.Default(ctx, sts)
			Expect(err).ToNot(HaveOccurred())

			Expect(sts.Labels[sliceNameLabel]).To(Equal("k8s-slice-0"))
			Expect(sts.Labels[sliceTypeLabel]).To(Equal("v6e"))
			Expect(sts.Labels[sliceTopologyLabel]).To(Equal("2x2x1"))
			Expect(sts.Spec.Template.Annotations[sliceNameAnnotation]).To(Equal("k8s-slice-0"))
			Expect(sts.Spec.Template.Labels[sliceNameLabel]).To(Equal("k8s-slice-0"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceNameLabel]).To(Equal("k8s-slice-0"))
		})

		It("should not stamp a slice name on a multi-host LWS leader StatefulSet", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-k8s",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "lws-k8s",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "k8s-slice",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "2",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}

			Expect(mutator.Default(ctx, sts)).To(Succeed())

			Expect(sts.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Annotations).NotTo(HaveKey(sliceNameAnnotation))
			Expect(sts.Spec.Template.Spec.NodeSelector).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x2"))
		})

		It("should not stamp a single slice name on a single-host (size=1) LWS leader StatefulSet", func() {
			replicas := int32(2)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-size1",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "lws-size1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "lws-size1",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}

			Expect(mutator.Default(ctx, sts)).To(Succeed())

			Expect(sts.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Annotations).NotTo(HaveKey(sliceNameAnnotation))
			Expect(sts.Spec.Template.Spec.NodeSelector).NotTo(HaveKey(sliceNameLabel))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x1"))
		})

		It("should leave an upgraded LWS size=1 leader template untouched when slice-name was stamped by an older release", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-lws-size1",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "legacy-lws-size1",
						sliceNameLabel:      "legacy-lws-size1-0",
						sliceTypeLabel:      "tpu7x",
						sliceTopologyLabel:  "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "legacy-lws-size1",
								sliceNameLabel:     "legacy-lws-size1-0",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
								sliceNameAnnotation:      "legacy-lws-size1-0",
							},
						},
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								sliceNameLabel:     "legacy-lws-size1-0",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
							Containers: []corev1.Container{{Name: "c", Image: "test"}},
						},
					},
				},
			}

			wantTemplate := sts.Spec.Template.DeepCopy()
			Expect(mutator.Default(ctx, sts)).To(Succeed())
			Expect(sts.Spec.Template).To(Equal(*wantTemplate))
		})

		It("should mutate standalone StatefulSets specifying TPU labels on spec.template.metadata.labels and update SSA-preserved legacy LWS leader nodeSelectors", func() {
			standalone := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "standalone-template-tpu",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								sliceNameLabel:     "standalone-slice",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}

			Expect(mutator.Default(ctx, standalone)).To(Succeed())
			Expect(standalone.Labels[sliceNameLabel]).To(Equal("standalone-slice"))
			Expect(standalone.Labels[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(standalone.Labels[sliceTopologyLabel]).To(Equal("2x2x1"))
			Expect(standalone.Spec.Template.Annotations[sliceNameAnnotation]).To(Equal("standalone-slice"))
			Expect(standalone.Spec.Template.Spec.NodeSelector[sliceNameLabel]).To(Equal("standalone-slice"))
			Expect(standalone.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(standalone.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x1"))

			// Upstream LWS uses Server-Side Apply (FieldManager: "lws"), which preserves
			// webhook-owned map entries in sts.Spec.Template.Labels (including sliceNameLabel)
			// when rolling out a leaderTemplate topology/accelerator change on an upgraded <= 0.6.0 leader.
			ssaRolledLegacyLeader := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-lws-ssa",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "legacy-lws-ssa",
						sliceNameLabel:      "legacy-lws-ssa-0",
						sliceTypeLabel:      "v6e",
						sliceTopologyLabel:  "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "legacy-lws-ssa",
								sliceNameLabel:     "legacy-lws-ssa-0",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
								sliceNameAnnotation:      "legacy-lws-ssa-0",
							},
						},
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								sliceNameLabel:     "legacy-lws-ssa-0",
								sliceTypeLabel:     "v6e",
								sliceTopologyLabel: "2x2x1",
							},
							Containers: []corev1.Container{{Name: "c", Image: "test"}},
						},
					},
				},
			}

			Expect(mutator.Default(ctx, ssaRolledLegacyLeader)).To(Succeed())
			Expect(ssaRolledLegacyLeader.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(ssaRolledLegacyLeader.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x2"))
		})

		It("should propagate updated template type/topology to nodeSelector on rollout of an upgraded LWS leader or worker without re-stamping stale top-level slice-name", func() {
			rolledLeader := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-lws-size1",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "legacy-lws-size1",
						sliceNameLabel:      "legacy-lws-size1-0",
						sliceTypeLabel:      "v6e",
						sliceTopologyLabel:  "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "legacy-lws-size1",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x2",
							},
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(mutator.Default(ctx, rolledLeader)).To(Succeed())
			Expect(rolledLeader.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(rolledLeader.Labels).NotTo(HaveKey(sliceTypeLabel))
			Expect(rolledLeader.Labels).NotTo(HaveKey(sliceTopologyLabel))
			Expect(rolledLeader.Spec.Template.Labels).NotTo(HaveKey(sliceNameLabel))
			Expect(rolledLeader.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(rolledLeader.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x2"))

			rolledWorker := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-worker-0",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel:       "lws-worker",
						labels.LWSRoleLabel:       "worker",
						labels.LWSGroupIndexLabel: "0",
						sliceNameLabel:            "worker-slice-0",
						sliceTypeLabel:            "v6e",
						sliceTopologyLabel:        "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "worker-slice",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x2",
							},
						},
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								sliceTypeLabel:     "v6e",
								sliceTopologyLabel: "2x2x1",
							},
							Containers: []corev1.Container{{Name: "c", Image: "test"}},
						},
					},
				},
			}
			Expect(mutator.Default(ctx, rolledWorker)).To(Succeed())
			Expect(rolledWorker.Labels[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(rolledWorker.Labels[sliceTopologyLabel]).To(Equal("2x2x2"))
			Expect(rolledWorker.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(rolledWorker.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x2"))
		})

		It("should still inject nodeSelector on an LWS worker StatefulSet identified by role label even when template slice-name is pre-populated", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-worker-0",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel:       "lws-worker",
						labels.LWSRoleLabel:       "worker",
						labels.LWSGroupIndexLabel: "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "worker-slice",
								sliceNameLabel:     "worker-slice-0",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x2",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}

			Expect(mutator.Default(ctx, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceNameLabel]).To(Equal("worker-slice-0"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("2x2x2"))
			Expect(sts.Spec.Template.Annotations[sliceNameAnnotation]).To(Equal("worker-slice-0"))
		})

		It("automatically injects accelerator and topology into pod template nodeSelector when specified in labels", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "auto-nodeselector-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel:     "auto-nodeselector-slice",
						sliceTypeLabel:     "tpu7x",
						sliceTopologyLabel: "4x4x4",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app": "test-app",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "c", Image: "test"}},
						},
					},
				},
			}

			err := mutator.Default(ctx, sts)
			Expect(err).ToNot(HaveOccurred())

			Expect(sts.Spec.Template.Spec.NodeSelector).ToNot(BeNil())
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(sts.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("4x4x4"))
		})

		It("should do nothing if slice-name label is missing", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-label-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app": "test",
							},
						},
					},
				},
			}

			err := mutator.Default(ctx, sts)
			Expect(err).ToNot(HaveOccurred())

			Expect(sts.Spec.Template.Labels).To(HaveLen(1))
			Expect(sts.Spec.Template.Labels["app"]).To(Equal("test"))
			Expect(sts.Spec.Template.Annotations).To(BeNil())
		})

		It("should return an error if slice requirements derivation fails", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bare-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel: "k8s-slice",
							},
						},
					},
				},
			}

			err := mutator.Default(ctx, sts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("slice-prefix was specified but the StatefulSet is not managed by LeaderWorkerSet"))
		})
	})
})
