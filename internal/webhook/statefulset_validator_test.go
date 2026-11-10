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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
)

const (
	testSliceTypeTPU7x = "tpu7x"
	testSliceTopo2x2x2 = "2x2x2"
)

var _ = Describe("StatefulSet Validator", func() {
	var (
		validator *StatefulSetValidator
		ctx       = context.Background()
	)

	BeforeEach(func() {
		validator = &StatefulSetValidator{
			Client: k8sClient,
		}
	})

	Context("ValidateCreate", func() {
		It("should allow creation if no other StatefulSet has the same slice-name", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unique-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "unique-slice",
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, sts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny creation if another StatefulSet has the same slice-name", func() {
			existingSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "shared-slice",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "existing"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "existing"},
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
			Expect(k8sClient.Create(ctx, existingSts)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, existingSts)
			}()

			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "new-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "shared-slice",
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, newSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already used by StatefulSet"))
		})

		It("should allow creation if the slice-name label is missing", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-label-sts",
					Namespace: "default",
				},
			}
			_, err := validator.ValidateCreate(ctx, sts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny duplicate slice-prefix across distinct LWS in same or different namespaces, while allowing leader and worker of the same LWS in the same namespace", func() {
			otherNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "validator-ns-b"}}
			if err := k8sClient.Create(ctx, otherNS); err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			two := int32(2)
			existingLeader := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-alpha",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "lws-alpha",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "lws-alpha"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								"app":              "lws-alpha",
								slicePrefixLabel:   "shared-lws-prefix",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, existingLeader)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, existingLeader)
			})

			// Same LWS name in the same namespace (worker StatefulSet) is allowed.
			sameLWSWorker := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-alpha-0",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel:       "lws-alpha",
						labels.LWSRoleLabel:       "worker",
						labels.LWSGroupIndexLabel: "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "shared-lws-prefix",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, sameLWSWorker)
			Expect(err).NotTo(HaveOccurred())

			// Distinct LWS in the same namespace claiming the same slice-prefix is denied.
			differentLWS := sameLWSWorker.DeepCopy()
			differentLWS.Name = "lws-beta"
			differentLWS.Labels = map[string]string{labels.LWSNameLabel: "lws-beta"}
			differentLWS.Spec.Template.Annotations = map[string]string{labels.LWSSizeAnnotation: "1"}
			_, err = validator.ValidateCreate(ctx, differentLWS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`slice-prefix "shared-lws-prefix" is already used by StatefulSet default/lws-alpha`))

			// Even an identically named LWS in a DIFFERENT namespace must be denied because Slice CRs are cluster-scoped.
			crossNamespaceSameLWSName := existingLeader.DeepCopy()
			crossNamespaceSameLWSName.Namespace = "validator-ns-b"
			_, err = validator.ValidateCreate(ctx, crossNamespaceSameLWSName)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`slice-prefix "shared-lws-prefix" is already used by StatefulSet default/lws-alpha`))

			// Standalone StatefulSet claiming a Slice name derived by existingLeader ("shared-lws-prefix-1") is denied.
			collidingStandalone := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "standalone-collider",
					Namespace: "validator-ns-b",
					Labels: map[string]string{
						sliceNameLabel: "shared-lws-prefix-1",
					},
				},
			}
			_, err = validator.ValidateCreate(ctx, collidingStandalone)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`slice-name "shared-lws-prefix-1" is already used by StatefulSet default/lws-alpha`))
		})

		It("should deny LWS slice-prefix creation when a derived Slice name collides with an existing standalone StatefulSet or exceeds 63 characters", func() {
			standalone := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "standalone-holder",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "taken-prefix-1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "standalone-holder"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "standalone-holder"}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, standalone)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, standalone)
			})

			two := int32(2)
			lwsCandidate := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-collider",
					Namespace: "default",
					Labels:    map[string]string{labels.LWSNameLabel: "lws-collider"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								slicePrefixLabel: "taken-prefix",
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, lwsCandidate)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`derived slice-name "taken-prefix-1" is already used by StatefulSet default/standalone-holder`))

			// 62-char prefix produces "<62>-0" (64 chars > 63 RFC 1123 limit).
			tooLong := lwsCandidate.DeepCopy()
			tooLong.Spec.Template.Labels[slicePrefixLabel] = strings.Repeat("a", 62)
			_, err = validator.ValidateCreate(ctx, tooLong)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds 63 characters"))

			// Zero-replica LWS with 62-char prefix must also be rejected on CREATE (#30).
			zero := int32(0)
			tooLongZeroReplicas := tooLong.DeepCopy()
			tooLongZeroReplicas.Spec.Replicas = &zero
			_, err = validator.ValidateCreate(ctx, tooLongZeroReplicas)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds 63 characters"))
		})

		It("should enforce collisions and RFC 1123 length limits on multi-host (size > 1) and LeaderExcluded LWS leaders and template-only standalone StatefulSets", func() {
			two := int32(2)
			size2Leader := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-host-lws",
					Namespace: "default",
					Labels:    map[string]string{labels.LWSNameLabel: "multi-host-lws"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "multi-host-lws"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "2"},
							Labels: map[string]string{
								"app":              "multi-host-lws",
								slicePrefixLabel:   "mh-prefix",
								sliceTypeLabel:     testSliceTypeTPU7x,
								sliceTopologyLabel: testSliceTopo2x2x2,
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, size2Leader)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, size2Leader)
			})

			// Inbound collision before worker StatefulSets exist (#30): standalone template-only slice-name (#28).
			templateOnlyStandalone := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "template-only-standalone", Namespace: "default"},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{sliceNameLabel: "mh-prefix-1"},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, templateOnlyStandalone)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`slice-name "mh-prefix-1" is already used by StatefulSet default/multi-host-lws`))

			// Create a standalone StatefulSet with template-only slice-name and verify outbound collision from a size > 1 LWS (#28, #30).
			existingTemplateOnly := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "template-holder", Namespace: "default"},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "template-holder"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app":          "template-holder",
								sliceNameLabel: "tpl-taken-1",
							},
						},
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, existingTemplateOnly)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, existingTemplateOnly)
			})

			collidingSize2 := size2Leader.DeepCopy()
			collidingSize2.Name = "colliding-size2"
			collidingSize2.Labels = map[string]string{labels.LWSNameLabel: "colliding-size2"}
			collidingSize2.Spec.Template.Labels[slicePrefixLabel] = "tpl-taken"
			_, err = validator.ValidateCreate(ctx, collidingSize2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`derived slice-name "tpl-taken-1" is already used by StatefulSet default/template-holder`))

			// LeaderExcluded LWS leader claims both <prefix>-leader and <prefix>-0..N-1.
			leaderExcluded := collidingSize2.DeepCopy()
			leaderExcluded.Spec.Template.Annotations = map[string]string{
				labels.LWSSizeAnnotation:               "4",
				labels.LWSSubgroupSizeAnnotation:       "2",
				labels.LWSSubgroupPolicyTypeAnnotation: "LeaderExcluded",
			}
			_, err = validator.ValidateCreate(ctx, leaderExcluded)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`derived slice-name "tpl-taken-1" is already used by StatefulSet default/template-holder`))

			// Multi-host size > 1 leader with 62-char prefix rejected on CREATE (#30).
			tooLongSize2 := collidingSize2.DeepCopy()
			tooLongSize2.Spec.Template.Labels[slicePrefixLabel] = strings.Repeat("b", 62)
			_, err = validator.ValidateCreate(ctx, tooLongSize2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds 63 characters"))
		})
	})

	Context("ValidateUpdate", func() {
		It("should deny update when slice-name changes to a conflicting name", func() {
			existingSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-update-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "taken-slice",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "existing-update"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "existing-update"},
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
			Expect(k8sClient.Create(ctx, existingSts)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, existingSts)
			}()

			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "updating-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "old-slice",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "updating-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "taken-slice",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already used by StatefulSet"))
		})

		It("should allow update when slice-name does not change", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "same-name-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "unchanged-slice",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "same-name-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "unchanged-slice",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should allow update when slice-name changes to a unique name", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rename-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "old-unique-slice",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rename-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "new-unique-slice",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reject update that changes slice-type label", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-type-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel: "v6e",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-type-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel: "tpu7x",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
			Expect(err.Error()).To(ContainSubstring(sliceTypeLabel))
		})

		It("should reject update that changes slice-topology label", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-topo-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTopologyLabel: "2x4",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-topo-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTopologyLabel: "4x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
			Expect(err.Error()).To(ContainSubstring(sliceTopologyLabel))
		})

		It("should reject update that removes a previously set immutable label", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-remove-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-remove-sts",
					Namespace: "default",
					Labels: map[string]string{
						// sliceTypeLabel removed
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
		})

		It("should allow update when immutable labels are unchanged", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-same-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-same-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should allow update on non-opted-in StatefulSet adding new TPU labels", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-noop-sts",
					Namespace: "default",
				},
			}
			newSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "immutable-noop-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should allow LeaderWorkerSet rollouts that change template slice labels, LWS size, or clear upgraded <= 0.6.0 top-level labels", func() {
			two := int32(2)
			baseSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-template-rollout",
					Namespace: "default",
					Labels:    map[string]string{labels.LWSNameLabel: "lws-template-rollout"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
							Labels: map[string]string{
								slicePrefixLabel:   "lws-rollout",
								sliceTypeLabel:     testSliceTypeTPU7x,
								sliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			}

			changedTopo := baseSts.DeepCopy()
			changedTopo.Spec.Template.Labels[sliceTopologyLabel] = testSliceTopo2x2x2
			_, err := validator.ValidateUpdate(ctx, baseSts, changedTopo)
			Expect(err).NotTo(HaveOccurred())

			changedSize := baseSts.DeepCopy()
			changedSize.Spec.Template.Annotations[labels.LWSSizeAnnotation] = "2"
			_, err = validator.ValidateUpdate(ctx, baseSts, changedSize)
			Expect(err).NotTo(HaveOccurred())

			// Upgraded <= 0.6.0 / LeaderExcluded leader clearing stale top-level labels while keeping template labels.
			legacyLeader := baseSts.DeepCopy()
			legacyLeader.Labels[sliceNameLabel] = "lws-rollout-0"
			legacyLeader.Labels[sliceTypeLabel] = "v6e"
			legacyLeader.Labels[sliceTopologyLabel] = "2x2x1"
			clearedLeader := baseSts.DeepCopy()
			_, err = validator.ValidateUpdate(ctx, legacyLeader, clearedLeader)
			Expect(err).NotTo(HaveOccurred())

			// Worker / LeaderExcluded rollouts update both top-level and template type/topology labels.
			oldWorker := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-template-rollout-0",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel:       "lws-template-rollout",
						labels.LWSRoleLabel:       "worker",
						labels.LWSGroupIndexLabel: "0",
						sliceNameLabel:            "lws-rollout-0",
						sliceTypeLabel:            "v6e",
						sliceTopologyLabel:        "2x2x1",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								slicePrefixLabel:   "lws-rollout",
								sliceNameLabel:     "lws-rollout-0",
								sliceTypeLabel:     "v6e",
								sliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			}
			rolledWorker := oldWorker.DeepCopy()
			rolledWorker.Labels[sliceTypeLabel] = testSliceTypeTPU7x
			rolledWorker.Labels[sliceTopologyLabel] = testSliceTopo2x2x2
			rolledWorker.Spec.Template.Labels[sliceTypeLabel] = testSliceTypeTPU7x
			rolledWorker.Spec.Template.Labels[sliceTopologyLabel] = testSliceTopo2x2x2
			_, err = validator.ValidateUpdate(ctx, oldWorker, rolledWorker)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject mutating slice-prefix, removing opted-in TPU labels, or scaling LWS replicas past 63-char Slice names", func() {
			two := int32(2)
			eleven := int32(11)
			prefix61 := strings.Repeat("p", 61)

			baseSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lws-prefix-guards",
					Namespace: "default",
					Labels:    map[string]string{labels.LWSNameLabel: "lws-prefix-guards"},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &two,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{labels.LWSSizeAnnotation: "2"},
							Labels: map[string]string{
								slicePrefixLabel:   prefix61,
								sliceTypeLabel:     testSliceTypeTPU7x,
								sliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			}

			changedPrefix := baseSts.DeepCopy()
			changedPrefix.Spec.Template.Labels[slicePrefixLabel] = "mutated-prefix"
			_, err := validator.ValidateUpdate(ctx, baseSts, changedPrefix)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))

			removedPrefix := baseSts.DeepCopy()
			delete(removedPrefix.Spec.Template.Labels, slicePrefixLabel)
			_, err = validator.ValidateUpdate(ctx, baseSts, removedPrefix)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))

			removedTopo := baseSts.DeepCopy()
			delete(removedTopo.Spec.Template.Labels, sliceTopologyLabel)
			_, err = validator.ValidateUpdate(ctx, baseSts, removedTopo)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be removed once set"))

			// Scaling size > 1 replicas 2 -> 11 introduces "<61>-10" (64 chars), exceeding RFC 1123 (#30).
			scaledTooWide := baseSts.DeepCopy()
			scaledTooWide.Spec.Replicas = &eleven
			_, err = validator.ValidateUpdate(ctx, baseSts, scaledTooWide)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds 63 characters"))

			// Valid K8s label values with uppercase or '_' characters are invalid DNS-1123 Slice CR names.
			invalidDNSSts := baseSts.DeepCopy()
			invalidDNSSts.Spec.Template.Labels[slicePrefixLabel] = "Invalid_Prefix"
			_, err = validator.ValidateCreate(ctx, invalidDNSSts)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not a valid RFC 1123 label"))

			zero := int32(0)
			invalidDNSZero := invalidDNSSts.DeepCopy()
			invalidDNSZero.Spec.Replicas = &zero
			_, err = validator.ValidateCreate(ctx, invalidDNSZero)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not a valid RFC 1123 label"))
		})

		It("should enforce label immutability when LWSNameLabel is set without slice-prefix and reject template-only slice-name collisions on update", func() {
			// #27: sameLWS without slice-prefix must NOT relax slice-type/slice-topology immutability.
			oldNoPrefixLWS := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "static-slice-lws",
					Namespace: "default",
					Labels: map[string]string{
						labels.LWSNameLabel: "static-slice-lws",
						sliceNameLabel:      "static-slice",
						sliceTypeLabel:      "v6e",
						sliceTopologyLabel:  "2x2x1",
					},
				},
			}
			mutatedNoPrefixLWS := oldNoPrefixLWS.DeepCopy()
			mutatedNoPrefixLWS.Labels[sliceTopologyLabel] = testSliceTopo2x2x2
			_, err := validator.ValidateUpdate(ctx, oldNoPrefixLWS, mutatedNoPrefixLWS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))

			// #28: Updating spec.template.metadata.labels[gke-tpu-slice] while dropping top-level slice-name must still check collisions.
			holder := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "tpl-update-holder",
					Namespace: "default",
					Labels:    map[string]string{sliceNameLabel: "tpl-update-taken"},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "tpl-update-holder"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "tpl-update-holder"}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, holder)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, holder)
			})

			oldStandalone := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "tpl-updating-sts",
					Namespace: "default",
					Labels:    map[string]string{sliceNameLabel: "tpl-old"},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{sliceNameLabel: "tpl-old"}},
					},
				},
			}
			newStandalone := oldStandalone.DeepCopy()
			delete(newStandalone.Labels, sliceNameLabel)
			newStandalone.Spec.Template.Labels[sliceNameLabel] = "tpl-update-taken"
			_, err = validator.ValidateUpdate(ctx, oldStandalone, newStandalone)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`slice-name "tpl-update-taken" is already used by StatefulSet default/tpl-update-holder`))
		})

		It("should reject template slice label mutations on StatefulSets not managed by LeaderWorkerSet", func() {
			oldSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "standalone-template", Namespace: "default"},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								sliceNameLabel:     "standalone-template-slice",
								sliceTypeLabel:     "tpu7x",
								sliceTopologyLabel: "2x2x1",
							},
						},
					},
				},
			}
			newSts := oldSts.DeepCopy()
			newSts.Spec.Template.Labels[sliceTopologyLabel] = "2x2x2"

			_, err := validator.ValidateUpdate(ctx, oldSts, newSts)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(sliceTopologyLabel))

			// Adding LWSNameLabel onto a standalone StatefulSet must not bypass template label immutability.
			spoofedLWS := newSts.DeepCopy()
			spoofedLWS.Labels = map[string]string{labels.LWSNameLabel: "spoofed-lws"}
			_, err = validator.ValidateUpdate(ctx, oldSts, spoofedLWS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(sliceTopologyLabel))
		})

		It("should not block finalizer removal or metadata-only updates when a slice-prefix collision already exists", func() {
			two := int32(2)
			newLeader := func(name string) *appsv1.StatefulSet {
				return &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "default",
						Labels:    map[string]string{labels.LWSNameLabel: name},
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &two,
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{labels.LWSSizeAnnotation: "1"},
								Labels: map[string]string{
									"app":              name,
									slicePrefixLabel:   "preexisting-collision",
									sliceTypeLabel:     "tpu7x",
									sliceTopologyLabel: "2x2x1",
								},
							},
							Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "test"}}},
						},
					},
				}
			}
			existing := newLeader("collision-owner")
			Expect(k8sClient.Create(ctx, existing)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, existing)
			})

			// A colliding StatefulSet admitted before this validator existed (or with the webhook disabled).
			oldColliding := newLeader("collision-legacy")
			oldColliding.Finalizers = []string{"test.tpu.google.com/finalizer"}

			By("allowing a metadata-only update")
			relabeled := oldColliding.DeepCopy()
			relabeled.Annotations = map[string]string{"example.com/touched": "true"}
			_, err := validator.ValidateUpdate(ctx, oldColliding, relabeled)
			Expect(err).NotTo(HaveOccurred())

			By("allowing finalizer removal during deletion")
			deleting := oldColliding.DeepCopy()
			deleting.Labels[sliceTypeLabel] = "tpu7x"
			now := metav1.Now()
			deleting.DeletionTimestamp = &now
			finalized := deleting.DeepCopy()
			finalized.Finalizers = nil
			delete(finalized.Labels, sliceTypeLabel)
			_, err = validator.ValidateUpdate(ctx, deleting, finalized)
			Expect(err).NotTo(HaveOccurred())

			By("still rejecting a scale-up that introduces a Slice name owned by another StatefulSet")
			three := int32(3)
			existingThree := existing.DeepCopy()
			existingThree.Spec.Replicas = &three
			Expect(k8sClient.Update(ctx, existingThree)).To(Succeed())

			scaled := oldColliding.DeepCopy()
			scaled.Spec.Replicas = &three
			_, err = validator.ValidateUpdate(ctx, oldColliding, scaled)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`derived slice-name "preexisting-collision-2" is already used by StatefulSet default/collision-owner`))
		})
	})

	Context("ValidateDelete", func() {
		It("should always allow deletion", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "delete-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel:     "some-slice",
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateDelete(ctx, sts)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
