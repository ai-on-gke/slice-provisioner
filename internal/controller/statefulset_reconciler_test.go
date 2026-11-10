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

package controller

import (
	"context"
	"strconv"
	"time"

	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
)

var _ = Describe("StatefulSet Controller", func() {
	Context("When reconciling a resource", func() {
		var resourceName string
		var sliceName string
		const priorityClassName = "test-priority-class"
		const priorityVal = 100

		ctx := context.Background()

		var typeNamespacedName types.NamespacedName
		statefulset := &appsv1.StatefulSet{}

		BeforeEach(func() {
			// Generate unique names for each test to avoid interference
			uniqueID := strconv.FormatInt(time.Now().UnixNano(), 10)
			resourceName = "sts-" + uniqueID
			sliceName = "slice-" + uniqueID
			typeNamespacedName = types.NamespacedName{
				Name:      resourceName,
				Namespace: "default",
			}

			By("creating the custom resource for the Kind StatefulSet")

			err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
			if err != nil && errors.IsNotFound(err) {
				resource := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
						Labels: map[string]string{
							sliceNameLabel:     sliceName,
							sliceTypeLabel:     "v6e",
							sliceTopologyLabel: "4x4",
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "test"},
							},
							Spec: corev1.PodSpec{
								NodeSelector: map[string]string{
									sliceNameLabel: sliceName,
								},
								Containers: []corev1.Container{
									{
										Name:  "test-container",
										Image: "test-image",
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

			By("Creating a PriorityClass")
			pc := &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: priorityClassName,
				},
				Value: priorityVal,
			}
			err = k8sClient.Create(ctx, pc)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		})

		AfterEach(func() {
			// Also, defensively cleanup the slice CR in case the test failed
			// or the controller didn't delete it for some reason.
			slice := &slicev1beta1.Slice{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			if err == nil {
				By("Cleanup any leftover Slice CR")
				if len(slice.Finalizers) > 0 {
					slice.Finalizers = nil
					Expect(k8sClient.Update(ctx, slice)).To(Succeed())
				}
				Expect(k8sClient.Delete(ctx, slice)).To(Succeed())

				By("Waiting for Slice to be fully deleted")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
					return errors.IsNotFound(err)
				}, time.Second*10, time.Millisecond*250).Should(BeTrue(), "Failed to wait for Slice to be deleted")
			} else {
				Expect(errors.IsNotFound(err)).To(BeTrue(), "Expected NotFound or no error on Get for Slice cleanup")
			}

			sts := &appsv1.StatefulSet{}
			err = k8sClient.Get(ctx, typeNamespacedName, sts)
			if err == nil {
				By("Cleanup the specific resource instance StatefulSet")
				Expect(k8sClient.Delete(ctx, sts)).To(Succeed())

				By("Cleanup: Manually triggering finalizer logic by re-reconciling")
				controllerReconciler := &StatefulSetReconciler{
					Client: k8sClient,
					Scheme: k8sClient.Scheme(),
				}
				// Call Reconcile to process the deletion and finalizer
				// We may need to call it multiple times if it requeues
				Eventually(func() bool {
					_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
						NamespacedName: typeNamespacedName,
					})
					if err != nil {
						return false
					}
					err = k8sClient.Get(ctx, typeNamespacedName, sts)
					return errors.IsNotFound(err) || !controllerutil.ContainsFinalizer(sts, sliceProvisionerFinalizer)
				}, time.Second*15, time.Second).Should(BeTrue(), "Failed to remove finalizer in AfterEach")

				By("Waiting for StatefulSet to be fully deleted")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, sts)
					return errors.IsNotFound(err)
				}, time.Second*10, time.Millisecond*250).Should(BeTrue(), "Failed to wait for StatefulSet to be deleted")
			} else {
				Expect(errors.IsNotFound(err)).To(BeTrue(), "Expected NotFound or no error on Get before delete")
			}

			// Cleanup PriorityClass
			pc := &schedulingv1.PriorityClass{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: priorityClassName}, pc)
			if err == nil {
				By("Cleanup PriorityClass")
				Expect(k8sClient.Delete(ctx, pc)).To(Succeed())
			}
		})

		It("should return no error if StatefulSet is not found", func() {
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "non-existent",
					Namespace: "default",
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("should successfully reconcile the resource without priority", func() {
			By("Reconciling the created resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			Expect(slice.Spec.Type).To(Equal(slicev1beta1.Type("v6e")))
			Expect(slice.Spec.Topology).To(Equal("4x4"))
			Expect(slice.Annotations).ToNot(HaveKey(slicePriorityAnnotation))
		})

		It("should add priority annotation when PriorityClass is present", func() {
			By("Updating StatefulSet to include PriorityClassName")
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return err
				}
				statefulset.Spec.Template.Spec.PriorityClassName = priorityClassName
				return k8sClient.Update(ctx, statefulset)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Reconciling the resource with PriorityClass")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created with priority annotation
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			Expect(slice.Annotations).ToNot(BeNil())
			Expect(slice.Annotations[slicePriorityAnnotation]).To(Equal(strconv.Itoa(priorityVal)))
		})

		It("should not add priority annotation if PriorityClass is not found", func() {
			By("Updating StatefulSet to include a non-existent PriorityClassName")
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return err
				}
				statefulset.Spec.Template.Spec.PriorityClassName = "non-existent-pc"
				return k8sClient.Update(ctx, statefulset)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Reconciling the resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created without priority annotation
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			Expect(slice.Annotations).ToNot(HaveKey(slicePriorityAnnotation))
		})

		It("should not modify an existing slice", func() {
			By("Reconciling the created resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that the slice was not modified
			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, updatedSlice)).To(Succeed())
			Expect(updatedSlice.ResourceVersion).To(Equal(slice.ResourceVersion))
		})

		It("should successfully delete the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Delete the StatefulSet
			Expect(k8sClient.Get(ctx, typeNamespacedName, statefulset)).To(Succeed())
			Expect(k8sClient.Delete(ctx, statefulset)).To(Succeed())

			// Reconcile again to trigger deletion logic
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// If slice still exists, it should requeue
			if result.Requeue || result.RequeueAfter > 0 { //nolint:staticcheck
				By("Reconciling again after slice is gone")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
					return errors.IsNotFound(err)
				}, time.Second*10, time.Millisecond*250).Should(BeTrue())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Check if slice was deleted
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*10, time.Millisecond*250).Should(BeTrue())
		})

		It("should wait for Slice CR to be deleted before removing StatefulSet finalizer", func() {
			By("Reconciling the created resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Adding a manual finalizer to the slice to prevent immediate deletion")
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				if err != nil {
					return err
				}
				controllerutil.AddFinalizer(slice, "test.io/manual-finalizer")
				return k8sClient.Update(ctx, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Deleting the StatefulSet")
			Expect(k8sClient.Get(ctx, typeNamespacedName, statefulset)).To(Succeed())
			Expect(k8sClient.Delete(ctx, statefulset)).To(Succeed())

			By("Reconciling the StatefulSet")
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0), "handleSliceDeletion should requeue with backoff when Slice still exists")

			By("Ensuring the StatefulSet still has the finalizer and Slice still exists")
			Expect(k8sClient.Get(ctx, typeNamespacedName, statefulset)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(statefulset, sliceProvisionerFinalizer)).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)).To(Succeed())
			Expect(slice.DeletionTimestamp.IsZero()).To(BeFalse())

			By("Removing the manual finalizer from the slice")
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				if err != nil {
					return err
				}
				controllerutil.RemoveFinalizer(slice, "test.io/manual-finalizer")
				return k8sClient.Update(ctx, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Waiting for Slice to be fully deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*10, time.Millisecond*250).Should(BeTrue())

			By("Reconciling the StatefulSet again")
			result, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			By("Ensuring the StatefulSet finalizer is now removed")
			err = k8sClient.Get(ctx, typeNamespacedName, statefulset)
			if err == nil {
				Expect(controllerutil.ContainsFinalizer(statefulset, sliceProvisionerFinalizer)).To(BeFalse())
			} else {
				Expect(errors.IsNotFound(err)).To(BeTrue())
			}
		})

		It("should not create a slice if the slice-name label is missing", func() {
			By("Creating a StatefulSet without the slice-name label")
			noLabelSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-label-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "4x4",
					},
				},
				Spec: statefulset.Spec,
			}
			Expect(k8sClient.Create(ctx, noLabelSts)).To(Succeed())
			defer func() {
				if err := k8sClient.Delete(ctx, noLabelSts); err != nil {
					Expect(err).ToNot(HaveOccurred())
				}
			}()

			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "no-label-sts", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that no slice was created
			slice := &slicev1beta1.Slice{}
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*2, time.Millisecond*250).Should(BeTrue())
		})

		It("should not create a slice if the slice-type label is missing", func() {
			By("Creating a StatefulSet without the slice-type label")
			noTypeSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-type-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel:     sliceName,
						sliceTopologyLabel: "4x4",
					},
				},
				Spec: statefulset.Spec,
			}
			Expect(k8sClient.Create(ctx, noTypeSts)).To(Succeed())
			defer func() {
				if err := k8sClient.Delete(ctx, noTypeSts); err != nil {
					Expect(err).ToNot(HaveOccurred())
				}
			}()

			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "no-type-sts", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that no slice was created
			slice := &slicev1beta1.Slice{}
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*2, time.Millisecond*250).Should(BeTrue())
		})

		It("should not create a slice if the slice-topology label is missing", func() {
			By("Creating a StatefulSet without the slice-topology label")
			noTopoSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-topo-sts",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel: "test-slice",
						sliceTypeLabel: "v6e",
					},
				},
				Spec: statefulset.Spec,
			}
			Expect(k8sClient.Create(ctx, noTopoSts)).To(Succeed())
			defer func() {
				if err := k8sClient.Delete(ctx, noTopoSts); err != nil {
					Expect(err).ToNot(HaveOccurred())
				}
			}()

			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "no-topo-sts", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that no slice was created
			slice := &slicev1beta1.Slice{}
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*2, time.Millisecond*250).Should(BeTrue())

		})

		It("should not update the slice if statefulset labels do not change", func() {
			By("Reconciling the created resource")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())
			initialResourceVersion := slice.ResourceVersion

			// Update StatefulSet with a non-slice label
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return err
				}
				statefulset.Labels["some-other-label"] = "new-value"
				return k8sClient.Update(ctx, statefulset)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check Slice was not updated
			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, updatedSlice)).To(Succeed())
			Expect(updatedSlice.ResourceVersion).To(Equal(initialResourceVersion))
		})

		It("should create a slice if statefulset is updated to add slice labels", func() {
			By("Creating a StatefulSet without slice labels")
			noLabelSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "add-label-sts",
					Namespace: "default",
				},
				Spec: statefulset.Spec,
			}
			Expect(k8sClient.Create(ctx, noLabelSts)).To(Succeed())
			defer func() {
				if err := k8sClient.Delete(ctx, noLabelSts); err != nil {
					Expect(err).ToNot(HaveOccurred())
				}
			}()

			typeNamespacedName := types.NamespacedName{Name: "add-label-sts", Namespace: "default"}

			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that no slice was created yet
			slice := &slicev1beta1.Slice{}
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*2, time.Millisecond*250).Should(BeTrue())

			By("Updating the StatefulSet to add slice labels")
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, noLabelSts)
				if err != nil {
					return err
				}
				noLabelSts.Labels = map[string]string{
					sliceNameLabel:     sliceName,
					sliceTypeLabel:     "v6e",
					sliceTopologyLabel: "4x4",
				}
				return k8sClient.Update(ctx, noLabelSts)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			Expect(slice.Spec.Type).To(Equal(slicev1beta1.Type("v6e")))
			Expect(slice.Spec.Topology).To(Equal("4x4"))
		})

		It("should not delete Slice CR when slice labels are removed from StatefulSet", func() {
			By("Reconciling the created resource to create the Slice")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Check if slice was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Updating the StatefulSet to remove slice labels")
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return err
				}
				delete(statefulset.Labels, sliceNameLabel)
				delete(statefulset.Labels, sliceTypeLabel)
				delete(statefulset.Labels, sliceTopologyLabel)
				return k8sClient.Update(ctx, statefulset)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Ensuring the finalizer is removed from the StatefulSet")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return false
				}
				return !controllerutil.ContainsFinalizer(statefulset, sliceProvisionerFinalizer)
			}, time.Second*10, time.Millisecond*250).Should(BeTrue())

			By("Ensuring the Slice CR still exists")
			Consistently(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*2, time.Millisecond*250).Should(Succeed())
		})

		It("should delete and recreate Slice when topology label changes", func() {
			By("Reconciling the created resource to create the Slice with topology 4x4")
			controllerReconciler := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Slice was created with topology "4x4"
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())
			Expect(slice.Spec.Topology).To(Equal("4x4"))

			By("Updating the StatefulSet topology label to 2x2")
			Eventually(func() error {
				err := k8sClient.Get(ctx, typeNamespacedName, statefulset)
				if err != nil {
					return err
				}
				statefulset.Labels[sliceTopologyLabel] = "2x2"
				return k8sClient.Update(ctx, statefulset)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			By("Reconciling to delete the old Slice")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for old Slice to be fully deleted
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*10, time.Millisecond*250).Should(BeTrue())

			By("Reconciling again to create a new Slice with topology 2x2")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify new Slice was created with topology "2x2"
			newSlice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, newSlice)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())
			Expect(newSlice.Spec.Topology).To(Equal("2x2"))
			Expect(newSlice.Spec.Type).To(Equal(slicev1beta1.Type("v6e")))
		})

		It("should test sliceToStatefulSet map func and shouldReconcileUpdate", func() {
			r := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-map-sts",
					Namespace: "default",
					Labels: map[string]string{
						"leaderworkerset.sigs.k8s.io/name":        "test-map-lws",
						"leaderworkerset.sigs.k8s.io/group-index": "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app": "test-map",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app":                                  "test-map",
								"cloud.google.com/slice-prefix":        "test-map-slice",
								"cloud.google.com/gke-tpu-accelerator": "v6e",
								"cloud.google.com/gke-tpu-topology":    "2x2x1",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "test-c",
									Image: "busybox",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())

			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-map-slice-0",
					Namespace: "default",
				},
			}
			reqs := r.sliceToStatefulSet(ctx, slice)
			Expect(reqs).NotTo(BeEmpty())

			sts2 := sts.DeepCopy()
			sts2.Spec.Template.Labels["cloud.google.com/gke-tpu-topology"] = "2x2x2"
			Expect(r.shouldReconcileUpdate(sts, sts2)).To(BeTrue())
			Expect(r.sliceToStatefulSet(ctx, sts)).To(BeNil())
			Expect(k8sClient.Delete(ctx, sts)).To(Succeed())
		})
		It("should test workloadToStatefulSets map func", func() {
			r := &StatefulSetReconciler{}
			wl := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "wl-test",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "StatefulSet", Name: "sts-owner"},
						{Kind: "Pod", Name: "sts-pod-0-abc"},
					},
				},
			}
			reqs := r.workloadToStatefulSets(ctx, wl)
			Expect(reqs).To(HaveLen(2))
			Expect(r.workloadToStatefulSets(ctx, &corev1.Pod{})).To(BeNil())
		})

		It("should create Slice CR for LWS Leader StatefulSet when LWS size is 1", func() {

			stsName := "lws-size1-test-leader"
			sliceName := "lws-size1-prefix-0"
			leaderSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      stsName,
					Namespace: "default",
					Labels: map[string]string{
						"leaderworkerset.sigs.k8s.io/name":        "lws-size1-test",
						"leaderworkerset.sigs.k8s.io/group-index": "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "lws-size1"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"leaderworkerset.sigs.k8s.io/size": "1",
							},
							Labels: map[string]string{
								"app":                                  "lws-size1",
								"cloud.google.com/slice-prefix":        "lws-size1-prefix",
								"cloud.google.com/gke-tpu-accelerator": "tpu7x",
								"cloud.google.com/gke-tpu-topology":    "2x2x1",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "c", Image: "busybox"},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, leaderSts)
			}()

			r := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: stsName},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Slice CR was created
			slice := &slicev1beta1.Slice{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
			}, time.Second*5, time.Millisecond*100).Should(Succeed())
			Expect(string(slice.Spec.Type)).To(Equal("tpu7x"))
			Expect(slice.Spec.Topology).To(Equal("2x2x1"))

			slice.Finalizers = nil
			_ = k8sClient.Update(ctx, slice)
			_ = k8sClient.Delete(ctx, slice)
		})

		It("should skip Slice CR creation for LWS Leader StatefulSet when LWS size is > 1", func() {

			stsName := "lws-size2-test-leader"
			sliceName := "lws-size2-prefix-0"
			leaderSts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      stsName,
					Namespace: "default",
					Labels: map[string]string{
						"leaderworkerset.sigs.k8s.io/name":        "lws-size2-test",
						"leaderworkerset.sigs.k8s.io/group-index": "0",
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "lws-size2"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"leaderworkerset.sigs.k8s.io/size": "2",
							},
							Labels: map[string]string{
								"app":                                  "lws-size2",
								"cloud.google.com/slice-prefix":        "lws-size2-prefix",
								"cloud.google.com/gke-tpu-accelerator": "tpu7x",
								"cloud.google.com/gke-tpu-topology":    "2x2x2",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "c", Image: "busybox"},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, leaderSts)
			}()

			r := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: stsName},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Slice CR was NOT created
			slice := &slicev1beta1.Slice{}
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
				return errors.IsNotFound(err)
			}, time.Second*2, time.Millisecond*100).Should(BeTrue())
		})
	})

	Context("When reconciling LWS size=1 StatefulSets", func() {
		ctx := context.Background()

		cleanupSlice := func(name string) {
			s := &slicev1beta1.Slice{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, s); err == nil {
				if len(s.Finalizers) > 0 {
					s.Finalizers = nil
					_ = k8sClient.Update(ctx, s)
				}
				_ = k8sClient.Delete(ctx, s)
			}
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name}, &slicev1beta1.Slice{}))
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		}

		cleanupStatefulSet := func(nn types.NamespacedName) {
			sts := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, nn, sts); err == nil {
				if len(sts.Finalizers) > 0 {
					sts.Finalizers = nil
					_ = k8sClient.Update(ctx, sts)
				}
				_ = k8sClient.Delete(ctx, sts)
			}
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, nn, &appsv1.StatefulSet{}))
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		}

		cleanupWorkload := func(nn types.NamespacedName) {
			wl := &utils.Workload{}
			if err := k8sClient.Get(ctx, nn, wl); err == nil {
				_ = k8sClient.Delete(ctx, wl)
			}
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, nn, &utils.Workload{}))
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		}

		ensureNamespace := func(name string) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
			if err := k8sClient.Create(ctx, ns); err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		}

		newLWSSize1Leader := func(ns, name, prefix string, replicas int32) *appsv1.StatefulSet {
			r := replicas
			return &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: ns,
					Labels: map[string]string{
						labels.LWSNameLabel: name,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &r,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							labels.LWSNameLabel: name,
							"app":               name,
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								labels.LWSSizeAnnotation: "1",
							},
							Labels: map[string]string{
								labels.LWSNameLabel:       name,
								"app":                     name,
								labels.SlicePrefixLabel:   prefix,
								labels.SliceTypeLabel:     "tpu7x",
								labels.SliceTopologyLabel: "2x2x1",
							},
						},
						Spec: corev1.PodSpec{
							SchedulerName: "slice-scheduler",
							Containers: []corev1.Container{
								{Name: "leader", Image: "busybox"},
							},
						},
					},
				},
			}
		}

		It("provisions per-replica Slices, scales up and down without churning sibling prefixes, updates priority, and holds finalizer until all owned Slices delete", func() {
			const (
				stsName = "lws-size1-multi"
				prefix  = "lws-size1-multi"
				pcName  = "lws-size1-pc"
				pcValue = 250
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: stsName}

			pc := &schedulingv1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{Name: pcName},
				Value:      pcValue,
			}
			if err := k8sClient.Create(ctx, pc); err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, pc)
			})

			leaderSts := newLWSSize1Leader("default", stsName, prefix, 2)
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				for _, sName := range []string{
					"lws-size1-multi-0",
					"lws-size1-multi-1",
					"lws-size1-multi-2",
					"lws-size1-multi-3",
					"lws-size1-orphaned-slice",
				} {
					cleanupSlice(sName)
				}
			})

			// Sibling Slice whose name starts with "lws-size1-multi-" ("lws-size1-multi-v2-0")
			// must never be touched during scale-down or deletion.
			siblingSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-size1-multi-v2-0",
					Labels: map[string]string{
						sliceOwnerNameLabel:      "lws-size1-multi-v2",
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "tpu7x",
					Topology:     "2x2x1",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, siblingSlice)).To(Succeed())
			DeferCleanup(func() {
				cleanupSlice(siblingSlice.Name)
			})

			for i, wantSlice := range []string{"lws-size1-multi-0", "lws-size1-multi-1"} {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        stsName + "-" + strconv.Itoa(i),
						Namespace:   "default",
						Labels:      leaderSts.Spec.Template.Labels,
						Annotations: leaderSts.Spec.Template.Annotations,
					},
				}
				Expect(utils.LeaderPodSliceName(pod)).To(Equal(wantSlice))
			}

			r := &StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			Expect(r.shouldHandleStatefulSet(leaderSts)).To(BeTrue())

			By("Provisioning initial 2 Slices for replicas=2")
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			rvBySlice := map[string]string{}
			for _, expectedSlice := range []string{"lws-size1-multi-0", "lws-size1-multi-1"} {
				slice := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: expectedSlice}, slice)).To(Succeed())
				Expect(string(slice.Spec.Type)).To(Equal("tpu7x"))
				Expect(slice.Spec.Topology).To(Equal("2x2x1"))
				Expect(slice.Labels[sliceOwnerNameLabel]).To(Equal(stsName))
				Expect(slice.Labels[sliceOwnerNamespaceLabel]).To(Equal("default"))
				Expect(slice.Labels[sliceOwnerKindLabel]).To(Equal(utils.KindStatefulSet))
				rvBySlice[expectedSlice] = slice.ResourceVersion
			}

			By("Verifying sliceToStatefulSet O(1) labeled fast path, unlabeled fallback, and non-StatefulSet ignore")
			slice1 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-1"}, slice1)).To(Succeed())
			Expect(r.sliceToStatefulSet(ctx, slice1)).To(Equal([]reconcile.Request{{NamespacedName: stsNN}}))

			unlabeledSlice := &slicev1beta1.Slice{ObjectMeta: metav1.ObjectMeta{Name: "lws-size1-multi-9"}}
			Expect(r.sliceToStatefulSet(ctx, unlabeledSlice)).To(ContainElement(reconcile.Request{NamespacedName: stsNN}))

			jobOwnedSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "lws-size1-multi-0",
					Labels: map[string]string{sliceOwnerKindLabel: utils.KindJob},
				},
			}
			Expect(r.sliceToStatefulSet(ctx, jobOwnedSlice)).To(BeNil())

			By("Scaling up replicas 2 -> 4 without churning existing Slices 0 and 1")
			var current appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			scaledUp := current.DeepCopy()
			four := int32(4)
			scaledUp.Spec.Replicas = &four
			Expect(r.shouldReconcileUpdate(&current, scaledUp)).To(BeTrue())
			Expect(k8sClient.Update(ctx, scaledUp)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			for _, expectedSlice := range []string{"lws-size1-multi-0", "lws-size1-multi-1", "lws-size1-multi-2", "lws-size1-multi-3"} {
				slice := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: expectedSlice}, slice)).To(Succeed())
				if prevRV, ok := rvBySlice[expectedSlice]; ok {
					Expect(slice.ResourceVersion).To(Equal(prevRV))
				}
			}

			By("Scaling down replicas 4 -> 2 and preserving sibling prefix Slices")
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			two := int32(2)
			current.Spec.Replicas = &two
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			for _, kept := range []string{"lws-size1-multi-0", "lws-size1-multi-1", "lws-size1-multi-v2-0"} {
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: kept}, &slicev1beta1.Slice{})).To(Succeed())
			}
			for _, removed := range []string{"lws-size1-multi-2", "lws-size1-multi-3"} {
				Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: removed}, &slicev1beta1.Slice{}))).To(BeTrue())
			}

			By("Updating PriorityClassName across all active Slices")
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			current.Spec.Template.Spec.PriorityClassName = pcName
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			for _, kept := range []string{"lws-size1-multi-0", "lws-size1-multi-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: kept}, s)).To(Succeed())
				Expect(s.Annotations[slicePriorityAnnotation]).To(Equal(strconv.Itoa(pcValue)))
			}

			By("Scaling down replicas 2 -> 0 and cleaning up all owned Slices while preserving finalizer")
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			zero := int32(0)
			current.Spec.Replicas = &zero
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			for _, removed := range []string{"lws-size1-multi-0", "lws-size1-multi-1"} {
				Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: removed}, &slicev1beta1.Slice{}))).To(BeTrue())
			}
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&current, sliceProvisionerFinalizer)).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-v2-0"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Scaling back up 0 -> 2 before testing deletion finalizer hold")
			current.Spec.Replicas = &two
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			// Also create an orphaned Slice owned by this StatefulSet whose name does not match spec.Prefix
			// to verify StatefulSet deletion cleans up ALL owned Slices.
			orphanedOwned := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-size1-orphaned-slice",
					Labels: map[string]string{
						sliceOwnerNameLabel:      stsName,
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "2x2x1", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, orphanedOwned)).To(Succeed())

			By("Holding StatefulSet finalizer during deletion until ALL owned Slices finish deleting")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-1"}, slice1)).To(Succeed())
			controllerutil.AddFinalizer(slice1, "test.io/external-slice-finalizer")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())

			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &current)).To(Succeed())

			delRes, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(delRes.RequeueAfter).To(BeNumerically(">", 0))

			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-orphaned-slice"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(&current, sliceProvisionerFinalizer)).To(BeTrue())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-1"}, slice1)).To(Succeed())
			controllerutil.RemoveFinalizer(slice1, "test.io/external-slice-finalizer")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-size1-multi-1"}, &slicev1beta1.Slice{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

			delRes, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(delRes.RequeueAfter).To(BeZero())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, stsNN, &appsv1.StatefulSet{}))).To(BeTrue())
		})

		It("requeues cleanly when a desired Slice is terminating or undergoes a topology change", func() {
			const (
				stsName = "lws-size1-requeue"
				prefix  = "lws-size1-requeue"
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: stsName}

			leaderSts := newLWSSize1Leader("default", stsName, prefix, 2)
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			By("Holding Slice-1 with a finalizer during scale-down 2 -> 1, then immediately scaling back up 1 -> 2")
			slice1 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			controllerutil.AddFinalizer(slice1, "test.io/terminating-hold")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())

			var current appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			one := int32(1)
			current.Spec.Replicas = &one
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			Expect(slice1.DeletionTimestamp.IsZero()).To(BeFalse())

			// Scale back up while Slice-1 is still terminating -> Reconcile must requeue cleanly.
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			two := int32(2)
			current.Spec.Replicas = &two
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(topologyChangeRequeueInterval))

			// Release terminating Slice-1 and reconcile to recreate fresh Slice-1.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			controllerutil.RemoveFinalizer(slice1, "test.io/terminating-hold")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{}))
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			Expect(slice1.DeletionTimestamp.IsZero()).To(BeTrue())

			By("Deleting and recreating all replica Slices when topology changes")
			Expect(k8sClient.Get(ctx, stsNN, &current)).To(Succeed())
			current.Spec.Template.Labels[labels.SliceTopologyLabel] = "2x2x2"
			Expect(k8sClient.Update(ctx, &current)).To(Succeed())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(topologyChangeRequeueInterval))

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			for _, sName := range []string{prefix + "-0", prefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.Spec.Topology).To(Equal("2x2x2"))
			}
		})

		It("hands per-replica Slices over from the leader to worker StatefulSets when LWS size changes 1 -> 2", func() {
			const (
				lwsName = "lws-handover-up"
				prefix  = "lws-handover-up"
			)
			leaderNN := types.NamespacedName{Namespace: "default", Name: lwsName}
			workerNN := types.NamespacedName{Namespace: "default", Name: lwsName + "-1"}

			leader := newLWSSize1Leader("default", lwsName, prefix, 2)
			Expect(k8sClient.Create(ctx, leader)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(leaderNN)
				cleanupStatefulSet(workerNN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())

			// Hold Slice-1 with an external finalizer so the handover is observable mid-flight.
			slice1 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			controllerutil.AddFinalizer(slice1, "test.io/handover-hold")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())

			By("LWS begins partitioned rollout to size=2 (partition=1): Slice-0 stays pinned while Slice-1 hands over")
			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			leader.Spec.Template.Annotations[labels.LWSSizeAnnotation] = "2"
			partOne := int32(1)
			leader.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
				Type:          appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partOne},
			}
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			// Slice-0 is still pinned by partition=1 and must remain alive and owned by leader.
			slice0 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, slice0)).To(Succeed())
			Expect(slice0.DeletionTimestamp.IsZero()).To(BeTrue())
			Expect(slice0.Labels[sliceOwnerNameLabel]).To(Equal(lwsName))
			// Slice-1 (>= partition) is terminating; leader keeps its finalizer.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			Expect(slice1.DeletionTimestamp.IsZero()).To(BeFalse())
			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeTrue())

			By("The group-1 worker StatefulSet waits for the leader's terminating Slice instead of erroring")
			worker := newLWSSize1Leader("default", workerNN.Name, prefix, 1)
			worker.Labels = map[string]string{
				labels.LWSNameLabel:       lwsName,
				labels.LWSRoleLabel:       utils.LWSWorkerRole,
				labels.LWSGroupIndexLabel: "1",
			}
			worker.Spec.Selector.MatchLabels = map[string]string{"app": workerNN.Name}
			worker.Spec.Template.Labels[labels.LWSNameLabel] = lwsName
			worker.Spec.Template.Annotations = map[string]string{
				labels.LWSSizeAnnotation:       "2",
				labels.LWSLeaderNameAnnotation: lwsName + "-1",
			}
			Expect(k8sClient.Create(ctx, worker)).To(Succeed())

			// Verify sliceToStatefulSet wakes BOTH the former leader owner AND the waiting worker counterparty.
			Expect(r.sliceToStatefulSet(ctx, slice1)).To(ConsistOf(
				reconcile.Request{NamespacedName: leaderNN},
				reconcile.Request{NamespacedName: workerNN},
			))

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: workerNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(topologyChangeRequeueInterval))

			By("Releasing held Slice-1 lets worker-1 claim Slice-1 while leader retains finalizer for pinned Slice-0")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, slice1)).To(Succeed())
			controllerutil.RemoveFinalizer(slice1, "test.io/handover-hold")
			Expect(k8sClient.Update(ctx, slice1)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{}))
			}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, slice0)).To(Succeed())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: workerNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			workerSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, workerSlice)).To(Succeed())
			Expect(workerSlice.Labels[sliceOwnerNameLabel]).To(Equal(workerNN.Name))

			By("Advancing partition 1 -> 0 triggers update predicate, releases Slice-0, and drops leader finalizer")
			oldLeader := leader.DeepCopy()
			partZero := int32(0)
			leader.Spec.UpdateStrategy.RollingUpdate.Partition = &partZero
			Expect(r.shouldReconcileUpdate(oldLeader, leader)).To(BeTrue())
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeTrue())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeFalse())
		})

		It("takes over group Slices during partitioned LWS size 2 -> 1 rollout without blocking released higher ordinals", func() {
			const (
				lwsName = "lws-handover-down"
				prefix  = "lws-handover-down"
			)
			leaderNN := types.NamespacedName{Namespace: "default", Name: lwsName}
			worker0NN := types.NamespacedName{Namespace: "default", Name: lwsName + "-0"}
			worker1NN := types.NamespacedName{Namespace: "default", Name: lwsName + "-1"}

			leader := newLWSSize1Leader("default", lwsName, prefix, 2)
			leader.Spec.Template.Annotations[labels.LWSSizeAnnotation] = "2"
			Expect(k8sClient.Create(ctx, leader)).To(Succeed())

			newWorker := func(idx string) *appsv1.StatefulSet {
				wName := lwsName + "-" + idx
				w := newLWSSize1Leader("default", wName, prefix, 1)
				w.Labels = map[string]string{
					labels.LWSNameLabel:       lwsName,
					labels.LWSRoleLabel:       utils.LWSWorkerRole,
					labels.LWSGroupIndexLabel: idx,
				}
				w.Spec.Selector.MatchLabels = map[string]string{"app": wName}
				w.Spec.Template.Labels[labels.LWSNameLabel] = lwsName
				w.Spec.Template.Annotations = map[string]string{
					labels.LWSSizeAnnotation:       "2",
					labels.LWSLeaderNameAnnotation: wName,
				}
				return w
			}
			worker0 := newWorker("0")
			worker1 := newWorker("1")
			Expect(k8sClient.Create(ctx, worker0)).To(Succeed())
			Expect(k8sClient.Create(ctx, worker1)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(leaderNN)
				cleanupStatefulSet(worker0NN)
				cleanupStatefulSet(worker1NN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: worker0NN})
			Expect(err).NotTo(HaveOccurred())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: worker1NN})
			Expect(err).NotTo(HaveOccurred())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())

			By("LWS rolls ordinal 1 first (deleting worker-1 while worker-0 still owns Slice-0)")
			Expect(k8sClient.Get(ctx, worker1NN, worker1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, worker1)).To(Succeed())
			Eventually(func() bool {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: worker1NN})
				return err == nil && errors.IsNotFound(k8sClient.Get(ctx, worker1NN, &appsv1.StatefulSet{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

			Expect(k8sClient.Get(ctx, leaderNN, leader)).To(Succeed())
			leader.Spec.Template.Annotations[labels.LWSSizeAnnotation] = "1"
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			// Even though Slice-0 hits SliceOwnerConflict against worker-0, leader MUST still provision Slice-1!
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already owned by StatefulSet default/" + worker0NN.Name))
			s1 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, s1)).To(Succeed())
			Expect(s1.Labels[sliceOwnerNameLabel]).To(Equal(lwsName))

			By("Deleting worker-0 releases Slice-0 and wakes leader via sliceToStatefulSet")
			s0BeforeDelete := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, s0BeforeDelete)).To(Succeed())
			Expect(r.sliceToStatefulSet(ctx, s0BeforeDelete)).To(ConsistOf(
				reconcile.Request{NamespacedName: worker0NN},
				reconcile.Request{NamespacedName: leaderNN},
			))

			Expect(k8sClient.Get(ctx, worker0NN, worker0)).To(Succeed())
			Expect(k8sClient.Delete(ctx, worker0)).To(Succeed())
			Eventually(func() bool {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: worker0NN})
				return err == nil && errors.IsNotFound(k8sClient.Get(ctx, worker0NN, &appsv1.StatefulSet{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			for _, sName := range []string{prefix + "-0", prefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.Labels[sliceOwnerNameLabel]).To(Equal(lwsName))
			}
		})

		It("cleans up obsolete Slices across size=1 <-> LeaderExcluded transitions and when TPU intent is removed", func() {
			const (
				lwsName = "lws-excl-trans"
				prefix  = "lws-excl-trans"
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: lwsName}
			leader := newLWSSize1Leader("default", lwsName, prefix, 2)
			Expect(k8sClient.Create(ctx, leader)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
				cleanupSlice(prefix + "-leader")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Transitioning size=1 -> LeaderExcluded deletes per-replica Slices and creates <prefix>-leader")
			Expect(k8sClient.Get(ctx, stsNN, leader)).To(Succeed())
			leader.Spec.Template.Annotations = map[string]string{
				labels.LWSSizeAnnotation:               "4",
				labels.LWSSubgroupSizeAnnotation:       "2",
				labels.LWSSubgroupPolicyTypeAnnotation: utils.LWSLeaderExcludedPolicy,
			}
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-leader"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Transitioning LeaderExcluded -> size=1 deletes <prefix>-leader and creates per-replica Slices")
			Expect(k8sClient.Get(ctx, stsNN, leader)).To(Succeed())
			leader.Spec.Template.Annotations = map[string]string{labels.LWSSizeAnnotation: "1"}
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-leader"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Removing TPU intent deletes owned Slices before removing sliceProvisionerFinalizer")
			Expect(k8sClient.Get(ctx, stsNN, leader)).To(Succeed())
			delete(leader.Spec.Template.Labels, labels.SlicePrefixLabel)
			delete(leader.Spec.Template.Labels, labels.SliceTypeLabel)
			delete(leader.Spec.Template.Labels, labels.SliceTopologyLabel)
			Expect(k8sClient.Update(ctx, leader)).To(Succeed())

			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, stsNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeTrue())

			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(k8sClient.Get(ctx, stsNN, leader)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(leader, sliceProvisionerFinalizer)).To(BeFalse())
		})

		It("isolates cross-namespace Slice ownership across collisions, scale-down, and deletion", func() {
			ensureNamespace("lws-ns-a")
			ensureNamespace("lws-ns-b")

			const sharedPrefix = "cross-ns-llama"
			nnA := types.NamespacedName{Namespace: "lws-ns-a", Name: "llama"}
			nnB := types.NamespacedName{Namespace: "lws-ns-b", Name: "llama"}

			stsA := newLWSSize1Leader("lws-ns-a", "llama", sharedPrefix, 2)
			Expect(k8sClient.Create(ctx, stsA)).To(Succeed())
			DeferCleanup(func() {
				cleanupStatefulSet(nnA)
				cleanupStatefulSet(nnB)
				cleanupSlice(sharedPrefix + "-0")
				cleanupSlice(sharedPrefix + "-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nnA})
			Expect(err).NotTo(HaveOccurred())

			for _, sName := range []string{sharedPrefix + "-0", sharedPrefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.Labels[sliceOwnerNamespaceLabel]).To(Equal("lws-ns-a"))
				Expect(s.Labels[sliceOwnerNameLabel]).To(Equal("llama"))
			}

			// Colliding LWS in lws-ns-b with the same StatefulSet name "llama" and replicas=1
			// (e.g. admitted when --enable-webhook=false).
			stsB := newLWSSize1Leader("lws-ns-b", "llama", sharedPrefix, 1)
			Expect(k8sClient.Create(ctx, stsB)).To(Succeed())

			By("Rejecting foreign-owned Slice-0 on reconcile without deleting lws-ns-a's Slice-1 during scale-down")
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already owned by StatefulSet lws-ns-a/llama"))

			// Both Slices owned by lws-ns-a/llama must still exist untouched.
			for _, sName := range []string{sharedPrefix + "-0", sharedPrefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.DeletionTimestamp.IsZero()).To(BeTrue())
				Expect(s.Labels[sliceOwnerNamespaceLabel]).To(Equal("lws-ns-a"))
			}

			By("Ensuring unadmitted Kueue cleanup on lws-ns-b/llama does not delete lws-ns-a's Slices")
			Expect(k8sClient.Get(ctx, nnB, stsB)).To(Succeed())
			stsB.Labels[labels.KueueQueueNameLabel] = "unadmitted-queue"
			Expect(k8sClient.Update(ctx, stsB)).To(Succeed())

			kueueR := &StatefulSetReconciler{
				Client:               k8sClient,
				Scheme:               k8sClient.Scheme(),
				EnableKueueAdmission: true,
			}
			_, err = kueueR.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			for _, sName := range []string{sharedPrefix + "-0", sharedPrefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.DeletionTimestamp.IsZero()).To(BeTrue())
			}

			By("Deleting lws-ns-b/llama removes its finalizer without deleting lws-ns-a's Slices")
			Expect(k8sClient.Get(ctx, nnB, stsB)).To(Succeed())
			Expect(k8sClient.Delete(ctx, stsB)).To(Succeed())
			delRes, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(delRes.RequeueAfter).To(BeZero())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, nnB, &appsv1.StatefulSet{}))).To(BeTrue())

			for _, sName := range []string{sharedPrefix + "-0", sharedPrefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.DeletionTimestamp.IsZero()).To(BeTrue())
			}

			By("Protecting legacy un-backfilled Slices in lws-ns-a when deleting or reconciling identically named LWS in lws-ns-b")
			legacyUnstamped := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns-a-legacy-0",
					Labels: map[string]string{
						sliceOwnerNameLabel: "llama",
						sliceOwnerKindLabel: utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "2x2x1", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, legacyUnstamped)).To(Succeed())
			DeferCleanup(func() {
				cleanupSlice("ns-a-legacy-0")
				cleanupSlice("ns-b-distinct-0")
				cleanupSlice("ns-b-optout-0")
			})

			// Case 1 (#5): CPU-only size > 1 LWS leader in lws-ns-b must not delete lws-ns-a's legacy Slice.
			cpuOnlyLeaderB := newLWSSize1Leader("lws-ns-b", "llama", "", 1)
			cpuOnlyLeaderB.Spec.Template.Annotations = map[string]string{labels.LWSSizeAnnotation: "2"}
			delete(cpuOnlyLeaderB.Spec.Template.Labels, labels.SlicePrefixLabel)
			delete(cpuOnlyLeaderB.Spec.Template.Labels, labels.SliceTypeLabel)
			delete(cpuOnlyLeaderB.Spec.Template.Labels, labels.SliceTopologyLabel)
			Expect(k8sClient.Create(ctx, cpuOnlyLeaderB)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-a-legacy-0"}, &slicev1beta1.Slice{})).To(Succeed())
			cleanupStatefulSet(nnB)

			// Case 2 (#9): Opted-in LWS in lws-ns-b that later removes TPU intent (opting out while holding
			// sliceProvisionerFinalizer) must release only its own Slice and NEVER delete lws-ns-a's legacy Slice.
			optOutB := newLWSSize1Leader("lws-ns-b", "llama", "ns-b-optout", 1)
			Expect(k8sClient.Create(ctx, optOutB)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-b-optout-0"}, &slicev1beta1.Slice{})).To(Succeed())

			Expect(k8sClient.Get(ctx, nnB, optOutB)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(optOutB, sliceProvisionerFinalizer)).To(BeTrue())
			delete(optOutB.Spec.Template.Labels, labels.SlicePrefixLabel)
			delete(optOutB.Spec.Template.Labels, labels.SliceTypeLabel)
			delete(optOutB.Spec.Template.Labels, labels.SliceTopologyLabel)
			Expect(k8sClient.Update(ctx, optOutB)).To(Succeed())

			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-b-optout-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-a-legacy-0"}, &slicev1beta1.Slice{})).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nnB, optOutB)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(optOutB, sliceProvisionerFinalizer)).To(BeFalse())
			cleanupStatefulSet(nnB)

			// Case 3 (#2): Opted-in LWS in lws-ns-b with distinct prefix "ns-b-distinct" must not delete "ns-a-legacy-0" on deletion.
			distinctB := newLWSSize1Leader("lws-ns-b", "llama", "ns-b-distinct", 1)
			Expect(k8sClient.Create(ctx, distinctB)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-b-distinct-0"}, &slicev1beta1.Slice{})).To(Succeed())

			Expect(k8sClient.Get(ctx, nnB, distinctB)).To(Succeed())
			Expect(k8sClient.Delete(ctx, distinctB)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nnB})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-b-distinct-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ns-a-legacy-0"}, &slicev1beta1.Slice{})).To(Succeed())
		})

		It("does not resurrect phantom Slices from stale mutator-stamped top-level labels when an LWS opts out after LeaderExcluded -> size=1", func() {
			const (
				stsName = "lws-stale-top-labels"
				prefix  = "lws-stale-top"
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: stsName}
			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				cleanupSlice(prefix + "-leader")
				cleanupSlice(prefix + "-0")
			})

			sts := newLWSSize1Leader("default", stsName, prefix, 1)
			// Simulate top-level labels left behind by an earlier LeaderExcluded mutation.
			sts.Labels[labels.SliceNameLabel] = prefix + "-leader"
			sts.Labels[labels.SliceTypeLabel] = "v6e"
			sts.Labels[labels.SliceTopologyLabel] = "2x2x1"
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{})).To(Succeed())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-leader"}, &slicev1beta1.Slice{}))).To(BeTrue())

			By("Removing TPU labels from Spec.Template.Labels while stale top-level labels remain")
			Expect(k8sClient.Get(ctx, stsNN, sts)).To(Succeed())
			delete(sts.Spec.Template.Labels, labels.SlicePrefixLabel)
			delete(sts.Spec.Template.Labels, labels.SliceTypeLabel)
			delete(sts.Spec.Template.Labels, labels.SliceTopologyLabel)
			Expect(k8sClient.Update(ctx, sts)).To(Succeed())

			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-leader"}, &slicev1beta1.Slice{}))).To(BeTrue())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-leader"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, stsNN, sts)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(sts, sliceProvisionerFinalizer)).To(BeFalse())
		})

		It("skips pre-partition worker-owned Slices without SliceOwnerConflict errors during partitioned size>1 -> size=1 rollouts", func() {
			const (
				stsName = "lws-rev-rollout"
				prefix  = "lws-rev-roll"
			)
			leaderNN := types.NamespacedName{Namespace: "default", Name: stsName}
			worker0NN := types.NamespacedName{Namespace: "default", Name: stsName + "-0"}

			workerSlice0 := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: prefix + "-0",
					Labels: map[string]string{
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerNameLabel:      worker0NN.Name,
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "2x2x1", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, workerSlice0)).To(Succeed())

			partOne := int32(1)
			leaderSts := newLWSSize1Leader("default", stsName, prefix, 2)
			leaderSts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
				Type:          appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partOne},
			}
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())

			DeferCleanup(func() {
				cleanupStatefulSet(leaderNN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			conflictBefore := testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))

			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))).To(Equal(conflictBefore))

			// Ordinal 1 (>= partition) is provisioned for the leader, while ordinal 0 (< partition) stays owned by worker-0.
			s1 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, s1)).To(Succeed())
			Expect(s1.Labels[sliceOwnerNameLabel]).To(Equal(stsName))

			s0 := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, s0)).To(Succeed())
			Expect(s0.Labels[sliceOwnerNameLabel]).To(Equal(worker0NN.Name))

			// Once partition drops to 0 and worker-0 deletes Slice-0, the leader claims Slice-0 cleanly.
			partZero := int32(0)
			Expect(k8sClient.Get(ctx, leaderNN, leaderSts)).To(Succeed())
			leaderSts.Spec.UpdateStrategy.RollingUpdate.Partition = &partZero
			Expect(k8sClient.Update(ctx, leaderSts)).To(Succeed())
			Expect(k8sClient.Delete(ctx, s0)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: leaderNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, s0)).To(Succeed())
			Expect(s0.Labels[sliceOwnerNameLabel]).To(Equal(stsName))
		})

		It("heals an upgraded 0.6.0 LWS size=1 leader and provisions LeaderExcluded and role-labeled worker Slices", func() {
			const (
				stsName = "lws-size1-upgraded"
				prefix  = "lws-size1-upgraded"
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: stsName}

			legacySlice0 := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{Name: prefix + "-0"},
				Spec:       slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "2x2x1", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, legacySlice0)).To(Succeed())

			upgradedSts := newLWSSize1Leader("default", stsName, prefix, 2)
			upgradedSts.Labels[labels.SliceNameLabel] = prefix + "-0"
			upgradedSts.Labels[labels.SliceTypeLabel] = "tpu7x"
			upgradedSts.Labels[labels.SliceTopologyLabel] = "2x2x1"
			upgradedSts.Spec.Template.Labels[labels.SliceNameLabel] = prefix + "-0"
			upgradedSts.Spec.Template.Annotations[labels.SliceNameAnnotation] = prefix + "-0"
			Expect(k8sClient.Create(ctx, upgradedSts)).To(Succeed())

			excludedNN := types.NamespacedName{Namespace: "default", Name: "lws-leader-excluded"}
			excludedSts := newLWSSize1Leader("default", excludedNN.Name, "lws-excl", 1)
			excludedSts.Spec.Template.Annotations = map[string]string{
				labels.LWSSizeAnnotation:               "4",
				labels.LWSSubgroupSizeAnnotation:       "2",
				labels.LWSSubgroupPolicyTypeAnnotation: utils.LWSLeaderExcludedPolicy,
			}
			Expect(k8sClient.Create(ctx, excludedSts)).To(Succeed())

			workerNN := types.NamespacedName{Namespace: "default", Name: "lws-role-worker-1"}
			workerSts := newLWSSize1Leader("default", workerNN.Name, "lws-role-w", 1)
			workerSts.Labels = map[string]string{
				labels.LWSNameLabel:       "lws-role-worker",
				labels.LWSRoleLabel:       utils.LWSWorkerRole,
				labels.LWSGroupIndexLabel: "1",
			}
			workerSts.Spec.Selector.MatchLabels = map[string]string{"app": workerNN.Name}
			workerSts.Spec.Template.Labels["app"] = workerNN.Name
			workerSts.Spec.Template.Annotations = map[string]string{labels.LWSSizeAnnotation: "2"}
			Expect(k8sClient.Create(ctx, workerSts)).To(Succeed())

			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				cleanupStatefulSet(excludedNN)
				cleanupStatefulSet(workerNN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
				cleanupSlice("lws-excl-leader")
				cleanupSlice("lws-role-w-1")
			})

			r := &StatefulSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			for _, sName := range []string{prefix + "-0", prefix + "-1"} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, s)).To(Succeed())
				Expect(s.Labels[sliceOwnerNameLabel]).To(Equal(stsName))
				Expect(s.Labels[sliceOwnerNamespaceLabel]).To(Equal("default"))
				Expect(s.Labels[sliceOwnerKindLabel]).To(Equal(utils.KindStatefulSet))
			}

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: excludedNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-excl-leader"}, &slicev1beta1.Slice{})).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: workerNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-role-w-1"}, &slicev1beta1.Slice{})).To(Succeed())
		})

		It("gates LWS Slices per Kueue replica group across size=1 leaders and size>1 workers, reads parent LWS queue-name, and propagates LWS lookup errors", func() {
			const (
				stsName  = "lws-kueue-grp"
				prefix   = "lws-kueue-grp"
				tpuQueue = "tpu-queue"
			)
			stsNN := types.NamespacedName{Namespace: "default", Name: stsName}
			wl0NN := types.NamespacedName{Namespace: "default", Name: "leaderworkerset-lws-kueue-grp-0-11111"}
			wl1NN := types.NamespacedName{Namespace: "default", Name: "leaderworkerset-lws-kueue-grp-1-22222"}
			trailingWorkerNN := types.NamespacedName{Namespace: "default", Name: stsName + "-1"}

			leaderSts := newLWSSize1Leader("default", stsName, prefix, 2)
			leaderSts.Spec.Template.Labels[labels.KueueQueueNameLabel] = tpuQueue
			Expect(k8sClient.Create(ctx, leaderSts)).To(Succeed())

			// #12 + #16: size > 1 subgrouped worker carrying group index ONLY via trailing StatefulSet name "-1".
			trailingWorker := newLWSSize1Leader("default", trailingWorkerNN.Name, "lws-kueue-w", 1)
			trailingWorker.Labels = map[string]string{labels.LWSNameLabel: stsName}
			trailingWorker.Spec.Selector.MatchLabels = map[string]string{"app": trailingWorkerNN.Name}
			trailingWorker.Spec.Template.Labels["app"] = trailingWorkerNN.Name
			trailingWorker.Spec.Template.Labels[labels.KueueQueueNameLabel] = tpuQueue
			trailingWorker.Spec.Template.Annotations = map[string]string{
				labels.LWSSizeAnnotation:         "4",
				labels.LWSSubgroupSizeAnnotation: "2",
				labels.LWSLeaderNameAnnotation:   stsName + "-1",
			}
			Expect(k8sClient.Create(ctx, trailingWorker)).To(Succeed())

			DeferCleanup(func() {
				cleanupStatefulSet(stsNN)
				cleanupStatefulSet(trailingWorkerNN)
				cleanupWorkload(wl0NN)
				cleanupWorkload(wl1NN)
				cleanupSlice(prefix + "-0")
				cleanupSlice(prefix + "-1")
				cleanupSlice("lws-kueue-w-1")
			})

			r := &StatefulSetReconciler{
				Client:               k8sClient,
				Scheme:               k8sClient.Scheme(),
				EnableKueueAdmission: true,
			}

			By("Holding all Slices before any Kueue Workload CR exists")
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			for _, sName := range []string{prefix + "-0", prefix + "-1"} {
				Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: sName}, &slicev1beta1.Slice{}))).To(BeTrue())
			}

			By("Admitting only replica group 0 provisions Slice-0 while holding Slice-1 on both size=1 leader and trailing-index size>1 worker (#12)")
			wl0 := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wl0NN.Name,
					Namespace: wl0NN.Namespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "leaderworkerset.x-k8s.io/v1",
						Kind:       utils.KindLeaderWorkerSet,
						Name:       stsName,
						UID:        leaderSts.UID,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, wl0)).To(Succeed())
			wl0.Status.Conditions = []metav1.Condition{{
				Type:               utils.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, wl0)).To(Succeed())

			wl1 := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wl1NN.Name,
					Namespace: wl1NN.Namespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "leaderworkerset.x-k8s.io/v1",
						Kind:       utils.KindLeaderWorkerSet,
						Name:       stsName,
						UID:        leaderSts.UID,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, wl1)).To(Succeed())

			Expect(r.workloadToStatefulSets(ctx, wl0)).To(ContainElements(
				reconcile.Request{NamespacedName: stsNN},
				reconcile.Request{NamespacedName: trailingWorkerNN},
			))
			podOwnedWl := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-lws-kueue-grp-0-hash",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1",
						Kind:       utils.KindPod,
						Name:       stsName + "-0",
					}},
				},
			}
			Expect(r.workloadToStatefulSets(ctx, podOwnedWl)).To(ConsistOf(
				reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: stsName + "-0"}},
				reconcile.Request{NamespacedName: stsNN},
			))

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{})).To(Succeed())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{}))).To(BeTrue())

			// Group 1 worker must NOT provision lws-kueue-w-1 even though group 0 is admitted!
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: trailingWorkerNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-kueue-w-1"}, &slicev1beta1.Slice{}))).To(BeTrue())

			By("Admitting replica group 1 provisions Slice-1 on both leader and worker")
			Expect(k8sClient.Get(ctx, wl1NN, wl1)).To(Succeed())
			wl1.Status.Conditions = []metav1.Condition{{
				Type:               utils.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, wl1)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{})).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: trailingWorkerNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lws-kueue-w-1"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Evicting replica group 0 cleans up Slice-0 while keeping admitted Slice-1 intact")
			Expect(k8sClient.Get(ctx, wl0NN, wl0)).To(Succeed())
			wl0.Status.Conditions = append(wl0.Status.Conditions, metav1.Condition{
				Type:               utils.WorkloadEvicted,
				Status:             metav1.ConditionTrue,
				Reason:             "Preempted",
				LastTransitionTime: metav1.Now(),
			})
			Expect(k8sClient.Status().Update(ctx, wl0)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: stsNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: prefix + "-1"}, &slicev1beta1.Slice{})).To(Succeed())

			By("Reading queue-name from unstructured parent LeaderWorkerSet CR and propagating Forbidden lookup errors (#16)")
			sc := runtime.NewScheme()
			Expect(appsv1.AddToScheme(sc)).To(Succeed())
			Expect(schedulingv1.AddToScheme(sc)).To(Succeed())
			Expect(slicev1beta1.AddToScheme(sc)).To(Succeed())
			Expect(utils.AddWorkloadToScheme(sc)).To(Succeed())

			parentQueueSts := newLWSSize1Leader("default", "lws-parent-q", "lws-parent-q", 1)
			parentQueueClient := fake.NewClientBuilder().
				WithScheme(sc).
				WithObjects(parentQueueSts).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == utils.KindLeaderWorkerSet {
							u.SetLabels(map[string]string{labels.KueueQueueNameLabel: "parent-queue"})
							return nil
						}
						return c.Get(ctx, key, obj, opts...)
					},
				}).
				Build()
			parentQueueR := &StatefulSetReconciler{
				Client:               parentQueueClient,
				Scheme:               sc,
				EnableKueueAdmission: true,
			}
			_, err = parentQueueR.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "lws-parent-q"},
			})
			Expect(err).NotTo(HaveOccurred())
			// Because parent LWS supplied queue-name="parent-queue" and no Workload exists yet, Slice creation is held!
			Expect(errors.IsNotFound(parentQueueClient.Get(ctx, types.NamespacedName{Name: "lws-parent-q-0"}, &slicev1beta1.Slice{}))).To(BeTrue())

			noQueueLabelSts := newLWSSize1Leader("default", "lws-rbac-err", "lws-rbac-err", 1)
			forbiddenClient := fake.NewClientBuilder().
				WithScheme(sc).
				WithObjects(noQueueLabelSts).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if obj.GetObjectKind().GroupVersionKind().Kind == utils.KindLeaderWorkerSet {
							return errors.NewForbidden(schema.GroupResource{Group: "leaderworkerset.x-k8s.io", Resource: "leaderworkersets"}, key.Name, nil)
						}
						return c.Get(ctx, key, obj, opts...)
					},
				}).
				Build()

			forbiddenR := &StatefulSetReconciler{
				Client:               forbiddenClient,
				Scheme:               sc,
				EnableKueueAdmission: true,
			}
			_, err = forbiddenR.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "lws-rbac-err"},
			})
			Expect(err).To(HaveOccurred())
			Expect(errors.IsForbidden(err)).To(BeTrue())
			Expect(errors.IsNotFound(forbiddenClient.Get(ctx, types.NamespacedName{Name: "lws-rbac-err-0"}, &slicev1beta1.Slice{}))).To(BeTrue())

			By("Reconciling queue-name label updates and evicting retained < partition Slices during size=1 -> size>1 rollouts")
			oldQueueSts := newLWSSize1Leader("default", "lws-queue-pred", "lws-queue-pred", 1)
			newTopQueueSts := oldQueueSts.DeepCopy()
			newTopQueueSts.Labels[labels.KueueQueueNameLabel] = "q1"
			Expect(r.shouldReconcileUpdate(oldQueueSts, newTopQueueSts)).To(BeTrue())

			newTplQueueSts := oldQueueSts.DeepCopy()
			newTplQueueSts.Spec.Template.Labels[labels.KueueQueueNameLabel] = "q2"
			Expect(r.shouldReconcileUpdate(oldQueueSts, newTplQueueSts)).To(BeTrue())

			partOne := int32(1)
			pinnedLeader := newLWSSize1Leader("default", "lws-pinned-evict", "lws-pin-ev", 2)
			pinnedLeader.Labels[labels.KueueQueueNameLabel] = tpuQueue
			pinnedLeader.Finalizers = []string{sliceProvisionerFinalizer}
			pinnedLeader.Spec.Template.Annotations = map[string]string{labels.LWSSizeAnnotation: "2"}
			pinnedLeader.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
				Type:          appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partOne},
			}
			pinnedSlice0 := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "lws-pin-ev-0",
					Labels: map[string]string{
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerNameLabel:      "lws-pinned-evict",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "tpu7x", Topology: "2x2x1", PartitionIds: []string{}},
			}
			pinnedWl0 := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "leaderworkerset-lws-pinned-evict-0-abcd1",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{{
						Kind: utils.KindLeaderWorkerSet,
						Name: "lws-pinned-evict",
					}},
				},
				Status: utils.WorkloadStatus{
					Conditions: []metav1.Condition{{
						Type:   utils.WorkloadAdmitted,
						Status: metav1.ConditionTrue,
					}},
				},
			}
			pinnedClient := fake.NewClientBuilder().
				WithScheme(sc).
				WithObjects(pinnedLeader, pinnedSlice0, pinnedWl0).
				Build()
			pinnedR := &StatefulSetReconciler{
				Client:               pinnedClient,
				Scheme:               sc,
				EnableKueueAdmission: true,
			}
			pinnedNN := types.NamespacedName{Namespace: "default", Name: "lws-pinned-evict"}

			// While group 0 is admitted and < partition, its Slice stays retained.
			_, err = pinnedR.Reconcile(ctx, reconcile.Request{NamespacedName: pinnedNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(pinnedClient.Get(ctx, types.NamespacedName{Name: "lws-pin-ev-0"}, &slicev1beta1.Slice{})).To(Succeed())

			// Once Kueue evicts group 0, releaseOwnedSlices immediately deletes lws-pin-ev-0.
			pinnedWl0.Status.Conditions = append(pinnedWl0.Status.Conditions, metav1.Condition{
				Type:   utils.WorkloadEvicted,
				Status: metav1.ConditionTrue,
			})
			Expect(pinnedClient.Update(ctx, pinnedWl0)).To(Succeed())
			_, err = pinnedR.Reconcile(ctx, reconcile.Request{NamespacedName: pinnedNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(errors.IsNotFound(pinnedClient.Get(ctx, types.NamespacedName{Name: "lws-pin-ev-0"}, &slicev1beta1.Slice{}))).To(BeTrue())
		})

	})
})

// TODO: Add tests for:
// - Concurrent reconciliation (race conditions)
// - Rapid create/delete cycles (finalizer race window)
// - AlreadyExists error from createSliceCR
// - Controller restart during Slice deletion (safety requeue)
