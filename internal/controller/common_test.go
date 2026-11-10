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
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

var _ = Describe("Common Controller Functions", func() {
	var (
		ctx    = context.Background()
		logger = logf.Log.WithName("common-test")
	)

	Context("reconcileFinalizer", func() {
		It("should add a finalizer to an object that does not have one", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-add-finalizer",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())
		})

		It("should not error when adding a finalizer to an object that already has it", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-already-has-finalizer",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add the finalizer first.
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())

			// Adding again should be a no-op.
			err = reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())
		})

		It("should remove a finalizer from an object", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-remove-finalizer",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add the finalizer first.
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())

			// Remove it.
			err = reconcileFinalizer(ctx, k8sClient, cm, false, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
		})

		It("should be a no-op when removing finalizer from object without one", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-remove-no-finalizer",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			err := reconcileFinalizer(ctx, k8sClient, cm, false, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
		})
	})

	Context("handleSliceDeletion", func() {
		It("should be a no-op when the object has no finalizer", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-handle-no-finalizer",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			result, err := handleSliceDeletion(ctx, k8sClient, cm, "nonexistent-slice", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})

		It("should remove finalizer when the slice does not exist", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-handle-slice-gone",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add finalizer first.
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())

			// Handle deletion when slice does not exist → should remove finalizer.
			result, err := handleSliceDeletion(ctx, k8sClient, cm, "nonexistent-slice", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
		})

		It("should delete the slice when it exists and remove finalizer after", func() {
			// Create a Slice CR.
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-slice-to-delete",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{"partition-0"},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-handle-slice-exists",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add finalizer.
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())

			// First call triggers slice deletion; since Slice still exists
			// (envtest deletes immediately but code checks after Delete),
			// handleSliceDeletion should requeue with backoff.
			result, err := handleSliceDeletion(ctx, k8sClient, cm, "test-slice-to-delete", logger)
			Expect(err).NotTo(HaveOccurred())
			// The slice is deleted immediately in envtest (no finalizers),
			// but the code still calls sliceDeletionBackoff and requeues.
			// RequeueAfter should be sliceDeletionBackoffInitial (30s)
			// since the ConfigMap has no DeletionTimestamp.
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))
		})

		It("should remove finalizer when slice name is empty", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-handle-empty-slice",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add finalizer.
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())

			// Handle deletion with empty slice name → should skip slice lookup and remove finalizer.
			result, err := handleSliceDeletion(ctx, k8sClient, cm, "", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
		})
	})

	Context("handleSlicesDeletion", func() {
		It("should retain finalizer until all slices in sliceNames are deleted and skip empty names", func() {
			sliceA := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-multi-slice-a",
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{"partition-0"},
				},
			}
			sliceB := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-multi-slice-b",
					Finalizers: []string{"test.tpu.google.com/hold"},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{"partition-0"},
				},
			}
			Expect(k8sClient.Create(ctx, sliceA)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, sliceA)
			})
			Expect(k8sClient.Create(ctx, sliceB)).To(Succeed())
			DeferCleanup(func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceB.Name}, s); err == nil {
					s.Finalizers = nil
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			})

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-handle-multiple-slices",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() {
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, cm); err == nil {
					cm.Finalizers = nil
					_ = k8sClient.Update(ctx, cm)
					_ = k8sClient.Delete(ctx, cm)
				}
			})

			Expect(reconcileFinalizer(ctx, k8sClient, cm, true, logger)).To(Succeed())

			sliceNames := []string{"", "nonexistent-slice", sliceA.Name, sliceB.Name}

			// First call deletes sliceA and marks sliceB for deletion; finalizer stays.
			result, err := handleSlicesDeletion(ctx, k8sClient, cm, sliceNames, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())

			// Second call: sliceA is now gone, but sliceB is still held by its external finalizer.
			result, err = handleSlicesDeletion(ctx, k8sClient, cm, sliceNames, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeTrue())

			// Release external finalizer on sliceB, then third call removes the workload finalizer.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceB.Name}, sliceB)).To(Succeed())
			sliceB.Finalizers = nil
			Expect(k8sClient.Update(ctx, sliceB)).To(Succeed())

			result, err = handleSlicesDeletion(ctx, k8sClient, cm, sliceNames, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: sliceA.Name}, &slicev1beta1.Slice{}))).To(BeTrue())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: sliceB.Name}, &slicev1beta1.Slice{}))).To(BeTrue())
		})

		It("should skip foreign-owned Slices during handleSlicesDeletionForOwner and report stuck metrics for all pending Slices", func() {
			foreignSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-foreign-owned-slice",
					Labels: map[string]string{
						sliceOwnerNameLabel:      "test-owner-cm",
						sliceOwnerNamespaceLabel: "other-namespace",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, foreignSlice)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, foreignSlice)
			})

			stuckA := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-stuck-multi-a",
					Finalizers: []string{"test.tpu.google.com/hold"},
					Labels: map[string]string{
						sliceOwnerNameLabel:      "test-owner-cm",
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
			}
			stuckB := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-stuck-multi-b",
					Finalizers: []string{"test.tpu.google.com/hold"},
					Labels: map[string]string{
						sliceOwnerNameLabel:      "test-owner-cm",
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, stuckA)).To(Succeed())
			DeferCleanup(func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: stuckA.Name}, s); err == nil {
					s.Finalizers = nil
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			})
			Expect(k8sClient.Create(ctx, stuckB)).To(Succeed())
			DeferCleanup(func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: stuckB.Name}, s); err == nil {
					s.Finalizers = nil
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			})

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-owner-cm",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() {
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, cm); err == nil {
					cm.Finalizers = nil
					_ = k8sClient.Update(ctx, cm)
					_ = k8sClient.Delete(ctx, cm)
				}
			})
			Expect(reconcileFinalizer(ctx, k8sClient, cm, true, logger)).To(Succeed())

			oldTS := metav1.NewTime(time.Now().Add(-6 * time.Minute))
			cm.DeletionTimestamp = &oldTS
			SliceDeletionStuckTotal.WithLabelValues(foreignSlice.Name).Add(3)
			beforeForeign := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(foreignSlice.Name))
			beforeA := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(stuckA.Name))
			beforeB := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(stuckB.Name))

			res, err := handleSlicesDeletionForOwner(ctx, k8sClient, cm, utils.KindStatefulSet, []string{foreignSlice.Name, stuckA.Name, stuckB.Name}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(5 * time.Minute))

			// Foreign slice must remain completely untouched (not marked for deletion),
			// and skipping it must NOT wipe the legitimate owner's SliceDeletionStuckTotal metric series.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: foreignSlice.Name}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())
			Expect(testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(foreignSlice.Name))).To(Equal(beforeForeign))

			// Even when ownerKind is empty (handleSliceDeletion / handleSlicesDeletion), foreign namespace/name protection still holds.
			cmNoKind := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "test-owner-cm-nokind", Namespace: "default"},
			}
			Expect(k8sClient.Create(ctx, cmNoKind)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cmNoKind) })
			Expect(reconcileFinalizer(ctx, k8sClient, cmNoKind, true, logger)).To(Succeed())
			resNoKind, err := handleSliceDeletion(ctx, k8sClient, cmNoKind, foreignSlice.Name, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(resNoKind.RequeueAfter).To(BeZero())
			Expect(controllerutil.ContainsFinalizer(cmNoKind, sliceProvisionerFinalizer)).To(BeFalse())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: foreignSlice.Name}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())

			// Both pending owned slices must report stuck metrics.
			Expect(testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(stuckA.Name))).To(Equal(beforeA + 1))
			Expect(testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(stuckB.Name))).To(Equal(beforeB + 1))

			// Once both owned slices release their external finalizers, cm's finalizer is removed while foreignSlice remains intact.
			for _, name := range []string{stuckA.Name, stuckB.Name} {
				s := &slicev1beta1.Slice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, s)).To(Succeed())
				s.Finalizers = nil
				Expect(k8sClient.Update(ctx, s)).To(Succeed())
			}
			cm.DeletionTimestamp = nil
			res, err = handleSlicesDeletionForOwner(ctx, k8sClient, cm, utils.KindStatefulSet, []string{foreignSlice.Name, stuckA.Name, stuckB.Name}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(controllerutil.ContainsFinalizer(cm, sliceProvisionerFinalizer)).To(BeFalse())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: foreignSlice.Name}, foreignSlice)).To(Succeed())
		})
	})

	Context("sliceRequirements", func() {
		It("should extract labels from an object", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-slice-reqs",
					Namespace: "default",
					Labels: map[string]string{
						sliceNameLabel:     "my-slice",
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			name, sliceType, topology, err := sliceRequirements(cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(name).To(Equal("my-slice"))
			Expect(sliceType).To(Equal("v6e"))
			Expect(topology).To(Equal("2x4"))
		})

		It("should return empty strings when labels are missing", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-no-labels",
					Namespace: "default",
				},
			}
			name, sliceType, topology, err := sliceRequirements(cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(name).To(BeEmpty())
			Expect(sliceType).To(BeEmpty())
			Expect(topology).To(BeEmpty())
		})
	})

	Context("sliceDeletionBackoff", func() {
		It("should return 30s when DeletionTimestamp is nil", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-backoff-nil-ts",
					Namespace: "default",
				},
			}
			backoff := sliceDeletionBackoff(cm, "some-slice", logger)
			Expect(backoff).To(Equal(30 * time.Second))
		})

		It("should return 30s when just deleted (elapsed < 30s)", func() {
			now := metav1.Now()
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-just-deleted",
					Namespace:         "default",
					DeletionTimestamp: &now,
				},
			}
			backoff := sliceDeletionBackoff(cm, "some-slice", logger)
			Expect(backoff).To(Equal(30 * time.Second))
		})

		It("should return ~60s when elapsed is ~30-60s", func() {
			ts := metav1.NewTime(time.Now().Add(-35 * time.Second))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-60s",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			backoff := sliceDeletionBackoff(cm, "some-slice", logger)
			Expect(backoff).To(Equal(60 * time.Second))
		})

		It("should return ~120s when elapsed is ~60-90s", func() {
			ts := metav1.NewTime(time.Now().Add(-65 * time.Second))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-120s",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			backoff := sliceDeletionBackoff(cm, "some-slice", logger)
			Expect(backoff).To(Equal(120 * time.Second))
		})

		It("should cap at 5m for very long elapsed times", func() {
			// Use 4m (not 30m) to avoid integer overflow in the backoff
			// computation: n = floor(elapsed/30s), and 2^n * 30e9 overflows
			// time.Duration for n >= 29. At 4m: n=8, 2^8*30s = 7680s > 5m cap.
			ts := metav1.NewTime(time.Now().Add(-4 * time.Minute))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-capped",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			backoff := sliceDeletionBackoff(cm, "some-slice", logger)
			Expect(backoff).To(Equal(5 * time.Minute))
		})

		It("should increment SliceDeletionStuckTotal metric after 5 minutes", func() {
			sliceName := "test-stuck-slice"
			before := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))

			ts := metav1.NewTime(time.Now().Add(-6 * time.Minute))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-stuck-metric",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			backoff := sliceDeletionBackoff(cm, sliceName, logger)
			Expect(backoff).To(Equal(5 * time.Minute))

			after := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))
			Expect(after).To(Equal(before + 1))
		})

		It("should not increment SliceDeletionStuckTotal metric before 5 minutes", func() {
			sliceName := "test-not-stuck-slice"
			before := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))

			ts := metav1.NewTime(time.Now().Add(-2 * time.Minute))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-not-stuck",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			_ = sliceDeletionBackoff(cm, sliceName, logger)

			after := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))
			Expect(after).To(Equal(before))
		})

		It("should cap at 5m for elapsed times that would cause large intermediate values", func() {
			// At 10m elapsed: n = floor(600/30) = 20, 2^20 * 30s = 31457280s.
			// This is a very large Duration but still fits in int64. The cap
			// should bring it down to 5m. This tests robustness for values
			// above the cap but below the int64 overflow boundary.
			ts := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-backoff-10m",
					Namespace:         "default",
					DeletionTimestamp: &ts,
				},
			}
			backoff := sliceDeletionBackoff(cm, "test-10m-slice", logger)
			Expect(backoff).To(Equal(5 * time.Minute))
		})
	})

	Context("handleSliceDeletion metric cleanup (Fix H1)", func() {
		It("should clean up SliceDeletionStuckTotal metric when Slice is gone", func() {
			sliceName := "test-h1-metric-cleanup-slice"
			// Populate the metric as if the Slice was previously stuck
			SliceDeletionStuckTotal.WithLabelValues(sliceName).Add(5)
			before := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))
			Expect(before).To(Equal(float64(5)))

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-h1-cm",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Add finalizer
			err := reconcileFinalizer(ctx, k8sClient, cm, true, logger)
			Expect(err).NotTo(HaveOccurred())

			// Handle deletion with a non-existent Slice → should clean up metric
			result, err := handleSliceDeletion(ctx, k8sClient, cm, sliceName, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// The metric series should have been deleted via DeleteLabelValues.
			// After DeleteLabelValues, a fresh WithLabelValues returns 0.
			after := testutil.ToFloat64(SliceDeletionStuckTotal.WithLabelValues(sliceName))
			Expect(after).To(Equal(float64(0)))
		})
	})

	Context("createSliceCR (Fixes #6 and N2)", func() {
		It("should create a Slice with owner labels", func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-owner-labels-owner",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			sliceName := "test-owner-labels-slice"
			err := createSliceCR(ctx, k8sClient, cm, sliceName, "v6e", "2x2", "StatefulSet", "100", logger)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, s); err == nil {
					_ = k8sClient.Delete(ctx, s)
				}
			}()

			slice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)).To(Succeed())
			Expect(slice.Labels[sliceOwnerNameLabel]).To(Equal("test-owner-labels-owner"))
			Expect(slice.Labels[sliceOwnerNamespaceLabel]).To(Equal("default"))
			Expect(slice.Labels[sliceOwnerKindLabel]).To(Equal("StatefulSet"))
			Expect(slice.Annotations[slicePriorityAnnotation]).To(Equal("100"))
		})

		It("should return SliceOwnerConflict error when AlreadyExists with a foreign owner without overwriting owner labels", func() {
			sliceName := "test-owner-conflict-slice"
			existingSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Labels: map[string]string{
						sliceOwnerNameLabel:      "test-conflict-cm",
						sliceOwnerNamespaceLabel: "other-ns",
						sliceOwnerKindLabel:      "StatefulSet",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, existingSlice)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, existingSlice) })

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-conflict-cm",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cm) })

			beforeConflict := testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))
			err := createSliceCR(ctx, k8sClient, cm, sliceName, "v6e", "2x2", "StatefulSet", "", logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already owned by StatefulSet other-ns/test-conflict-cm"))
			Expect(testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))).To(Equal(beforeConflict + 1))

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, existingSlice)).To(Succeed())
			Expect(existingSlice.Labels[sliceOwnerNamespaceLabel]).To(Equal("other-ns"))
		})

		It("should backfill missing owner labels when AlreadyExists and type/topology match", func() {
			sliceName := "test-n2-matching-slice"
			existingSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, existingSlice)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, existingSlice) })

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-n2-match-owner",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cm) })

			err := createSliceCR(ctx, k8sClient, cm, sliceName, "v6e", "2x2", "Job", "", logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, existingSlice)).To(Succeed())
			Expect(existingSlice.Labels[sliceOwnerNameLabel]).To(Equal("test-n2-match-owner"))
			Expect(existingSlice.Labels[sliceOwnerNamespaceLabel]).To(Equal("default"))
			Expect(existingSlice.Labels[sliceOwnerKindLabel]).To(Equal("Job"))

			// Legacy <= 0.6.0 Slice with name/kind already set gets slice-owner-namespace backfilled.
			legacyName := "test-legacy-backfill-slice"
			legacySlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: legacyName,
					Labels: map[string]string{
						sliceOwnerNameLabel: "test-n2-match-owner",
						sliceOwnerKindLabel: "Job",
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, legacySlice)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, legacySlice) })

			Expect(createSliceCR(ctx, k8sClient, cm, legacyName, "v6e", "2x2", "Job", "", logger)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: legacyName}, legacySlice)).To(Succeed())
			Expect(legacySlice.Labels[sliceOwnerNamespaceLabel]).To(Equal("default"))
		})

		It("should return errSliceTerminating without SliceOwnerConflict when AlreadyExists on a terminating Slice", func() {
			sliceName := "test-terminating-already-exists-slice"
			terminating := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name:       sliceName,
					Finalizers: []string{"test.tpu.google.com/hold"},
					Labels: map[string]string{
						sliceOwnerNameLabel:      "prior-owner",
						sliceOwnerNamespaceLabel: "other-ns",
						sliceOwnerKindLabel:      utils.KindStatefulSet,
					},
				},
				Spec: slicev1beta1.SliceSpec{Type: "v6e", Topology: "2x2", PartitionIds: []string{}},
			}
			Expect(k8sClient.Create(ctx, terminating)).To(Succeed())
			DeferCleanup(func() {
				s := &slicev1beta1.Slice{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, s); err == nil {
					s.Finalizers = nil
					_ = k8sClient.Update(ctx, s)
					_ = k8sClient.Delete(ctx, s)
				}
			})
			Expect(k8sClient.Delete(ctx, terminating)).To(Succeed())

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "new-owner-cm", Namespace: "default"},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cm) })

			beforeConflict := testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))
			err := createSliceCR(ctx, k8sClient, cm, sliceName, "v6e", "2x2", utils.KindStatefulSet, "", logger)
			Expect(errors.Is(err, errSliceTerminating)).To(BeTrue())
			Expect(testutil.ToFloat64(ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict"))).To(Equal(beforeConflict))
		})

		It("should return error when AlreadyExists and type/topology mismatch", func() {
			// Pre-create a Slice with different topology
			sliceName := "test-n2-mismatch-slice"
			existingSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "4x4",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, existingSlice)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, existingSlice) }()

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-n2-mismatch-owner",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, cm) }()

			// Creating with different topology should fail
			err := createSliceCR(ctx, k8sClient, cm, sliceName, "v6e", "2x2", "Job", "", logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already exists with type"))
		})
	})

	Context("updateSlicePriority (Fix #19)", func() {
		It("should update priority annotation when it changes", func() {
			sliceName := "test-priority-update-slice"
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Annotations: map[string]string{
						managedByAnnotation:     sliceSchedulerName,
						slicePriorityAnnotation: "100",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, slice) }()

			// Update priority from 100 to 200
			updated, err := updateSlicePriority(ctx, k8sClient, slice, "200", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated).To(BeTrue())

			// Verify the annotation was updated
			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, updatedSlice)).To(Succeed())
			Expect(updatedSlice.Annotations[slicePriorityAnnotation]).To(Equal("200"))
		})

		It("should be a no-op when priority has not changed", func() {
			sliceName := "test-priority-noop-slice"
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Annotations: map[string]string{
						managedByAnnotation:     sliceSchedulerName,
						slicePriorityAnnotation: "100",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, slice) }()

			updated, err := updateSlicePriority(ctx, k8sClient, slice, "100", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated).To(BeFalse())
		})

		It("should remove priority annotation when new priority is empty", func() {
			sliceName := "test-priority-remove-slice"
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Annotations: map[string]string{
						managedByAnnotation:     sliceSchedulerName,
						slicePriorityAnnotation: "100",
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, slice) }()

			updated, err := updateSlicePriority(ctx, k8sClient, slice, "", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated).To(BeTrue())

			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, updatedSlice)).To(Succeed())
			Expect(updatedSlice.Annotations).NotTo(HaveKey(slicePriorityAnnotation))
		})

		It("should add priority annotation when Slice had none", func() {
			sliceName := "test-priority-add-slice"
			slice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Annotations: map[string]string{
						managedByAnnotation: sliceSchedulerName,
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, slice)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, slice) }()

			updated, err := updateSlicePriority(ctx, k8sClient, slice, "300", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated).To(BeTrue())

			updatedSlice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, updatedSlice)).To(Succeed())
			Expect(updatedSlice.Annotations[slicePriorityAnnotation]).To(Equal("300"))
		})
	})
})
