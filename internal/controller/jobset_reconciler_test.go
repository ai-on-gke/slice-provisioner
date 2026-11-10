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

	"github.com/ai-on-gke/slice-provisioner/internal/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// Test priority values representing the integer value from a PriorityClass.
// These are string representations because priorityValue() returns strconv.Itoa(priorityClass.Value).
const (
	testLowPriority  = "100" // Low-priority workload (e.g. batch)
	testHighPriority = "200" // High-priority workload (e.g. serving)
)

var _ = Describe("JobSet Controller", func() {
	Context("When reconciling a Job managed by JobSet", func() {
		var jobSetName string
		var jobName string
		var sliceName string
		var jobNamespacedName types.NamespacedName
		var jobSetNamespacedName types.NamespacedName
		namespace := "default"
		ctx := context.Background()

		BeforeEach(func() {
			uniqueID := strconv.FormatInt(time.Now().UnixNano(), 10)
			jobSetName = "js-" + uniqueID
			jobName = jobSetName + "-worker-0"
			sliceName = "slice-" + uniqueID
			jobNamespacedName = types.NamespacedName{Name: jobName, Namespace: namespace}
			jobSetNamespacedName = types.NamespacedName{Name: jobSetName, Namespace: namespace}

			// Create JobSet
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobSetName,
					Namespace: namespace,
					Labels:    map[string]string{slicePrefixLabel: "test"},
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					Suspend: ptrTo(true),
				},
			}
			Expect(k8sClient.Create(ctx, js)).To(Succeed())

			// Create Child Job
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						jobIndexLabel:          "0",
						replicatedJobNameLabel: "worker",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
						sliceNameLabel:         sliceName, // Simulating webhook
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:  "test",
									Image: "test",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
										},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())
		})

		AfterEach(func() {
			js := &jobsetv1alpha2.JobSet{}
			if err := k8sClient.Get(ctx, jobSetNamespacedName, js); err == nil {
				Expect(k8sClient.Delete(ctx, js)).To(Succeed())
			}

			jobList := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobList, client.InNamespace(namespace))).To(Succeed())
			for _, job := range jobList.Items {
				job.Finalizers = nil
				_ = k8sClient.Update(ctx, &job)
				_ = k8sClient.Delete(ctx, &job)
			}

			sliceList := &slicev1beta1.SliceList{}
			Expect(k8sClient.List(ctx, sliceList)).To(Succeed())
			for _, slice := range sliceList.Items {
				_ = k8sClient.Delete(ctx, &slice)
			}
		})

		It("should provision slices and handle lifecycle", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// First reconcile: create slices, add finalizer
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			slice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)).To(Succeed())
			Expect(slice.Annotations[managedByAnnotation]).To(Equal(sliceSchedulerName))
			Expect(slice.Annotations[retryOnFailureAnnotation]).To(Equal("true"))

			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).To(ContainElement(sliceProvisionerFinalizer))

			// Second reconcile: nothing to do (already provisioned)
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Mark slice as Ready (simulating external scheduler)
			slice.Status.Conditions = []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "READY",
					Message:            "Slice is ready",
				},
			}
			Expect(k8sClient.Status().Update(ctx, slice)).To(Succeed())

			// Third reconcile: should just return success
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should cleanup slice when Job is deleted", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Provision first
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			slice := &slicev1beta1.Slice{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, slice)).To(Succeed())

			// Delete Job
			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(k8sClient.Delete(ctx, updatedJob)).To(Succeed())

			// Reconcile deletion - loop until the finalizer is gone or the Job is deleted.
			Eventually(func() bool {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
				if err != nil {
					return false
				}

				err = k8sClient.Get(ctx, jobNamespacedName, updatedJob)
				if apierrors.IsNotFound(err) {
					return true
				}
				// Check if our finalizer is gone
				for _, f := range updatedJob.Finalizers {
					if f == sliceProvisionerFinalizer {
						return false
					}
				}
				return true
			}, time.Second*15, time.Second).Should(BeTrue(), "Job finalizer should be removed after reconciliation")
		})
		DescribeTable("should delete the slice once the Job finishes and not recreate it",
			func(conditions ...batchv1.JobConditionType) {
				reconciler := &JobSetReconciler{
					Client: k8sClient,
					Scheme: k8sClient.Scheme(),
				}

				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, &slicev1beta1.Slice{})).To(Succeed())

				job := &batchv1.Job{}
				Expect(k8sClient.Get(ctx, jobNamespacedName, job)).To(Succeed())
				now := metav1.Now()
				job.Status.StartTime = &now
				for _, t := range conditions {
					job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
						Type:               t,
						Status:             corev1.ConditionTrue,
						LastProbeTime:      now,
						LastTransitionTime: now,
					})
					if t == batchv1.JobComplete {
						job.Status.CompletionTime = &now
					}
				}
				Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

				_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
				Expect(err).NotTo(HaveOccurred())
				err = k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, &slicev1beta1.Slice{})
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "Slice should be deleted for a finished Job")

				// The Slice delete event re-enqueues the Job; that reconcile
				// must not provision a new Slice.
				_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
				Expect(err).NotTo(HaveOccurred())
				err = k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, &slicev1beta1.Slice{})
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "Slice should not be recreated for a finished Job")
			},
			Entry("Complete", batchv1.JobSuccessCriteriaMet, batchv1.JobComplete),
			Entry("Failed", batchv1.JobFailureTarget, batchv1.JobFailed),
		)

		It("should test workloadToJobs and sliceToJob map funcs", func() {
			r := &JobSetReconciler{Client: k8sClient}
			wl := &utils.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "wl-js",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "JobSet", Name: "js-owner"},
						{Kind: "Job", Name: "job-owner"},
					},
				},
			}
			reqs := r.workloadToJobs(ctx, wl)
			Expect(reqs).To(HaveLen(1))
			Expect(r.workloadToJobs(ctx, &corev1.Pod{})).To(BeNil())
			Expect(r.sliceToJob(ctx, &corev1.Pod{})).To(BeNil())
		})

	})

	Context("shouldHandleJob predicate (Fix #20)", func() {
		var reconciler *JobSetReconciler

		BeforeEach(func() {
			reconciler = &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
		})

		It("should return true when finalizer present but labels missing", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "job-finalizer-only",
					Namespace:  "default",
					Finalizers: []string{sliceProvisionerFinalizer},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
							}},
						},
					},
				},
			}
			Expect(reconciler.shouldHandleJob(job)).To(BeTrue())
		})

		It("should return true when labels and TPU present but no finalizer", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-labels-only",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(reconciler.shouldHandleJob(job)).To(BeTrue())
		})

		It("should return false when neither finalizer nor labels present", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-nothing",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
							}},
						},
					},
				},
			}
			Expect(reconciler.shouldHandleJob(job)).To(BeFalse())
		})

		It("should return false when labels present but no TPU resources and no finalizer", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-no-tpu",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
							}},
						},
					},
				},
			}
			Expect(reconciler.shouldHandleJob(job)).To(BeFalse())
		})
	})

	Context("Label removal finalizer cleanup (Fix #20)", func() {
		var jobName string
		var jobSetName string
		var sliceName string
		var jobNamespacedName types.NamespacedName
		namespace := "default"
		ctx := context.Background()

		BeforeEach(func() {
			uniqueID := strconv.FormatInt(time.Now().UnixNano(), 10)
			jobSetName = "js-lr-" + uniqueID
			jobName = jobSetName + "-worker-0"
			sliceName = "slice-lr-" + uniqueID
			jobNamespacedName = types.NamespacedName{Name: jobName, Namespace: namespace}

			// Create parent JobSet
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobSetName,
					Namespace: namespace,
					Labels:    map[string]string{slicePrefixLabel: "test"},
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					Suspend: ptrTo(true),
				},
			}
			Expect(k8sClient.Create(ctx, js)).To(Succeed())
		})

		AfterEach(func() {
			js := &jobsetv1alpha2.JobSet{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: jobSetName, Namespace: namespace}, js); err == nil {
				Expect(k8sClient.Delete(ctx, js)).To(Succeed())
			}
			jobList := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobList, client.InNamespace(namespace))).To(Succeed())
			for _, job := range jobList.Items {
				job.Finalizers = nil
				_ = k8sClient.Update(ctx, &job)
				_ = k8sClient.Delete(ctx, &job)
			}
			sliceList := &slicev1beta1.SliceList{}
			Expect(k8sClient.List(ctx, sliceList)).To(Succeed())
			for _, slice := range sliceList.Items {
				_ = k8sClient.Delete(ctx, &slice)
			}
		})

		It("should remove finalizer when Job has finalizer but slice labels are removed", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Create a Job with all required labels
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						jobIndexLabel:          "0",
						replicatedJobNameLabel: "worker",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
						sliceNameLabel:         sliceName,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			// First reconcile: adds finalizer + creates Slice
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Verify finalizer was added
			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).To(ContainElement(sliceProvisionerFinalizer))

			// Remove the slice-name label
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
				delete(updatedJob.Labels, sliceNameLabel)
				return k8sClient.Update(ctx, updatedJob)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again: guard should remove finalizer
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Verify finalizer was removed
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).NotTo(ContainElement(sliceProvisionerFinalizer))
		})

		It("should remove finalizer when Job has finalizer but jobset-name label is removed", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Create Job with TPU resources
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						jobIndexLabel:          "0",
						replicatedJobNameLabel: "worker",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
						sliceNameLabel:         sliceName,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			// First reconcile: adds finalizer
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).To(ContainElement(sliceProvisionerFinalizer))

			// Remove TPU resources (can't mutate immutable field in real K8s, but
			// we can remove the jobset-name label to trigger the same guard path)
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
				delete(updatedJob.Labels, jobSetNameLabel)
				return k8sClient.Update(ctx, updatedJob)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile again: guard removes finalizer
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).NotTo(ContainElement(sliceProvisionerFinalizer))
		})

		It("should remove finalizer when Job is not managed by JobSet", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Create a Job with all labels
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						jobIndexLabel:          "0",
						replicatedJobNameLabel: "worker",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
						sliceNameLabel:         sliceName,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			// First reconcile: adds finalizer
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).To(ContainElement(sliceProvisionerFinalizer))

			// Remove the jobset-name label (making it no longer "managed by JobSet")
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
				delete(updatedJob.Labels, jobSetNameLabel)
				return k8sClient.Update(ctx, updatedJob)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile: guard should remove finalizer
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).NotTo(ContainElement(sliceProvisionerFinalizer))
		})

		It("should allow clean deletion after labels are removed (not stuck in Terminating)", func() {
			reconciler := &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Create Job with all required labels
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						jobIndexLabel:          "0",
						replicatedJobNameLabel: "worker",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
						sliceNameLabel:         sliceName,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			// First reconcile: adds finalizer + creates Slice
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updatedJob := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).To(ContainElement(sliceProvisionerFinalizer))

			// Remove labels
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
				delete(updatedJob.Labels, sliceNameLabel)
				delete(updatedJob.Labels, sliceTypeLabel)
				delete(updatedJob.Labels, sliceTopologyLabel)
				return k8sClient.Update(ctx, updatedJob)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())

			// Reconcile: guard removes finalizer
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.Finalizers).NotTo(ContainElement(sliceProvisionerFinalizer))

			// Now delete the Job — should succeed without getting stuck.
			// In envtest there is no GC controller to fully remove the object,
			// so we verify the delete succeeds and DeletionTimestamp is set.
			Expect(k8sClient.Delete(ctx, updatedJob)).To(Succeed())

			Expect(k8sClient.Get(ctx, jobNamespacedName, updatedJob)).To(Succeed())
			Expect(updatedJob.DeletionTimestamp.IsZero()).To(BeFalse(),
				"Job should have DeletionTimestamp set (not stuck due to missing finalizer)")
		})
	})

	Context("requestsTPU with init containers (#10)", func() {
		It("should return true when TPU is in init container only", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-init-tpu",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "main",
								Image: "test",
							}},
							InitContainers: []corev1.Container{{
								Name:  "init",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(requestsTPU(job)).To(BeTrue())
		})

		It("should return true when TPU is in regular container", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-regular-tpu",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "main",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(requestsTPU(job)).To(BeTrue())
		})

		It("should return false when no TPU in any container", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-no-tpu",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "main",
								Image: "test",
							}},
							InitContainers: []corev1.Container{{
								Name:  "init",
								Image: "test",
							}},
						},
					},
				},
			}
			Expect(requestsTPU(job)).To(BeFalse())
		})

		It("should return true when TPU is in init container Requests (not Limits)", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-init-tpu-requests",
					Namespace: "default",
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "main",
								Image: "test",
							}},
							InitContainers: []corev1.Container{{
								Name:  "init",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(requestsTPU(job)).To(BeTrue())
		})
	})

	Context("shouldReconcileUpdate with priority changes", func() {
		var reconciler *JobSetReconciler

		BeforeEach(func() {
			reconciler = &JobSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
		})

		It("should return true when PriorityClassName changes", func() {
			oldJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-old-priority",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy:     corev1.RestartPolicyNever,
							PriorityClassName: "low-priority",
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			newJob := oldJob.DeepCopy()
			newJob.Spec.Template.Spec.PriorityClassName = "high-priority"
			Expect(reconciler.shouldReconcileUpdate(oldJob, newJob)).To(BeTrue())
		})

		It("should return false when only the priority annotation changes", func() {
			// priorityValue() reads PriorityClassName, not the annotation.
			// Annotation-only changes should NOT trigger a reconciliation
			// since it would result in a no-op update.
			oldJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-annotation-only",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
					Annotations: map[string]string{
						slicePriorityAnnotation: testLowPriority,
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			newJob := oldJob.DeepCopy()
			newJob.Annotations = map[string]string{
				slicePriorityAnnotation: testHighPriority,
			}
			Expect(reconciler.shouldReconcileUpdate(oldJob, newJob)).To(BeFalse())
		})

		It("should return true only when the Job becomes finished", func() {
			oldJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-finishes",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			targetJob := oldJob.DeepCopy()
			targetJob.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue}}
			Expect(reconciler.shouldReconcileUpdate(oldJob, targetJob)).To(BeFalse(), "SuccessCriteriaMet alone is not finished")

			finishedJob := targetJob.DeepCopy()
			finishedJob.Status.Conditions = append(finishedJob.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
			Expect(reconciler.shouldReconcileUpdate(targetJob, finishedJob)).To(BeTrue())
			Expect(reconciler.shouldReconcileUpdate(finishedJob, finishedJob.DeepCopy())).To(BeFalse())
		})

		It("should return false when nothing changes", func() {
			oldJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-no-change",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel: "my-jobset",
						sliceNameLabel:  "my-slice",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			newJob := oldJob.DeepCopy()
			Expect(reconciler.shouldReconcileUpdate(oldJob, newJob)).To(BeFalse())
		})
	})

	Context("Cross-namespace Slice ownership isolation", func() {
		ctx := context.Background()

		It("never deletes or mutates a Slice owned by another namespace or Kind across Job lifecycle", func() {
			otherNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "jobset-other-ns"}}
			if err := k8sClient.Create(ctx, otherNS); err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			sliceName := "shared-jobset-slice"
			foreignSlice := &slicev1beta1.Slice{
				ObjectMeta: metav1.ObjectMeta{
					Name: sliceName,
					Labels: map[string]string{
						sliceOwnerNameLabel:      "foreign-job-0",
						sliceOwnerNamespaceLabel: "default",
						sliceOwnerKindLabel:      utils.KindJob,
					},
					Annotations: map[string]string{
						managedByAnnotation:     sliceSchedulerName,
						slicePriorityAnnotation: testLowPriority,
					},
				},
				Spec: slicev1beta1.SliceSpec{
					Type:         "v6e",
					Topology:     "2x2",
					PartitionIds: []string{},
				},
			}
			Expect(k8sClient.Create(ctx, foreignSlice)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, foreignSlice) })

			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "colliding-js",
					Namespace: "jobset-other-ns",
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					Suspend: ptrTo(true),
				},
			}
			Expect(k8sClient.Create(ctx, js)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, js) })

			jobNN := types.NamespacedName{Namespace: "jobset-other-ns", Name: "foreign-job-0"}
			collidingJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobNN.Name,
					Namespace: jobNN.Namespace,
					Labels: map[string]string{
						jobSetNameLabel:    js.Name,
						sliceNameLabel:     sliceName,
						sliceTypeLabel:     "tpu7x",
						sliceTopologyLabel: "2x2x2",
					},
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:  "c",
								Image: "busybox",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										tpuResourceName: *resource.NewQuantity(4, resource.DecimalSI),
									},
								},
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, collidingJob)).To(Succeed())
			DeferCleanup(func() {
				j := &batchv1.Job{}
				if err := k8sClient.Get(ctx, jobNN, j); err == nil {
					j.Finalizers = nil
					_ = k8sClient.Update(ctx, j)
					_ = k8sClient.Delete(ctx, j)
				}
			})

			r := &JobSetReconciler{
				Client:               k8sClient,
				Scheme:               k8sClient.Scheme(),
				EnableKueueAdmission: true,
			}

			// 1. Suspended Kueue JobSet must NOT delete default/foreign-job-0's Slice.
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: jobNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())

			// 2. Unsuspended Job with mismatched topology must fail with SliceOwnerConflict instead of deleting foreignSlice.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: js.Namespace, Name: js.Name}, js)).To(Succeed())
			js.Spec.Suspend = ptrTo(false)
			Expect(k8sClient.Update(ctx, js)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: jobNN})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already owned by Job default/foreign-job-0"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())
			Expect(string(foreignSlice.Spec.Type)).To(Equal("v6e"))

			// 3. Finished colliding Job must NOT delete foreignSlice.
			Expect(k8sClient.Get(ctx, jobNN, collidingJob)).To(Succeed())
			now := metav1.Now()
			collidingJob.Status.StartTime = &now
			collidingJob.Status.CompletionTime = &now
			collidingJob.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
			}
			Expect(k8sClient.Status().Update(ctx, collidingJob)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: jobNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())

			// 4. Deleting colliding Job removes its own finalizer while leaving foreignSlice intact.
			Expect(k8sClient.Get(ctx, jobNN, collidingJob)).To(Succeed())
			Expect(k8sClient.Delete(ctx, collidingJob)).To(Succeed())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: jobNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sliceName}, foreignSlice)).To(Succeed())
			Expect(foreignSlice.DeletionTimestamp.IsZero()).To(BeTrue())
		})
	})
})

func ptrTo[T any](v T) *T {
	return &v
}
