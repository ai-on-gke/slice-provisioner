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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

var _ = Describe("JobSet Validator", func() {
	var (
		validator *JobSetValidator
		ctx       = context.Background()
	)

	BeforeEach(func() {
		validator = &JobSetValidator{
			Client: k8sClient,
		}
	})

	Context("Validate JobSet", func() {
		It("should allow JobSet without slice-prefix label", func() {
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-prefix-js",
					Namespace: "default",
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name: "worker",
							Template: batchv1.JobTemplateSpec{
								Spec: batchv1.JobSpec{
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: "test",
													Resources: corev1.ResourceRequirements{
														Limits: corev1.ResourceList{
															corev1.ResourceName(tpuResourceName): resource.MustParse("4"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, js)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should allow JobSet with slice-prefix and required labels", func() {
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "valid-js",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "test",
					},
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name: "worker",
							Template: batchv1.JobTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										sliceTypeLabel:     "v6e",
										sliceTopologyLabel: "2x2",
									},
								},
								Spec: batchv1.JobSpec{
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: "test",
													Resources: corev1.ResourceRequirements{
														Limits: corev1.ResourceList{
															corev1.ResourceName(tpuResourceName): resource.MustParse("4"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, js)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny JobSet with slice-prefix but missing required labels when TPUs are requested", func() {
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-js",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "test",
					},
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name: "worker",
							Template: batchv1.JobTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										sliceTypeLabel: "v6e",
										// sliceTopologyLabel is missing
									},
								},
								Spec: batchv1.JobSpec{
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: "test",
													Resources: corev1.ResourceRequirements{
														Limits: corev1.ResourceList{
															corev1.ResourceName(tpuResourceName): resource.MustParse("4"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, js)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("requests TPUs but is missing required Job template labels"))
		})

		It("should allow JobSet with slice-prefix and missing labels if TPUs are NOT requested", func() {
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-tpu-valid-js",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "test",
					},
				},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name: "worker",
							Template: batchv1.JobTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										// labels missing but it's okay because no TPUs
									},
								},
								Spec: batchv1.JobSpec{
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: "test",
													// no TPU limit
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, js)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should deny JobSet if slice-prefix is already used by another JobSet", func() {
			prefix := "duplicate-prefix"
			// Create first JobSet
			js1 := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "js1",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: prefix,
					},
				},
				Spec: jobsetv1alpha2.JobSetSpec{},
			}
			Expect(k8sClient.Create(ctx, js1)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, js1)
			}()

			// Try to create second JobSet with same prefix
			js2 := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "js2",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: prefix,
					},
				},
				Spec: jobsetv1alpha2.JobSetSpec{},
			}
			_, err := validator.ValidateCreate(ctx, js2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("slice-prefix %q is already used", prefix)))
		})
	})

	Context("ValidateUpdate", func() {
		It("should reject update that changes slice-type label", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-type",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "my-prefix",
						sliceTypeLabel:   "v6e",
					},
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-type",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "my-prefix",
						sliceTypeLabel:   "tpu7x",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
			Expect(err.Error()).To(ContainSubstring(sliceTypeLabel))
		})

		It("should reject update that changes slice-topology label", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-topo",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "my-prefix",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-topo",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "my-prefix",
						sliceTopologyLabel: "4x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
			Expect(err.Error()).To(ContainSubstring(sliceTopologyLabel))
		})

		It("should reject update that changes slice-prefix label", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-prefix",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "old-prefix",
					},
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-prefix",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "new-prefix",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
			Expect(err.Error()).To(ContainSubstring(slicePrefixLabel))
		})

		It("should reject update that removes a previously set immutable label", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-remove",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "my-prefix",
						sliceTypeLabel:   "v6e",
					},
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-remove",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "my-prefix",
						// sliceTypeLabel removed
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable once set"))
		})

		It("should allow update when immutable labels are unchanged", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-same",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "my-prefix",
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-same",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "my-prefix",
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should allow update on non-opted-in JobSet (no TPU labels)", func() {
			oldJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-noop",
					Namespace: "default",
				},
			}
			newJS := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "update-js-noop",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel: "new-prefix",
					},
				},
			}
			_, err := validator.ValidateUpdate(ctx, oldJS, newJS)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("ValidateDelete", func() {
		It("should always allow deletion", func() {
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "delete-js",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "delete-prefix",
						sliceTypeLabel:     "v6e",
						sliceTopologyLabel: "2x4",
					},
				},
			}
			_, err := validator.ValidateDelete(ctx, js)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
