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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

var _ = Describe("JobSet Webhook", func() {
	Context("When creating a Job", func() {
		ctx := context.Background()
		jobSetName := "test-jobset"
		namespace := "default"

		It("should inject nodeSelector, labels, and annotations into Pod template", func() {
			// Create Parent JobSet
			jobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobSetName,
					Namespace: namespace,
					Labels: map[string]string{
						slicePrefixLabel: "test-prefix",
					},
				},
			}
			Expect(k8sClient.Create(ctx, jobSet)).To(Succeed())

			// Create Job
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        jobSetName,
						replicatedJobNameLabel: "replicated-job",
						jobIndexLabel:          "0",
						sliceTypeLabel:         "v6e",
						sliceTopologyLabel:     "2x2",
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
										corev1.ResourceName(tpuResourceName): {},
									},
								},
							}},
						},
					},
				},
			}

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			expectedSliceName := "test-prefix-replicated-job-0"
			Expect(job.Labels[sliceNameLabel]).To(Equal(expectedSliceName))
			Expect(job.Spec.Template.Labels[sliceNameLabel]).To(Equal(expectedSliceName))
			Expect(job.Spec.Template.Labels[sliceTypeLabel]).To(Equal("v6e"))
			Expect(job.Spec.Template.Labels[sliceTopologyLabel]).To(Equal("2x2"))
			Expect(job.Spec.Template.Annotations[sliceNameAnnotation]).To(Equal(expectedSliceName))
			Expect(job.Spec.Template.Spec.NodeSelector[sliceNameLabel]).To(Equal(expectedSliceName))
		})

		It("should not mutate a Job that is not part of a JobSet", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "standalone-job",
					Namespace: namespace,
					Labels:    map[string]string{},
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// No slice labels should have been injected.
			Expect(job.Labels[sliceNameLabel]).To(BeEmpty())
			Expect(job.Spec.Template.Spec.NodeSelector).To(BeNil())
		})

		It("automatically injects accelerator and topology into pod template nodeSelector when specified on parent JobSet", func() {
			jobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "auto-nodeselector-jobset",
					Namespace: "default",
					Labels: map[string]string{
						slicePrefixLabel:   "auto-prefix",
						sliceTypeLabel:     "tpu7x",
						sliceTopologyLabel: "4x4x4",
					},
				},
			}
			Expect(k8sClient.Create(ctx, jobSet)).To(Succeed())

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "auto-nodeselector-job",
					Namespace: "default",
					Labels: map[string]string{
						jobSetNameLabel:        "auto-nodeselector-jobset",
						replicatedJobNameLabel: "worker",
						jobIndexLabel:          "0",
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).ToNot(HaveOccurred())

			Expect(job.Spec.Template.Spec.NodeSelector).ToNot(BeNil())
			Expect(job.Spec.Template.Spec.NodeSelector[sliceTypeLabel]).To(Equal("tpu7x"))
			Expect(job.Spec.Template.Spec.NodeSelector[sliceTopologyLabel]).To(Equal("4x4x4"))
		})

		It("should not mutate when parent JobSet is missing slice-prefix label", func() {
			noPrefixJobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-prefix-jobset",
					Namespace: namespace,
					Labels:    map[string]string{},
				},
			}
			Expect(k8sClient.Create(ctx, noPrefixJobSet)).To(Succeed())

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-no-prefix",
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        "no-prefix-jobset",
						replicatedJobNameLabel: "worker",
						jobIndexLabel:          "0",
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			Expect(job.Labels[sliceNameLabel]).To(BeEmpty())
		})

		It("should not mutate when Job is missing required metadata labels", func() {
			metaJobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "meta-jobset",
					Namespace: namespace,
					Labels: map[string]string{
						slicePrefixLabel: "meta-prefix",
					},
				},
			}
			Expect(k8sClient.Create(ctx, metaJobSet)).To(Succeed())

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job-missing-meta",
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel: "meta-jobset",
						// missing replicatedJobNameLabel and jobIndexLabel
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			Expect(job.Labels[sliceNameLabel]).To(BeEmpty())
		})

		It("should inject nodeSelector targeting the correct slice", func() {
			nsJobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ns-jobset",
					Namespace: namespace,
					Labels: map[string]string{
						slicePrefixLabel: "ns-prefix",
					},
				},
			}
			Expect(k8sClient.Create(ctx, nsJobSet)).To(Succeed())

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ns-job",
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        "ns-jobset",
						replicatedJobNameLabel: "worker",
						jobIndexLabel:          "1",
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			expectedSliceName := "ns-prefix-worker-1"
			Expect(job.Spec.Template.Spec.NodeSelector).NotTo(BeNil())
			Expect(job.Spec.Template.Spec.NodeSelector[sliceNameLabel]).To(Equal(expectedSliceName))
		})

		It("should inject annotations on both the Job and Pod template", func() {
			annJobSet := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ann-jobset",
					Namespace: namespace,
					Labels: map[string]string{
						slicePrefixLabel: "ann-prefix",
					},
				},
			}
			Expect(k8sClient.Create(ctx, annJobSet)).To(Succeed())

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ann-job",
					Namespace: namespace,
					Labels: map[string]string{
						jobSetNameLabel:        "ann-jobset",
						replicatedJobNameLabel: "worker",
						jobIndexLabel:          "0",
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

			jobMutator := &JobSetMutator{Client: k8sClient}
			err := jobMutator.Default(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			expectedSliceName := "ann-prefix-worker-0"
			Expect(job.Annotations[sliceNameAnnotation]).To(Equal(expectedSliceName))
			Expect(job.Spec.Template.Annotations[sliceNameAnnotation]).To(Equal(expectedSliceName))
		})
	})
})
