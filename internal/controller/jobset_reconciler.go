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
	"fmt"

	"github.com/ai-on-gke/slice-provisioner/internal/utils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// JobSetReconciler handles TPU Slice provisioning and cleanup for Jobs managed by a JobSet.
type JobSetReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	EnableKueueAdmission bool
}

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;delete;update;patch
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=get;list;watch

// Reconcile coordinates the provisioning and cleanup of TPU Slices for individual Jobs.
func (r *JobSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("job", req.NamespacedName)

	job := &batchv1.Job{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to fetch Job, skipping reconciliation")
		return ctrl.Result{}, fmt.Errorf("fetching Job %s: %w", req.NamespacedName, err)
	}

	logger.Info("Starting Job reconciliation")
	sliceName := job.GetLabels()[sliceNameLabel]

	// Guard: if this Job no longer meets handling criteria (labels removed)
	// but still has our finalizer, remove the finalizer and return.
	// This prevents Jobs from getting stuck in Terminating state.
	// Mirrors the StatefulSet reconciler pattern at statefulset_reconciler.go L72-78.
	// NOTE: This does NOT clean up any orphaned Slice CR that was created
	// before labels were removed — the sliceName is no longer available.
	// Orphaned Slices are handled by the slice-scheduler's garbage collection.
	if !(managedByJobSet(job) && hasSliceLabel(job) && requestsTPU(job)) {
		if controllerutil.ContainsFinalizer(job, sliceProvisionerFinalizer) {
			logger.Info("Job no longer meets handling criteria but has finalizer, removing finalizer",
				"name", job.Name, "namespace", job.Namespace)
			if err := reconcileFinalizer(ctx, r.Client, job, false, logger); err != nil {
				return ctrl.Result{}, fmt.Errorf("removing finalizer from unlabeled Job %s/%s: %w", job.Namespace, job.Name, err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is added before checking deletion, so that a
	// Job created and deleted before first reconcile is not missed.
	if err := reconcileFinalizer(ctx, r.Client, job, true, logger); err != nil {
		return ctrl.Result{}, fmt.Errorf("adding finalizer to Job %s/%s: %w", job.Namespace, job.Name, err)
	}

	// Handle deletion and cleanup of associated TPU Slices.
	if !job.DeletionTimestamp.IsZero() {
		return handleSlicesDeletionForOwner(ctx, r.Client, job, utils.KindJob, []string{sliceName}, logger)
	}

	// A finished Job no longer needs its Slice. Return before the
	// provisioning path below so the Slice delete event, which re-enqueues
	// this Job via sliceToJob, does not recreate it.
	if jobFinished(job) {
		if _, err := deleteSliceForOwner(ctx, r.Client, job, utils.KindJob, sliceName, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("deleting Slice %s for finished Job %s/%s: %w", sliceName, job.Namespace, job.Name, err)
		}
		return ctrl.Result{}, nil
	}

	jobSetName := job.GetLabels()[jobSetNameLabel]
	jobSet := &jobsetv1alpha2.JobSet{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: jobSetName}, jobSet); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Error(err, "Parent JobSet not found; requeueing to handle potential deletion race")
			return ctrl.Result{RequeueAfter: topologyChangeRequeueInterval}, nil
		}
		logger.Error(err, "Failed to fetch parent JobSet")
		return ctrl.Result{}, fmt.Errorf("fetching parent JobSet %s/%s: %w", job.Namespace, jobSetName, err)
	}

	if r.EnableKueueAdmission {
		if jobSet.Spec.Suspend != nil && *jobSet.Spec.Suspend {
			return utils.CleanupUnadmittedSliceForOwner(ctx, r.Client, sliceName, job.Namespace, job.Name, utils.KindJob)
		}
		queueName := jobSet.Labels["kueue.x-k8s.io/queue-name"]
		admitted, err := utils.IsWorkloadAdmittedForParent(ctx, r.Client, jobSet.Namespace, jobSet.Name, queueName, utils.KindJobSet)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !admitted {
			logger.V(1).Info("Kueue Workload not admitted yet, holding Slice creation")
			return utils.CleanupUnadmittedSliceForOwner(ctx, r.Client, sliceName, job.Namespace, job.Name, utils.KindJob)
		}
	}

	sliceName, sliceType, sliceTopology, err := sliceRequirements(job)
	if err != nil {
		logger.Error(err, "invalid TPU Slice requirements", "job", job.Name)
		return ctrl.Result{}, fmt.Errorf("resolving slice requirements for %s/%s: %w", job.Namespace, job.Name, err)
	}
	if sliceType == "" || sliceTopology == "" {
		logger.Info("Job missing Slice labels, falling back to parent JobSet", "jobSet", jobSet.Name)
		var jsErr error
		sliceName, sliceType, sliceTopology, jsErr = sliceRequirements(jobSet)
		if jsErr != nil {
			logger.Error(jsErr, "invalid TPU Slice requirements on parent JobSet", "jobSet", jobSet.Name)
			return ctrl.Result{}, fmt.Errorf("resolving slice requirements for parent JobSet %s/%s: %w", jobSet.Namespace, jobSet.Name, jsErr)
		}
	}
	if sliceType == "" || sliceTopology == "" {
		logger.V(1).Info("Job and parent JobSet missing Slice requirements (type/topology), skipping Slice creation")
		return ctrl.Result{}, nil
	}

	// Check if a Slice CR already exists for this Job.
	slice := &slicev1beta1.Slice{}
	err = r.Get(ctx, client.ObjectKey{Name: sliceName}, slice)
	if err == nil {
		if !slice.DeletionTimestamp.IsZero() {
			logger.Info("Slice is already being deleted, requeueing to wait for deletion to complete", "slice", sliceName)
			return ctrl.Result{RequeueAfter: topologyChangeRequeueInterval}, nil
		}
		if utils.IsForeignSliceOwner(slice, job.Namespace, job.Name, utils.KindJob) {
			ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict").Inc()
			return ctrl.Result{}, fmt.Errorf("slice %s is already owned by %s (requested by Job %s/%s)",
				sliceName, formatSliceOwner(slice), job.Namespace, job.Name)
		}
		if string(slice.Spec.Type) != sliceType || slice.Spec.Topology != sliceTopology {
			logger.Info("Type/topology change detected, deleting old Slice to trigger fresh recreation",
				"oldType", string(slice.Spec.Type), "newType", sliceType,
				"oldTopology", slice.Spec.Topology, "newTopology", sliceTopology)
			if err := r.Delete(ctx, slice); err != nil {
				if apierrors.IsNotFound(err) {
					logger.Info("Slice already deleted during topology change", "slice", sliceName)
				} else {
					logger.Error(err, "Failed to delete old Slice for topology update recreation")
					SliceOperationsTotal.WithLabelValues("Delete", "Error").Inc()
					return ctrl.Result{}, fmt.Errorf("deleting Slice %s for topology change: %w", sliceName, err)
				}
			} else {
				SliceOperationsTotal.WithLabelValues("Delete", "Success").Inc()
			}
			return ctrl.Result{RequeueAfter: topologyChangeRequeueInterval}, nil
		}

		if err := backfillSliceOwnerLabels(ctx, r.Client, slice, job, utils.KindJob, logger); err != nil {
			return ctrl.Result{}, err
		}

		// Slice exists and type/topology match. Check if priority needs updating.
		priorityStr, err := priorityValue(ctx, r.Client, job.Spec.Template.Spec.PriorityClassName, logger)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("resolving priority for Job %s/%s: %w", job.Namespace, job.Name, err)
		}
		if _, err := updateSlicePriority(ctx, r.Client, slice, priorityStr, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating priority for Slice %s: %w", sliceName, err)
		}
		return ctrl.Result{}, nil
	}
	if !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to fetch slice CR", "slice", sliceName)
		ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
		return ctrl.Result{}, fmt.Errorf("fetching Slice %s: %w", sliceName, err)
	}

	// Determine Slice priority based on Job's PriorityClass.
	priorityStr, err := priorityValue(ctx, r.Client, job.Spec.Template.Spec.PriorityClassName, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving priority for Job %s/%s: %w", job.Namespace, job.Name, err)
	}

	// Provision a new Slice CR for the Job.
	if err := createSliceCR(ctx, r.Client, job, sliceName, sliceType, sliceTopology, utils.KindJob, priorityStr, logger); err != nil {
		if errors.Is(err, errSliceTerminating) {
			return ctrl.Result{RequeueAfter: topologyChangeRequeueInterval}, nil
		}
		return ctrl.Result{}, fmt.Errorf("creating Slice %s for Job %s/%s: %w", sliceName, job.Namespace, job.Name, err)
	}

	return ctrl.Result{}, nil
}

func (r *JobSetReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	b := ctrl.NewControllerManagedBy(mgr).
		WithOptions(opts).
		For(&batchv1.Job{}, builder.WithPredicates(r.eventFilter())).
		Watches(
			&slicev1beta1.Slice{},
			handler.EnqueueRequestsFromMapFunc(r.sliceToJob),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(e event.CreateEvent) bool { return false },
				UpdateFunc:  func(e event.UpdateEvent) bool { return false },
				DeleteFunc:  func(e event.DeleteEvent) bool { return true },
				GenericFunc: func(e event.GenericEvent) bool { return false },
			}),
		)
	if r.EnableKueueAdmission {
		b.Watches(
			&utils.Workload{},
			handler.EnqueueRequestsFromMapFunc(r.workloadToJobs),
			builder.WithPredicates(utils.WorkloadAdmissionPredicate()),
		)
	}
	return b.Complete(r)
}

func (r *JobSetReconciler) workloadToJobs(ctx context.Context, o client.Object) []reconcile.Request {
	wl, ok := o.(*utils.Workload)
	if !ok {
		return nil
	}
	var requests []reconcile.Request
	for _, owner := range wl.OwnerReferences {
		if owner.Kind == "JobSet" {
			var jobList batchv1.JobList
			if err := r.List(ctx, &jobList, client.InNamespace(wl.Namespace), client.MatchingLabels{jobSetNameLabel: owner.Name}); err != nil {
				log.FromContext(ctx).Error(err, "failed to list Jobs for Workload mapper", "workload", wl.Name, "jobset", owner.Name)
				ReconciliationErrorsTotal.WithLabelValues("MapperListError").Inc()
			} else {
				for _, job := range jobList.Items {
					requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: job.Namespace, Name: job.Name}})
				}
			}
		} else if owner.Kind == "Job" {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: wl.Namespace, Name: owner.Name}})
		}
	}
	return requests
}

func (r *JobSetReconciler) sliceToJob(ctx context.Context, o client.Object) []reconcile.Request {
	slice, ok := o.(*slicev1beta1.Slice)
	if !ok {
		return nil
	}
	if ownerKind := slice.Labels[sliceOwnerKindLabel]; ownerKind != "" && ownerKind != utils.KindJob {
		return nil
	}
	var jobList batchv1.JobList
	if err := r.List(ctx, &jobList, client.MatchingLabels{sliceNameLabel: slice.Name}); err != nil {
		log.FromContext(ctx).Error(err, "failed to list Jobs for Slice mapper", "slice", slice.Name)
		ReconciliationErrorsTotal.WithLabelValues("MapperListError").Inc()
		return nil
	}
	requests := make([]reconcile.Request, 0, len(jobList.Items))
	for _, job := range jobList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: job.Namespace,
				Name:      job.Name,
			},
		})
	}
	return requests
}

func (r *JobSetReconciler) eventFilter() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return r.shouldHandleJob(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return r.shouldReconcileUpdate(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return r.shouldHandleJob(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return r.shouldHandleJob(e.Object)
		},
	}
}

func (r *JobSetReconciler) shouldHandleJob(obj client.Object) bool {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return false
	}
	// Check finalizer in addition to labels so that Jobs whose labels were
	// removed after the finalizer was added still get reconciled for cleanup.
	// Without this, such Jobs get stuck in Terminating forever.
	hasFinalizer := controllerutil.ContainsFinalizer(job, sliceProvisionerFinalizer)
	return hasFinalizer || (managedByJobSet(job) && hasSliceLabel(job) && requestsTPU(job))
}

func (r *JobSetReconciler) shouldReconcileUpdate(oldObj, newObj client.Object) bool {
	if !r.shouldHandleJob(oldObj) && !r.shouldHandleJob(newObj) {
		return false
	}

	oldJob, okOld := oldObj.(*batchv1.Job)
	newJob, okNew := newObj.(*batchv1.Job)
	if !okOld || !okNew {
		return false
	}

	if oldJob.DeletionTimestamp.IsZero() && !newJob.DeletionTimestamp.IsZero() {
		return true
	}

	if !jobFinished(oldJob) && jobFinished(newJob) {
		return true
	}

	if oldJob.Labels[sliceNameLabel] != newJob.Labels[sliceNameLabel] ||
		oldJob.Labels[sliceTypeLabel] != newJob.Labels[sliceTypeLabel] ||
		oldJob.Labels[sliceTopologyLabel] != newJob.Labels[sliceTopologyLabel] {
		return true
	}

	if oldJob.Spec.Template.Spec.PriorityClassName != newJob.Spec.Template.Spec.PriorityClassName ||
		oldJob.Spec.Template.Spec.SchedulerName != newJob.Spec.Template.Spec.SchedulerName {
		return true
	}

	// NOTE: We intentionally do NOT check slicePriorityAnnotation on the
	// workload here. priorityValue() derives priority solely from
	// PriorityClassName, so an annotation-only change would trigger a
	// no-op reconcile. Priority annotation changes on the Slice CR
	// itself are handled by PriorityClassName changes above.

	return false
}

// jobFinished reports whether the Job has a Complete or Failed condition.
// Recent Kubernetes versions add these only after the Job's Pods have terminated,
// unlike SuccessCriteriaMet and FailureTarget, so the Slice is no longer in
// use once either is set.
func jobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func managedByJobSet(job *batchv1.Job) bool {
	val, ok := job.Labels[jobSetNameLabel]
	return ok && val != ""
}

func hasSliceLabel(job *batchv1.Job) bool {
	val, ok := job.Labels[sliceNameLabel]
	return ok && val != ""
}

func requestsTPU(job *batchv1.Job) bool {
	// Check both regular containers and init containers to match
	// the validator's behavior (jobset_validator.go).
	// NOTE: We use separate loops instead of append() to avoid
	// mutating the informer cache's backing array.
	for _, container := range job.Spec.Template.Spec.Containers {
		_, inLimits := container.Resources.Limits[tpuResourceName]
		_, inRequests := container.Resources.Requests[tpuResourceName]
		if inLimits || inRequests {
			return true
		}
	}
	for _, container := range job.Spec.Template.Spec.InitContainers {
		_, inLimits := container.Resources.Limits[tpuResourceName]
		_, inRequests := container.Resources.Requests[tpuResourceName]
		if inLimits || inRequests {
			return true
		}
	}
	return false
}
