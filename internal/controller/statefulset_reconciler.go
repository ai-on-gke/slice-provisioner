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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	"github.com/ai-on-gke/slice-provisioner/internal/labels"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	EnableKueueAdmission bool
}

type sliceTarget struct {
	name      string
	sliceType string
	topology  string
	priority  string
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=get;list;watch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;delete;update;patch
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching StatefulSet %s: %w", req.NamespacedName, err)
	}

	spec, err := utils.ResolveStatefulSetSlices(sts)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving slice requirements for %s/%s: %w", sts.Namespace, sts.Name, err)
	}

	if spec.SkipSharedLeader {
		return r.releaseOwnedSlices(ctx, sts, true, logger)
	}

	if !spec.HasIntent {
		if !controllerutil.ContainsFinalizer(sts, sliceProvisionerFinalizer) {
			return ctrl.Result{}, nil
		}
		if utils.LWSName(sts) != "" {
			return r.releaseOwnedSlices(ctx, sts, false, logger)
		}
		if err := reconcileFinalizer(ctx, r.Client, sts, false, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer from StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is added before checking deletion, so that an
	// object created and deleted before first reconcile is not missed.
	if err := reconcileFinalizer(ctx, r.Client, sts, true, logger); err != nil {
		return ctrl.Result{}, fmt.Errorf("adding finalizer to StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
	}

	// Handle deletion. Include every Slice CR belonging to this StatefulSet
	// alongside spec.DesiredNames so scaled-down or relabeled Slices are never leaked.
	if !sts.DeletionTimestamp.IsZero() {
		return r.deleteStatefulSetSlices(ctx, sts, spec.DesiredNames, logger)
	}

	if spec.Type == "" {
		logger.Info("StatefulSet is missing slice-type label", "statefulset", sts.Name)
		return ctrl.Result{}, nil
	}
	if spec.Topology == "" {
		logger.Info("StatefulSet is missing slice-topology label", "statefulset", sts.Name)
		return ctrl.Result{}, nil
	}

	if err := r.cleanupObsoleteStatefulSetSlices(ctx, sts, spec.DesiredNames, logger); err != nil {
		return ctrl.Result{}, err
	}
	if len(spec.DesiredNames) == 0 {
		return ctrl.Result{}, nil
	}

	desiredToProvision := spec.DesiredNames
	if r.EnableKueueAdmission {
		var err error
		desiredToProvision, err = r.filterAdmittedSlices(ctx, sts, spec, logger)
		if err != nil || len(desiredToProvision) == 0 {
			return ctrl.Result{}, err
		}
	}

	priorityStr, err := priorityValue(ctx, r.Client, sts.Spec.Template.Spec.PriorityClassName, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving priority for StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
	}

	needsRequeue := false
	var reconcileErrs []error
	for _, sName := range desiredToProvision {
		requeue, err := r.reconcileDesiredSlice(ctx, sts, sliceTarget{
			name:      sName,
			sliceType: spec.Type,
			topology:  spec.Topology,
			priority:  priorityStr,
		}, logger)
		if err != nil {
			reconcileErrs = append(reconcileErrs, err)
			continue
		}
		if requeue {
			needsRequeue = true
		}
	}
	if len(reconcileErrs) > 0 {
		return ctrl.Result{}, errors.Join(reconcileErrs...)
	}
	if needsRequeue {
		return ctrl.Result{RequeueAfter: topologyChangeRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *StatefulSetReconciler) deleteStatefulSetSlices(ctx context.Context, sts *appsv1.StatefulSet, desiredNames []string, logger logr.Logger) (ctrl.Result, error) {
	toDelete := slices.Clone(desiredNames)
	owned, err := r.listOwnedSlices(ctx, sts)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range owned {
		if stsOwnsListedSlice(sts, &owned[i]) && !slices.Contains(toDelete, owned[i].Name) {
			toDelete = append(toDelete, owned[i].Name)
		}
	}
	return handleSlicesDeletionForOwner(ctx, r.Client, sts, utils.KindStatefulSet, toDelete, logger)
}

func (r *StatefulSetReconciler) cleanupObsoleteStatefulSetSlices(ctx context.Context, sts *appsv1.StatefulSet, desiredNames []string, logger logr.Logger) error {
	// Clean up Slices owned by this StatefulSet that are no longer in
	// desiredNames (e.g. after scale-down, scale-to-zero, or LWS
	// size=1 <-> LeaderExcluded transitions).
	owned, err := r.listOwnedSlices(ctx, sts)
	if err != nil {
		return err
	}
	for i := range owned {
		s := &owned[i]
		if slices.Contains(desiredNames, s.Name) || !stsOwnsListedSlice(sts, s) {
			continue
		}
		if _, err := deleteSliceForOwner(ctx, r.Client, sts, utils.KindStatefulSet, s.Name, logger); err != nil {
			return fmt.Errorf("cleaning up obsolete Slice for StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
		}
	}
	return nil
}

// releaseOwnedSlices deletes Slices still owned by a StatefulSet that no longer
// desires Slices (either a size > 1 shared LWS leader or an opted-out StatefulSet)
// before removing sliceProvisionerFinalizer. During an LWS partitioned rolling
// update from size=1 to size > 1, Slices for ordinals < rollingUpdate.partition
// remain pinned to the running size=1 Pods until Partition advances.
func (r *StatefulSetReconciler) releaseOwnedSlices(ctx context.Context, sts *appsv1.StatefulSet, skipSharedLeader bool, logger logr.Logger) (ctrl.Result, error) {
	owned, err := r.listOwnedSlices(ctx, sts)
	if err != nil {
		return ctrl.Result{}, err
	}

	prefix := sts.Spec.Template.Labels[labels.SlicePrefixLabel]
	pinnedCount := partitionPinnedSliceCount(sts, skipSharedLeader)

	var admittedPinnedGroups []bool
	if len(owned) > 0 && pinnedCount > 0 && prefix != "" && r.EnableKueueAdmission {
		if parentName := utils.LWSName(sts); parentName != "" {
			queueName, qErr := r.resolveLWSQueueName(ctx, sts, parentName)
			if qErr != nil {
				return ctrl.Result{}, qErr
			}
			admittedPinnedGroups, err = utils.AdmittedLWSGroups(ctx, r.Client, sts.Namespace, parentName, sts.Name, pinnedCount, queueName)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	names := make([]string, 0, len(owned))
	ownedByName := make(map[string]*slicev1beta1.Slice, len(owned))
	retainedCount := 0
	for i := range owned {
		s := &owned[i]
		if !stsOwnsListedSlice(sts, s) {
			continue
		}
		if pinnedCount > 0 && prefix != "" {
			if ord, ok := utils.ParseIndexedSliceOrdinal(s.Name, prefix); ok && ord < pinnedCount {
				if admittedPinnedGroups == nil || admittedPinnedGroups[ord] {
					retainedCount++
					continue
				}
				logger.V(1).Info("Releasing pinned rollout Slice after Kueue Workload eviction/unadmission",
					"statefulset", sts.Name, "groupIndex", ord, "slice", s.Name)
			}
		}
		names = append(names, s.Name)
		ownedByName[s.Name] = s
	}

	if len(names) == 0 {
		if retainedCount > 0 {
			return ctrl.Result{}, nil
		}
		if skipSharedLeader {
			logger.Info("StatefulSet is LWS Leader (size > 1), skipping Slice CR creation", "statefulset", sts.Name)
		}
		if err := reconcileFinalizer(ctx, r.Client, sts, false, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer from StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
		}
		return ctrl.Result{}, nil
	}

	logger.Info("Releasing Slices no longer desired by StatefulSet",
		"statefulset", sts.Name, "slices", names, "retainedPinned", retainedCount)
	if !controllerutil.ContainsFinalizer(sts, sliceProvisionerFinalizer) {
		// Without a finalizer there is nothing to hold; request deletion best-effort.
		for _, sName := range names {
			if _, err := deleteSliceForOwner(ctx, r.Client, sts, utils.KindStatefulSet, sName, logger); err != nil {
				return ctrl.Result{}, fmt.Errorf("releasing Slice %s from StatefulSet %s/%s: %w", sName, sts.Namespace, sts.Name, err)
			}
		}
		return ctrl.Result{}, nil
	}
	if retainedCount > 0 || sts.DeletionTimestamp.IsZero() {
		return r.deleteReleasedSlicesOnLiveStatefulSet(ctx, sts, names, ownedByName, retainedCount > 0, logger)
	}
	return handleSlicesDeletionForOwner(ctx, r.Client, sts, utils.KindStatefulSet, names, logger)
}

func partitionPinnedSliceCount(sts *appsv1.StatefulSet, skipSharedLeader bool) int {
	if !skipSharedLeader || !sts.DeletionTimestamp.IsZero() {
		return 0
	}
	p := rollingUpdatePartition(sts)
	if p <= 0 {
		return 0
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas <= 0 {
		return 0
	}
	return int(min(p, replicas))
}

func (r *StatefulSetReconciler) deleteReleasedSlicesOnLiveStatefulSet(ctx context.Context, sts *appsv1.StatefulSet, names []string, ownedByName map[string]*slicev1beta1.Slice, keepFinalizer bool, logger logr.Logger) (ctrl.Result, error) {
	var pendingSlices []string
	for _, sName := range names {
		gone, err := deleteSliceForOwner(ctx, r.Client, sts, utils.KindStatefulSet, sName, logger)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			pendingSlices = append(pendingSlices, sName)
		}
	}
	if len(pendingSlices) > 0 {
		var requeueAfter time.Duration
		for _, sName := range pendingSlices {
			backoffObj := client.Object(sts)
			if s := ownedByName[sName]; s != nil && !s.DeletionTimestamp.IsZero() {
				backoffObj = s
			}
			d := sliceDeletionBackoff(backoffObj, sName, logger)
			if requeueAfter == 0 || d < requeueAfter {
				requeueAfter = d
			}
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	if !keepFinalizer {
		if err := reconcileFinalizer(ctx, r.Client, sts, false, logger); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer from StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
		}
	}
	return ctrl.Result{}, nil
}

func stsOwnsListedSlice(sts *appsv1.StatefulSet, s *slicev1beta1.Slice) bool {
	if ownerNS := s.Labels[sliceOwnerNamespaceLabel]; ownerNS != "" {
		return sts.Namespace != "" && ownerNS == sts.Namespace
	}
	prefix := sts.Spec.Template.Labels[labels.SlicePrefixLabel]
	if prefix != "" {
		return utils.MatchesIndexedSlicePrefix(s.Name, prefix) || s.Name == prefix+"-leader"
	}
	if sName := sts.Labels[labels.SliceNameLabel]; sName != "" {
		return s.Name == sName
	}
	if sName := sts.Spec.Template.Labels[labels.SliceNameLabel]; sName != "" {
		return s.Name == sName
	}
	return false
}

func rollingUpdatePartition(sts *appsv1.StatefulSet) int32 {
	if sts == nil || sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		return 0
	}
	ru := sts.Spec.UpdateStrategy.RollingUpdate
	if ru == nil || ru.Partition == nil {
		return 0
	}
	if p := *ru.Partition; p > 0 {
		return p
	}
	return 0
}

func (r *StatefulSetReconciler) filterAdmittedSlices(ctx context.Context, sts *appsv1.StatefulSet, spec utils.StatefulSetSliceSpec, logger logr.Logger) ([]string, error) {
	parentName := utils.LWSName(sts)
	if parentName == "" {
		return spec.DesiredNames, nil
	}
	queueName, err := r.resolveLWSQueueName(ctx, sts, parentName)
	if err != nil {
		return nil, err
	}

	if spec.Prefix != "" {
		admittedGroups, err := utils.AdmittedLWSGroups(ctx, r.Client, sts.Namespace, parentName, sts.Name, len(spec.DesiredNames), queueName)
		if err != nil {
			return nil, err
		}
		admittedNames := make([]string, 0, len(spec.DesiredNames))
		for i, sName := range spec.DesiredNames {
			if admittedGroups[i] {
				admittedNames = append(admittedNames, sName)
				continue
			}
			logger.V(1).Info("Kueue Workload for LWS replica group not admitted yet, holding Slice creation", "groupIndex", i, "slice", sName)
			if _, err := utils.CleanupUnadmittedSliceForOwner(ctx, r.Client, sName, sts.Namespace, sts.Name, utils.KindStatefulSet); err != nil {
				return nil, err
			}
		}
		return admittedNames, nil
	}

	var admitted bool
	if groupIdx, ok := lwsStatefulSetGroupIndex(sts); ok {
		admitted, err = utils.IsLWSGroupWorkloadAdmitted(ctx, r.Client, sts.Namespace, parentName, sts.Name, groupIdx, queueName)
	} else {
		admitted, err = utils.IsWorkloadAdmittedForParent(ctx, r.Client, sts.Namespace, parentName, queueName,
			utils.KindLeaderWorkerSet, utils.KindStatefulSet, utils.KindPod)
	}
	if err != nil {
		return nil, err
	}
	if !admitted {
		logger.V(1).Info("Kueue Workload not admitted yet, holding Slice creation")
		for _, sName := range spec.DesiredNames {
			if _, err := utils.CleanupUnadmittedSliceForOwner(ctx, r.Client, sName, sts.Namespace, sts.Name, utils.KindStatefulSet); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	return spec.DesiredNames, nil
}

func lwsStatefulSetGroupIndex(sts *appsv1.StatefulSet) (int, bool) {
	if idxStr := sts.Labels[labels.LWSGroupIndexLabel]; idxStr != "" {
		idx, err := strconv.Atoi(idxStr)
		return idx, err == nil && idx >= 0
	}
	if idxStr := sts.Spec.Template.Labels[labels.LWSGroupIndexLabel]; idxStr != "" {
		idx, err := strconv.Atoi(idxStr)
		return idx, err == nil && idx >= 0
	}
	if utils.LWSRole(sts) == utils.LWSWorkerRole {
		if idxStr, ok := utils.ParseTrailingIndex(sts.Name); ok {
			idx, err := strconv.Atoi(idxStr)
			return idx, err == nil && idx >= 0
		}
	}
	return 0, false
}

// resolveLWSQueueName resolves the Kueue queue name from the StatefulSet's
// metadata or Pod template labels, falling back to an unstructured Get on the
// parent LeaderWorkerSet CR (avoiding an LWS informer requirement on clusters
// where the LeaderWorkerSet CRD is not installed).
func (r *StatefulSetReconciler) resolveLWSQueueName(ctx context.Context, sts *appsv1.StatefulSet, lwsName string) (string, error) {
	if q := sts.Labels[labels.KueueQueueNameLabel]; q != "" {
		return q, nil
	}
	if q := sts.Spec.Template.Labels[labels.KueueQueueNameLabel]; q != "" {
		return q, nil
	}
	lws := &unstructured.Unstructured{}
	lws.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "leaderworkerset.x-k8s.io",
		Version: "v1",
		Kind:    utils.KindLeaderWorkerSet,
	})
	if err := r.Get(ctx, client.ObjectKey{Namespace: sts.Namespace, Name: lwsName}, lws); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return "", nil
		}
		ReconciliationErrorsTotal.WithLabelValues("GetLWSError").Inc()
		return "", fmt.Errorf("fetching LeaderWorkerSet %s/%s: %w", sts.Namespace, lwsName, err)
	}
	return lws.GetLabels()[labels.KueueQueueNameLabel], nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	b := ctrl.NewControllerManagedBy(mgr).
		WithOptions(opts).
		For(&appsv1.StatefulSet{}, builder.WithPredicates(r.eventFilter())).
		Watches(
			&slicev1beta1.Slice{},
			handler.EnqueueRequestsFromMapFunc(r.sliceToStatefulSet),
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
			handler.EnqueueRequestsFromMapFunc(r.workloadToStatefulSets),
			builder.WithPredicates(utils.WorkloadAdmissionPredicate()),
		)
	}
	return b.Complete(r)
}

func appendUniqueRequest(requests []reconcile.Request, nn types.NamespacedName) []reconcile.Request {
	req := reconcile.Request{NamespacedName: nn}
	if !slices.Contains(requests, req) {
		requests = append(requests, req)
	}
	return requests
}

func (r *StatefulSetReconciler) workloadToStatefulSets(ctx context.Context, o client.Object) []reconcile.Request {
	wl, ok := o.(*utils.Workload)
	if !ok {
		return nil
	}
	var requests []reconcile.Request
	for _, owner := range wl.OwnerReferences {
		switch owner.Kind {
		case utils.KindStatefulSet:
			requests = appendUniqueRequest(requests, types.NamespacedName{Namespace: wl.Namespace, Name: owner.Name})
		case utils.KindPod:
			if _, ok := utils.ParseTrailingIndex(owner.Name); ok {
				requests = appendUniqueRequest(requests, types.NamespacedName{Namespace: wl.Namespace, Name: owner.Name})
			}
			if idx := strings.LastIndex(owner.Name, "-"); idx > 0 {
				requests = appendUniqueRequest(requests, types.NamespacedName{Namespace: wl.Namespace, Name: owner.Name[:idx]})
			}
		case utils.KindLeaderWorkerSet:
			var stsList appsv1.StatefulSetList
			if err := r.List(ctx, &stsList, client.InNamespace(wl.Namespace)); err != nil {
				log.FromContext(ctx).Error(err, "failed to list StatefulSets for Workload mapper", "workload", wl.Name, "leaderworkerset", owner.Name)
				ReconciliationErrorsTotal.WithLabelValues("MapperListError").Inc()
				continue
			}
			for i := range stsList.Items {
				sts := &stsList.Items[i]
				if utils.LWSName(sts) == owner.Name {
					requests = appendUniqueRequest(requests, types.NamespacedName{Namespace: sts.Namespace, Name: sts.Name})
				}
			}
		}
	}
	return requests
}

func (r *StatefulSetReconciler) sliceToStatefulSet(ctx context.Context, o client.Object) []reconcile.Request {
	slice, ok := o.(*slicev1beta1.Slice)
	if !ok {
		return nil
	}
	if ownerKind := slice.Labels[sliceOwnerKindLabel]; ownerKind != "" && ownerKind != utils.KindStatefulSet {
		return nil
	}
	if ownerNS, ownerName := slice.Labels[sliceOwnerNamespaceLabel], slice.Labels[sliceOwnerNameLabel]; ownerNS != "" && ownerName != "" {
		requests := []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: ownerNS,
				Name:      ownerName,
			},
		}}
		var stsList appsv1.StatefulSetList
		if err := r.List(ctx, &stsList, client.InNamespace(ownerNS)); err != nil {
			log.FromContext(ctx).Error(err, "failed to list StatefulSets in owner namespace for Slice mapper", "slice", slice.Name, "namespace", ownerNS)
			ReconciliationErrorsTotal.WithLabelValues("MapperListError").Inc()
			return requests
		}
		for i := range stsList.Items {
			sts := &stsList.Items[i]
			if stsDesiresSlice(sts, slice.Name) {
				requests = appendUniqueRequest(requests, types.NamespacedName{Namespace: sts.Namespace, Name: sts.Name})
			}
		}
		return requests
	}

	var stsList appsv1.StatefulSetList
	if err := r.List(ctx, &stsList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list StatefulSets for Slice mapper", "slice", slice.Name)
		ReconciliationErrorsTotal.WithLabelValues("MapperListError").Inc()
		return nil
	}
	requests := make([]reconcile.Request, 0, len(stsList.Items))
	for i := range stsList.Items {
		sts := &stsList.Items[i]
		if stsOwnsSlice(sts, slice) {
			requests = appendUniqueRequest(requests, types.NamespacedName{
				Namespace: sts.Namespace,
				Name:      sts.Name,
			})
		}
	}
	return requests
}

func stsDesiresSlice(sts *appsv1.StatefulSet, sliceName string) bool {
	spec, err := utils.ResolveStatefulSetSlices(sts)
	if err != nil || !spec.HasIntent {
		return false
	}
	if slices.Contains(spec.DesiredNames, sliceName) {
		return true
	}
	return spec.Prefix != "" && utils.MatchesIndexedSlicePrefix(sliceName, spec.Prefix)
}

// stsOwnsSlice reports whether sts manages slice, covering both single-slice
// StatefulSets and multi-replica LWS size=1 leader StatefulSets.
func stsOwnsSlice(sts *appsv1.StatefulSet, slice *slicev1beta1.Slice) bool {
	if utils.IsForeignSliceOwner(slice, sts.Namespace, sts.Name, utils.KindStatefulSet) {
		return false
	}
	return stsDesiresSlice(sts, slice.Name)
}

func (r *StatefulSetReconciler) eventFilter() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return r.shouldHandleStatefulSet(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return r.shouldReconcileUpdate(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return r.shouldHandleStatefulSet(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return r.shouldHandleStatefulSet(e.Object)
		},
	}
}

func (r *StatefulSetReconciler) shouldHandleStatefulSet(obj client.Object) bool {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		return false
	}
	spec, _ := utils.ResolveStatefulSetSlices(sts)
	return spec.HasIntent || controllerutil.ContainsFinalizer(sts, sliceProvisionerFinalizer)
}

func (r *StatefulSetReconciler) shouldReconcileUpdate(oldObj, newObj client.Object) bool {
	if !r.shouldHandleStatefulSet(oldObj) && !r.shouldHandleStatefulSet(newObj) {
		return false
	}

	oldSts, okOld := oldObj.(*appsv1.StatefulSet)
	newSts, okNew := newObj.(*appsv1.StatefulSet)
	if !okOld || !okNew {
		return false
	}

	if oldSts.DeletionTimestamp.IsZero() && !newSts.DeletionTimestamp.IsZero() {
		return true
	}

	oldSpec, _ := utils.ResolveStatefulSetSlices(oldSts)
	newSpec, _ := utils.ResolveStatefulSetSlices(newSts)
	if !oldSpec.Equal(newSpec) {
		return true
	}

	if rollingUpdatePartition(oldSts) != rollingUpdatePartition(newSts) {
		return true
	}

	if oldSts.Labels[labels.KueueQueueNameLabel] != newSts.Labels[labels.KueueQueueNameLabel] ||
		oldSts.Spec.Template.Labels[labels.KueueQueueNameLabel] != newSts.Spec.Template.Labels[labels.KueueQueueNameLabel] {
		return true
	}

	if oldSts.Spec.Template.Spec.PriorityClassName != newSts.Spec.Template.Spec.PriorityClassName ||
		oldSts.Spec.Template.Spec.SchedulerName != newSts.Spec.Template.Spec.SchedulerName {
		return true
	}

	return false
}

// listOwnedSlices returns existing Slices whose owner labels identify sts.
func (r *StatefulSetReconciler) listOwnedSlices(ctx context.Context, sts *appsv1.StatefulSet) ([]slicev1beta1.Slice, error) {
	var sliceList slicev1beta1.SliceList
	if err := r.List(ctx, &sliceList, client.MatchingLabels{sliceOwnerNameLabel: sts.Name}); err != nil {
		ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
		return nil, fmt.Errorf("listing Slices for StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
	}
	owned := make([]slicev1beta1.Slice, 0, len(sliceList.Items))
	for i := range sliceList.Items {
		s := &sliceList.Items[i]
		if utils.IsSliceOwnedBy(s, sts.Namespace, sts.Name, utils.KindStatefulSet) {
			owned = append(owned, *s)
		}
	}
	return owned, nil
}

// reconcileDesiredSlice ensures a single desired Slice CR exists with the
// expected ownership, type, topology, and priority. It returns requeue=true
// when a Slice is currently terminating and needs fresh creation afterward.
func (r *StatefulSetReconciler) reconcileDesiredSlice(ctx context.Context, sts *appsv1.StatefulSet, target sliceTarget, logger logr.Logger) (bool, error) {
	slice := &slicev1beta1.Slice{}
	err := r.Get(ctx, client.ObjectKey{Name: target.name}, slice)
	if apierrors.IsNotFound(err) {
		if err := createSliceCR(ctx, r.Client, sts, target.name, target.sliceType, target.topology, utils.KindStatefulSet, target.priority, logger); err != nil {
			if errors.Is(err, errSliceTerminating) {
				return true, nil
			}
			return false, fmt.Errorf("reconciling Slice %s for StatefulSet %s/%s: %w", target.name, sts.Namespace, sts.Name, err)
		}
		return false, nil
	}
	if err != nil {
		ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
		return false, fmt.Errorf("fetching Slice %s: %w", target.name, err)
	}

	// A terminating Slice (including one being released by another owner, e.g.
	// an LWS leader handing over to worker StatefulSets after a size change)
	// is never mutated; wait for deletion to complete and recreate it.
	if !slice.DeletionTimestamp.IsZero() {
		delAge := time.Since(slice.DeletionTimestamp.Time)
		if delAge > 5*time.Minute {
			logger.Info("WARNING: Slice deletion is taking unusually long, external finalizer may be blocking", "slice", target.name, "deletingFor", delAge.Round(time.Second))
		}
		logger.Info("Slice is already being deleted, requeueing to wait for deletion to complete", "slice", target.name)
		return true, nil
	}

	if utils.IsForeignSliceOwner(slice, sts.Namespace, sts.Name, utils.KindStatefulSet) {
		if isPrePartitionLWSWorkerSlice(sts, slice, target.name) {
			logger.V(1).Info("Slice is still owned by pre-partition LWS worker StatefulSet during rollout; waiting for worker release",
				"slice", target.name, "actualOwnerName", slice.Labels[sliceOwnerNameLabel])
			return false, nil
		}
		ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict").Inc()
		return false, fmt.Errorf("slice %s is already owned by %s", target.name, formatSliceOwner(slice))
	}

	if string(slice.Spec.Type) != target.sliceType || slice.Spec.Topology != target.topology {
		logger.Info("Type/topology change detected, deleting old Slice to trigger fresh recreation",
			"slice", target.name, "oldType", string(slice.Spec.Type), "newType", target.sliceType,
			"oldTopology", slice.Spec.Topology, "newTopology", target.topology)
		if err := r.Delete(ctx, slice); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Slice already deleted during topology change", "slice", target.name)
				return true, nil
			}
			SliceOperationsTotal.WithLabelValues("Delete", "Error").Inc()
			return false, fmt.Errorf("deleting Slice %s for topology change: %w", target.name, err)
		}
		SliceOperationsTotal.WithLabelValues("Delete", "Success").Inc()
		return true, nil
	}

	if err := backfillSliceOwnerLabels(ctx, r.Client, slice, sts, utils.KindStatefulSet, logger); err != nil {
		return false, err
	}

	if _, err := updateSlicePriority(ctx, r.Client, slice, target.priority, logger); err != nil {
		return false, err
	}
	return false, nil
}

func isPrePartitionLWSWorkerSlice(sts *appsv1.StatefulSet, slice *slicev1beta1.Slice, sliceName string) bool {
	lwsName := utils.LWSName(sts)
	if lwsName == "" || utils.LWSRole(sts) != utils.LWSLeaderRole || sts.Labels[labels.LWSGroupIndexLabel] != "" {
		return false
	}
	p := rollingUpdatePartition(sts)
	if p <= 0 {
		return false
	}
	prefix := sts.Spec.Template.Labels[labels.SlicePrefixLabel]
	ord, ok := utils.ParseIndexedSliceOrdinal(sliceName, prefix)
	if !ok || ord >= int(p) {
		return false
	}
	workerStsName := fmt.Sprintf("%s-%d", lwsName, ord)
	return utils.IsSliceOwnedBy(slice, sts.Namespace, workerStsName, utils.KindStatefulSet)
}
