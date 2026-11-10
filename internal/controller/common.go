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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	slicev1beta1 "github.com/ai-on-gke/slice-provisioner/apis/slice/v1beta1"
	"github.com/ai-on-gke/slice-provisioner/internal/utils"
)

var errSliceTerminating = errors.New("slice is terminating")

// sliceRequirements extracts slice name, type, and topology using utils.SliceRequirements.
func sliceRequirements(obj client.Object) (string, string, string, error) {
	return utils.SliceRequirements(obj)
}

// reconcileFinalizer ensures the slice provisioner finalizer is either added or removed from the object.
func reconcileFinalizer(ctx context.Context, c client.Client, obj client.Object, add bool, logger logr.Logger) error {
	hasFinalizer := controllerutil.ContainsFinalizer(obj, sliceProvisionerFinalizer)
	if add && !hasFinalizer {
		logger.Info("adding finalizer", "kind", obj.GetObjectKind().GroupVersionKind().Kind, "name", obj.GetName())
		deepCopy, ok := obj.DeepCopyObject().(client.Object)
		if !ok {
			logger.Error(nil, "failed to deep copy object for finalizer addition", "name", obj.GetName())
			return fmt.Errorf("failed to deep copy object %s", obj.GetName())
		}
		patch := client.MergeFrom(deepCopy)
		controllerutil.AddFinalizer(obj, sliceProvisionerFinalizer)
		if err := c.Patch(ctx, obj, patch); err != nil {
			ReconciliationErrorsTotal.WithLabelValues("AddFinalizerError").Inc()
			return fmt.Errorf("patching finalizer addition on %s: %w", obj.GetName(), err)
		}
	} else if !add && hasFinalizer {
		logger.Info("removing finalizer", "kind", obj.GetObjectKind().GroupVersionKind().Kind, "name", obj.GetName())
		deepCopy, ok := obj.DeepCopyObject().(client.Object)
		if !ok {
			logger.Error(nil, "failed to deep copy object for finalizer removal", "name", obj.GetName())
			return fmt.Errorf("failed to deep copy object %s", obj.GetName())
		}
		patch := client.MergeFrom(deepCopy)
		controllerutil.RemoveFinalizer(obj, sliceProvisionerFinalizer)
		if err := c.Patch(ctx, obj, patch); err != nil {
			ReconciliationErrorsTotal.WithLabelValues("RemoveFinalizerError").Inc()
			return fmt.Errorf("patching finalizer removal on %s: %w", obj.GetName(), err)
		}
	}
	return nil
}

// handleSliceDeletion manages the deletion of a TPU Slice associated with a client object.
func handleSliceDeletion(ctx context.Context, c client.Client, obj client.Object, sliceName string, logger logr.Logger) (ctrl.Result, error) {
	return handleSlicesDeletionForOwner(ctx, c, obj, "", []string{sliceName}, logger)
}

// handleSlicesDeletion manages the deletion of zero or more TPU Slices associated
// with obj, retaining sliceProvisionerFinalizer until every Slice is NotFound.
func handleSlicesDeletion(ctx context.Context, c client.Client, obj client.Object, sliceNames []string, logger logr.Logger) (ctrl.Result, error) {
	return handleSlicesDeletionForOwner(ctx, c, obj, "", sliceNames, logger)
}

// handleSlicesDeletionForOwner manages the deletion of zero or more TPU Slices
// associated with obj, skipping Slices owned by a different workload and
// retaining sliceProvisionerFinalizer until every owned Slice is NotFound.
func handleSlicesDeletionForOwner(ctx context.Context, c client.Client, obj client.Object, ownerKind string, sliceNames []string, logger logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, sliceProvisionerFinalizer) {
		return ctrl.Result{}, nil
	}

	objLogger := logger.WithValues("kind", obj.GetObjectKind().GroupVersionKind().Kind, "name", obj.GetName())
	var pendingSlices []string
	for _, sName := range sliceNames {
		if sName == "" {
			continue
		}
		gone, err := deleteSliceForOwner(ctx, c, obj, ownerKind, sName, objLogger)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			pendingSlices = append(pendingSlices, sName)
		}
	}

	if len(pendingSlices) > 0 {
		// Safety-net requeue with exponential backoff in case any Slice has an
		// external finalizer or a delete watch event is missed. Evaluate every
		// pending Slice so stuck logs and metrics cover all blocked Slices.
		var requeueAfter time.Duration
		for _, sName := range pendingSlices {
			requeueAfter = sliceDeletionBackoff(obj, sName, objLogger)
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	if err := reconcileFinalizer(ctx, c, obj, false, objLogger); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer during deletion of %s: %w", obj.GetName(), err)
	}

	return ctrl.Result{}, nil
}

// deleteSlice requests deletion of the named Slice unless it is already being
// deleted. It returns true once the Slice no longer exists.
func deleteSlice(ctx context.Context, c client.Client, sliceName string, logger logr.Logger) (bool, error) {
	return deleteSliceForOwner(ctx, c, nil, "", sliceName, logger)
}

// deleteSliceForOwner requests deletion of sliceName unless it is already being
// deleted or is owned by a workload other than obj/ownerKind.
func deleteSliceForOwner(ctx context.Context, c client.Client, obj client.Object, ownerKind, sliceName string, logger logr.Logger) (bool, error) {
	slice := &slicev1beta1.Slice{}
	err := c.Get(ctx, types.NamespacedName{Name: sliceName}, slice)
	if apierrors.IsNotFound(err) {
		SliceDeletionStuckTotal.DeleteLabelValues(sliceName)
		return true, nil
	}
	if err != nil {
		logger.Error(err, "unable to get slice", "slice", sliceName)
		ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
		return false, fmt.Errorf("fetching Slice %s during deletion: %w", sliceName, err)
	}
	if obj != nil && utils.IsForeignSliceOwner(slice, obj.GetNamespace(), obj.GetName(), ownerKind) {
		logger.Info("skipping deletion of Slice owned by another workload",
			"slice", sliceName,
			"actualOwnerNamespace", slice.Labels[sliceOwnerNamespaceLabel],
			"actualOwnerName", slice.Labels[sliceOwnerNameLabel],
			"actualOwnerKind", slice.Labels[sliceOwnerKindLabel])
		return true, nil
	}
	if !slice.DeletionTimestamp.IsZero() {
		return false, nil
	}
	logger.Info("deleting slice for cleanup", "sliceName", sliceName)
	if err := c.Delete(ctx, slice); err != nil {
		if apierrors.IsNotFound(err) {
			SliceDeletionStuckTotal.DeleteLabelValues(sliceName)
			return true, nil
		}
		logger.Error(err, "unable to delete slice", "slice", sliceName)
		ReconciliationErrorsTotal.WithLabelValues("DeleteSliceError").Inc()
		SliceOperationsTotal.WithLabelValues("Delete", "Error").Inc()
		return false, fmt.Errorf("deleting Slice %s: %w", sliceName, err)
	}
	SliceOperationsTotal.WithLabelValues("Delete", "Success").Inc()
	return false, nil
}

// priorityValue fetches the priority value from a PriorityClass.
func priorityValue(ctx context.Context, c client.Client, priorityClassName string, logger logr.Logger) (string, error) {
	if priorityClassName == "" {
		return "", nil
	}
	priorityClass := &schedulingv1.PriorityClass{}
	err := c.Get(ctx, types.NamespacedName{Name: priorityClassName}, priorityClass)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("priorityclass not found", "priorityClassName", priorityClassName)
			return "", nil
		}
		logger.Error(err, "unable to fetch priorityclass", "priorityClassName", priorityClassName)
		ReconciliationErrorsTotal.WithLabelValues("GetPriorityClassError").Inc()
		return "", fmt.Errorf("fetching PriorityClass %s: %w", priorityClassName, err)
	}
	return strconv.Itoa(int(priorityClass.Value)), nil
}

// createSliceCR creates a new Slice custom resource.
func createSliceCR(ctx context.Context, c client.Client, obj client.Object, sliceName, sliceType, sliceTopology, ownerKind, priorityStr string, logger logr.Logger) error {
	logger.Info("creating a new slice", "sliceName", sliceName)
	sliceLabels := map[string]string{
		sliceOwnerNameLabel: obj.GetName(),
		sliceOwnerKindLabel: ownerKind,
	}
	if obj.GetNamespace() != "" {
		sliceLabels[sliceOwnerNamespaceLabel] = obj.GetNamespace()
	}
	newSlice := &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   sliceName,
			Labels: sliceLabels,
		},
		Spec: slicev1beta1.SliceSpec{
			Type:         slicev1beta1.Type(sliceType),
			Topology:     sliceTopology,
			PartitionIds: []string{},
		},
	}

	annotations := map[string]string{
		managedByAnnotation:      sliceSchedulerName,
		retryOnFailureAnnotation: "true",
	}

	if priorityStr != "" {
		annotations[slicePriorityAnnotation] = priorityStr
	}
	newSlice.Annotations = annotations

	if err := c.Create(ctx, newSlice); err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing := &slicev1beta1.Slice{}
			if getErr := c.Get(ctx, types.NamespacedName{Name: sliceName}, existing); getErr != nil {
				logger.Error(getErr, "Slice already exists but failed to fetch for validation", "slice", sliceName)
				ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
				return fmt.Errorf("fetching existing Slice %s for validation: %w", sliceName, getErr)
			}
			if !existing.DeletionTimestamp.IsZero() {
				logger.Info("Existing Slice is terminating, waiting for deletion before recreating", "slice", sliceName)
				return fmt.Errorf("%w: %s", errSliceTerminating, sliceName)
			}
			if utils.IsForeignSliceOwner(existing, obj.GetNamespace(), obj.GetName(), ownerKind) {
				ReconciliationErrorsTotal.WithLabelValues("SliceOwnerConflict").Inc()
				return fmt.Errorf("slice %s is already owned by %s", sliceName, formatSliceOwner(existing))
			}
			if string(existing.Spec.Type) != sliceType || existing.Spec.Topology != sliceTopology {
				logger.Error(nil, "Slice already exists with mismatched type/topology",
					"slice", sliceName,
					"expectedType", sliceType, "actualType", string(existing.Spec.Type),
					"expectedTopology", sliceTopology, "actualTopology", existing.Spec.Topology,
				)
				ReconciliationErrorsTotal.WithLabelValues("SliceTypeMismatch").Inc()
				return fmt.Errorf("Slice %s already exists with type=%s/topology=%s, expected type=%s/topology=%s",
					sliceName, existing.Spec.Type, existing.Spec.Topology, sliceType, sliceTopology)
			}
			if err := backfillSliceOwnerLabels(ctx, c, existing, obj, ownerKind, logger); err != nil {
				return err
			}
			logger.Info("Slice already exists with matching type/topology, treating as success", "slice", sliceName)
			return nil
		}
		logger.Error(err, "unable to create slice", "slice", sliceName)
		ReconciliationErrorsTotal.WithLabelValues("CreateSliceError").Inc()
		SliceOperationsTotal.WithLabelValues("Create", "Error").Inc()
		return fmt.Errorf("creating Slice %s: %w", sliceName, err)
	}
	SliceOperationsTotal.WithLabelValues("Create", "Success").Inc()
	if obj.GetGeneration() <= 1 {
		SliceCreationProcessingMilliseconds.Observe(float64(time.Since(obj.GetCreationTimestamp().Time).Milliseconds()))
	}
	return nil
}

func formatSliceOwner(slice *slicev1beta1.Slice) string {
	kind := slice.Labels[sliceOwnerKindLabel]
	ns := slice.Labels[sliceOwnerNamespaceLabel]
	name := slice.Labels[sliceOwnerNameLabel]
	ref := name
	if ns != "" {
		ref = ns + "/" + name
	}
	if kind == "" {
		return strings.TrimSpace(ref)
	}
	return fmt.Sprintf("%s %s", kind, ref)
}

func backfillSliceOwnerLabels(ctx context.Context, c client.Client, slice *slicev1beta1.Slice, obj client.Object, ownerKind string, logger logr.Logger) error {
	needsNamespace := obj.GetNamespace() != "" && slice.Labels[sliceOwnerNamespaceLabel] == ""
	if slice.Labels[sliceOwnerNameLabel] != "" && slice.Labels[sliceOwnerKindLabel] != "" && !needsNamespace {
		return nil
	}
	deepCopy := slice.DeepCopy()
	patch := client.MergeFrom(deepCopy)
	if slice.Labels == nil {
		slice.Labels = make(map[string]string)
	}
	if slice.Labels[sliceOwnerNameLabel] == "" {
		slice.Labels[sliceOwnerNameLabel] = obj.GetName()
	}
	if slice.Labels[sliceOwnerKindLabel] == "" {
		slice.Labels[sliceOwnerKindLabel] = ownerKind
	}
	if needsNamespace {
		slice.Labels[sliceOwnerNamespaceLabel] = obj.GetNamespace()
	}
	if patchErr := c.Patch(ctx, slice, patch); patchErr != nil {
		logger.Error(patchErr, "failed to backfill owner labels on existing Slice", "slice", slice.Name)
		ReconciliationErrorsTotal.WithLabelValues("PatchSliceError").Inc()
		return fmt.Errorf("backfilling owner labels on Slice %s: %w", slice.Name, patchErr)
	}
	logger.Info("backfilled owner labels on existing Slice", "slice", slice.Name,
		"ownerNamespace", obj.GetNamespace(), "ownerName", obj.GetName(), "ownerKind", ownerKind)
	return nil
}

// updateSlicePriority patches the Slice's priority annotation if it differs
// from the expected value. Returns true if a patch was applied.
func updateSlicePriority(ctx context.Context, c client.Client, slice *slicev1beta1.Slice, expectedPriority string, logger logr.Logger) (bool, error) {
	currentPriority := slice.Annotations[slicePriorityAnnotation]
	if currentPriority == expectedPriority {
		return false, nil
	}

	deepCopy := slice.DeepCopy()
	patch := client.MergeFrom(deepCopy)
	if slice.Annotations == nil {
		slice.Annotations = make(map[string]string)
	}
	if expectedPriority == "" {
		delete(slice.Annotations, slicePriorityAnnotation)
	} else {
		slice.Annotations[slicePriorityAnnotation] = expectedPriority
	}
	if err := c.Patch(ctx, slice, patch); err != nil {
		logger.Error(err, "failed to update Slice priority annotation", "slice", slice.Name,
			"oldPriority", currentPriority, "newPriority", expectedPriority)
		ReconciliationErrorsTotal.WithLabelValues("UpdateSlicePriorityError").Inc()
		return false, fmt.Errorf("patching Slice %s priority annotation: %w", slice.Name, err)
	}
	logger.Info("updated Slice priority annotation", "slice", slice.Name,
		"oldPriority", currentPriority, "newPriority", expectedPriority)
	return true, nil
}

const (
	sliceDeletionBackoffInitial = 30 * time.Second
	sliceDeletionBackoffMax     = 5 * time.Minute
	sliceDeletionStuckThreshold = 5 * time.Minute
)

// sliceDeletionBackoff computes the requeue delay for a Slice that still
// exists after a delete request. It uses exponential backoff starting at
// 30s, doubling on each cycle, capped at 5m. When the Slice has been
// pending deletion for longer than sliceDeletionStuckThreshold, a warning
// is logged and a metric is incremented.
func sliceDeletionBackoff(obj client.Object, sliceName string, logger logr.Logger) time.Duration {
	deletionTS := obj.GetDeletionTimestamp()
	if deletionTS == nil {
		return sliceDeletionBackoffInitial
	}

	elapsed := time.Since(deletionTS.Time)
	if elapsed < 0 {
		elapsed = 0
	}

	// Compute which backoff interval we're on: 30s, 60s, 120s, 300s(cap).
	// interval = initial * 2^n, where n = floor(elapsed / initial).
	// Cap n early to avoid float64→int64 overflow for large elapsed times
	// (2^4 * 30s = 480s already exceeds 5m cap).
	n := int(elapsed / sliceDeletionBackoffInitial)
	if n > 4 {
		n = 4
	}
	backoff := time.Duration(float64(sliceDeletionBackoffInitial) * math.Pow(2, float64(n)))
	if backoff > sliceDeletionBackoffMax {
		backoff = sliceDeletionBackoffMax
	}

	if elapsed >= sliceDeletionStuckThreshold {
		logger.Error(nil, "Slice deletion stuck: Slice still exists after threshold, may have an external finalizer",
			"slice", sliceName,
			"elapsed", elapsed.Round(time.Second).String(),
			"nextRequeue", backoff.String(),
		)
		// Increments every reconcile cycle past the threshold (not once per
		// stuck episode). This is intentional for rate-based alerting.
		SliceDeletionStuckTotal.WithLabelValues(sliceName).Inc()
	}

	return backoff
}
