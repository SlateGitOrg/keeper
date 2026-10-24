package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/metrics"
	"github.com/archit2501/keeper/internal/snapshot"
)

// RestoreReconciler materialises a Completed Backup into new claims.
type RestoreReconciler struct {
	Base
	Drivers    map[keeperv1.DriverType]snapshot.Driver
	CSIEnabled bool
}

// +kubebuilder:rbac:groups=backup.keeper.dev,resources=restores,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=backup.keeper.dev,resources=restores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.keeper.dev,resources=restores/finalizers,verbs=update

// Reconcile implements reconcile.Reconciler.
func (r *RestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rs keeperv1.Restore
	// Read-your-writes: the reconcile builds on its own last status write.
	if err := r.APIReader.Get(ctx, req.NamespacedName, &rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if rs.Status.Phase.Terminal() || !rs.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	orig := rs.DeepCopy()
	res, stepErr := r.step(ctx, &rs)
	rs.Status.ObservedGeneration = rs.Generation
	conflict, err := r.patchStatus(ctx, &rs, orig, !equality.Semantic.DeepEqual(orig.Status, rs.Status))
	if err != nil {
		return ctrl.Result{}, errors.Join(stepErr, err)
	}
	if conflict {
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	return res, stepErr
}

func (r *RestoreReconciler) step(ctx context.Context, rs *keeperv1.Restore) (ctrl.Result, error) {
	if rs.Status.Phase == "" {
		rs.Status.Phase = keeperv1.RestorePending
		rs.Status.StartTime = r.metaNow()
	}
	var b keeperv1.Backup
	if err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.BackupName}, &b); err != nil {
		if apierrors.IsNotFound(err) {
			return r.fail(rs, "BackupNotFound", fmt.Sprintf("backup %s not found", rs.Spec.BackupName))
		}
		return ctrl.Result{}, err
	}
	switch {
	case !b.DeletionTimestamp.IsZero() && rs.Status.Phase == keeperv1.RestorePending:
		// A Restore already reading the backup finishes first: the Backup
		// finalizer waits for it. A new one must not start.
		return r.fail(rs, "BackupDeleting", fmt.Sprintf("backup %s is being deleted", b.Name))
	case b.Status.Phase == keeperv1.BackupFailed:
		return r.fail(rs, "BackupFailed", fmt.Sprintf("backup %s failed: %s", b.Name, b.Status.Message))
	case b.Status.Phase != keeperv1.BackupCompleted:
		rs.Status.Message = fmt.Sprintf("waiting for backup %s to complete", b.Name)
		setCondition(&rs.Status.Conditions, rs.Generation, keeperv1.ConditionReady, false, "WaitingForBackup", rs.Status.Message)
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	if b.Spec.Driver.Type == keeperv1.DriverCSISnapshot && !r.CSIEnabled {
		return r.fail(rs, "DriverUnavailable", "the VolumeSnapshot API is not installed in this cluster")
	}
	d, err := snapshot.For(b.Spec.Driver.Type, r.Drivers)
	if err != nil {
		return r.fail(rs, "DriverUnavailable", err.Error())
	}
	vols, err := plan(rs, &b)
	if err != nil {
		return r.fail(rs, "InvalidRestore", err.Error())
	}

	if err := d.StartRestore(ctx, rs, &b, vols); err != nil {
		var conflict *snapshot.ConflictError
		if errors.As(err, &conflict) {
			return r.fail(rs, "TargetExists", err.Error())
		}
		return ctrl.Result{}, err
	}
	if rs.Status.Phase == keeperv1.RestorePending {
		rs.Status.Phase = keeperv1.RestoreRestoring
		r.normal(rs, "Restoring", "Restoring %d volume(s) from backup %s", len(vols), b.Name)
	}
	p, err := d.ObserveRestore(ctx, rs, &b, vols)
	if err != nil {
		return ctrl.Result{}, err
	}
	rs.Status.Volumes = rs.Status.Volumes[:0]
	for _, v := range vols {
		rs.Status.Volumes = append(rs.Status.Volumes, keeperv1.VolumeRestore{
			SourcePVC: v.Source.PVCName, TargetPVC: v.Target, JobName: p.Jobs[v.Target], Ready: p.Ready[v.Target],
		})
	}
	if p.Failed {
		return r.fail(rs, "RestoreFailed", p.Message)
	}
	if !p.Done {
		rs.Status.Message = "restoring"
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	rs.Status.Phase = keeperv1.RestoreCompleted
	rs.Status.CompletionTime = r.metaNow()
	rs.Status.Message = ""
	setCondition(&rs.Status.Conditions, rs.Generation, keeperv1.ConditionReady, true, "Completed", "all volumes restored")
	metrics.RestoresTotal.WithLabelValues("completed").Inc()
	r.normal(rs, "Completed", "Restored %d volume(s)", len(vols))
	return ctrl.Result{}, nil
}

// plan maps the Backup's volumes to target claim names.
func plan(rs *keeperv1.Restore, b *keeperv1.Backup) ([]snapshot.RestoreVolume, error) {
	recorded := map[string]keeperv1.VolumeBackup{}
	for _, v := range b.Status.Volumes {
		recorded[v.PVCName] = v
	}
	names := slices.Clone(rs.Spec.Volumes)
	if len(names) == 0 {
		for _, v := range b.Status.Volumes {
			names = append(names, v.PVCName)
		}
	}
	slices.Sort(names)
	var out []snapshot.RestoreVolume
	for _, n := range names {
		v, ok := recorded[n]
		if !ok {
			return nil, fmt.Errorf("backup %s has no volume %q", b.Name, n)
		}
		target := rs.Spec.PVCNamePrefix + n
		if errs := validation.IsDNS1123Subdomain(target); len(errs) > 0 {
			return nil, fmt.Errorf("target claim name %q is invalid: %v", target, errs)
		}
		out = append(out, snapshot.RestoreVolume{Source: v, Target: target})
	}
	return out, nil
}

func (r *RestoreReconciler) fail(rs *keeperv1.Restore, reason, msg string) (ctrl.Result, error) {
	rs.Status.Phase = keeperv1.RestoreFailed
	rs.Status.Message = msg
	rs.Status.CompletionTime = r.metaNow()
	setCondition(&rs.Status.Conditions, rs.Generation, keeperv1.ConditionReady, false, reason, msg)
	metrics.RestoresTotal.WithLabelValues("failed").Inc()
	r.warning(rs, reason, "Restore failed: %s", msg)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler.
func (r *RestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&keeperv1.Restore{}).
		Owns(&batchv1.Job{}).
		Watches(&keeperv1.Backup{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			var list keeperv1.RestoreList
			if err := mgr.GetClient().List(ctx, &list, client.InNamespace(o.GetNamespace()), client.MatchingFields{IndexRestoreBackup: o.GetName()}); err != nil {
				return nil
			}
			reqs := make([]reconcile.Request, 0, len(list.Items))
			for _, rs := range list.Items {
				if !rs.Status.Phase.Terminal() {
					reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&rs)})
				}
			}
			return reqs
		})).
		Named("restore").
		Complete(r)
}
