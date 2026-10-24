package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/hooks"
	"github.com/archit2501/keeper/internal/metrics"
	"github.com/archit2501/keeper/internal/snapshot"
	"github.com/archit2501/keeper/internal/storage"
)

// BackupManifestVersion is the schema version of BackupManifest.
const BackupManifestVersion = 1

// BackupManifest is written to object storage when a Backup completes. It is
// self-describing so a Backup can be understood (and, in future, re-imported)
// from the bucket alone.
type BackupManifest struct {
	Version      int                     `json:"version"`
	Namespace    string                  `json:"namespace"`
	Name         string                  `json:"name"`
	UID          types.UID               `json:"uid"`
	Policy       string                  `json:"policy,omitempty"`
	Driver       keeperv1.DriverType     `json:"driver"`
	SnapshotTime time.Time               `json:"snapshotTime"`
	Volumes      []keeperv1.VolumeBackup `json:"volumes"`
	Hooks        []keeperv1.HookResult   `json:"hooks,omitempty"`
}

// BackupReconciler drives a Backup through
// Pending → PreHooks → Snapshotting → PostHooks → Finalizing → Completed,
// failing to Failed from any step. Each reconcile performs one step and
// persists status, so the lifecycle survives operator restarts.
//
// The Backup itself is read from the API server rather than the informer
// cache: steps have side effects (hooks exec into the workload), so a step
// must never run from a copy that predates the status write of the previous
// step. Status writes use optimistic locking for the same reason.
type BackupReconciler struct {
	Base
	Stores  storage.Factory
	Drivers map[keeperv1.DriverType]snapshot.Driver
	Hooks   *hooks.Runner
	// CSIEnabled records whether the VolumeSnapshot API is served.
	CSIEnabled bool
	// MaxConcurrentReconciles bounds parallel Backups (hooks run inline).
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=backup.keeper.dev,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.keeper.dev,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.keeper.dev,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create;get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;delete;deletecollection
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile implements reconcile.Reconciler.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var b keeperv1.Backup
	if err := r.APIReader.Get(ctx, req.NamespacedName, &b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !b.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &b)
	}
	if controllerutil.AddFinalizer(&b, keeperv1.FinalizerBackupCleanup) {
		return ctrl.Result{}, r.Update(ctx, &b)
	}
	if b.Status.Phase.Terminal() {
		return ctrl.Result{}, nil
	}

	orig := b.DeepCopy()
	res, stepErr := r.step(ctx, &b)
	b.Status.ObservedGeneration = b.Generation
	conflict, err := r.patchStatus(ctx, &b, orig, !equality.Semantic.DeepEqual(orig.Status, b.Status))
	if err != nil {
		return ctrl.Result{}, errors.Join(stepErr, err)
	}
	if conflict {
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	return res, stepErr
}

func (r *BackupReconciler) step(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	switch b.Status.Phase {
	case "", keeperv1.BackupPending:
		return r.start(ctx, b)
	case keeperv1.BackupPreHooks:
		return r.preHooks(ctx, b)
	case keeperv1.BackupSnapshotting:
		return r.snapshot(ctx, b)
	case keeperv1.BackupPostHooks:
		return r.postHooks(ctx, b)
	case keeperv1.BackupFinalizing:
		return r.finish(ctx, b)
	}
	return ctrl.Result{}, fmt.Errorf("backup %s: unknown phase %q", b.Name, b.Status.Phase)
}

func (r *BackupReconciler) driver(b *keeperv1.Backup) (snapshot.Driver, error) {
	if b.Spec.Driver.Type == keeperv1.DriverCSISnapshot && !r.CSIEnabled {
		return nil, errors.New("the snapshot.storage.k8s.io/v1 VolumeSnapshot API is not installed in this cluster")
	}
	return snapshot.For(b.Spec.Driver.Type, r.Drivers)
}

func (r *BackupReconciler) start(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	if b.Status.StartTime == nil {
		b.Status.StartTime = r.metaNow()
	}
	b.Status.Phase = keeperv1.BackupPending
	if _, err := r.driver(b); err != nil {
		return r.fail(ctx, b, "DriverUnavailable", err.Error())
	}
	if _, err := r.Stores.Open(ctx, b.Namespace, b.Spec.Storage); err != nil {
		if isTransient(err) {
			return ctrl.Result{}, err
		}
		return r.fail(ctx, b, "StorageUnavailable", err.Error())
	}
	vols, reason, err := r.resolveVolumes(ctx, b)
	if err != nil {
		if reason != "" {
			return r.fail(ctx, b, reason, err.Error())
		}
		return ctrl.Result{}, err
	}
	b.Status.Volumes = vols
	b.Status.Phase = keeperv1.BackupPreHooks
	if b.Spec.Hooks == nil || len(b.Spec.Hooks.Pre) == 0 {
		b.Status.Phase = keeperv1.BackupSnapshotting
	}
	r.normal(b, "Started", "Backing up %d volume(s) with the %s driver", len(vols), b.Spec.Driver.Type)
	return ctrl.Result{}, nil
}

// isTransient reports API errors worth retrying; everything else (missing or
// malformed Secrets, invalid endpoints) fails the Backup.
func isTransient(err error) bool {
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		return !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) && !apierrors.IsInvalid(err)
	}
	return false
}

// resolveVolumes turns spec.source into per-claim status entries. A non-empty
// reason marks the error as a permanent failure of this Backup.
func (r *BackupReconciler) resolveVolumes(ctx context.Context, b *keeperv1.Backup) ([]keeperv1.VolumeBackup, string, error) {
	var claims []corev1.PersistentVolumeClaim
	src := b.Spec.Source
	if src.PVCSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(src.PVCSelector)
		if err != nil {
			return nil, "InvalidSelector", err
		}
		var list corev1.PersistentVolumeClaimList
		if err := r.List(ctx, &list, client.InNamespace(b.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
			return nil, "", err
		}
		claims = list.Items
	} else {
		for _, name := range src.PVCNames {
			var pvc corev1.PersistentVolumeClaim
			if err := r.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, &pvc); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, "VolumeNotFound", fmt.Errorf("claim %s not found", name)
				}
				return nil, "", err
			}
			claims = append(claims, pvc)
		}
	}
	if len(claims) == 0 {
		return nil, "NoVolumes", errors.New("no claims match the source")
	}
	slices.SortFunc(claims, func(a, c corev1.PersistentVolumeClaim) int { return compareStrings(a.Name, c.Name) })
	vols := make([]keeperv1.VolumeBackup, 0, len(claims))
	for _, c := range claims {
		if c.Status.Phase != corev1.ClaimBound {
			return nil, "VolumeNotBound", fmt.Errorf("claim %s is %s, not Bound", c.Name, phaseOr(c.Status.Phase))
		}
		v := keeperv1.VolumeBackup{PVCName: c.Name, AccessModes: c.Spec.AccessModes, VolumeMode: c.Spec.VolumeMode}
		if q, ok := c.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			v.Capacity = q
		}
		if c.Spec.StorageClassName != nil {
			v.StorageClassName = *c.Spec.StorageClassName
		}
		vols = append(vols, v)
	}
	return vols, "", nil
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func phaseOr(p corev1.PersistentVolumeClaimPhase) string {
	if p == "" {
		return "Pending"
	}
	return string(p)
}

func (r *BackupReconciler) runHooks(ctx context.Context, b *keeperv1.Backup, stage hooks.Stage) hooks.Outcome {
	var hs []keeperv1.Hook
	if b.Spec.Hooks != nil {
		hs = b.Spec.Hooks.Pre
		if stage == hooks.StagePost {
			hs = b.Spec.Hooks.Post
		}
	}
	out := r.Hooks.Run(ctx, b.Namespace, stage, hs)
	for _, res := range out.Results {
		result := "success"
		if !res.Succeeded {
			result = "failure"
		}
		metrics.HookExecutions.WithLabelValues(string(stage), result).Inc()
	}
	b.Status.Hooks = append(b.Status.Hooks, out.Results...)
	return out
}

func (r *BackupReconciler) preHooks(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	out := r.runHooks(ctx, b, hooks.StagePre)
	if out.Failed {
		setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionPreHooksSucceeded, false, "HookFailed", out.Message)
		r.resume(ctx, b)
		return r.fail(ctx, b, "PreHookFailed", out.Message)
	}
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionPreHooksSucceeded, true, "HooksSucceeded", fmt.Sprintf("%d pre hook execution(s) succeeded", len(out.Results)))
	b.Status.Phase = keeperv1.BackupSnapshotting
	return ctrl.Result{}, nil
}

// resume runs post hooks after a failure so an application frozen by pre
// hooks is never left frozen. Their outcome cannot rescue the Backup.
func (r *BackupReconciler) resume(ctx context.Context, b *keeperv1.Backup) {
	if b.Spec.Hooks == nil || len(b.Spec.Hooks.Post) == 0 {
		return
	}
	if out := r.runHooks(ctx, b, hooks.StagePost); out.Failed {
		setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionPostHooksSucceeded, false, "HookFailed", out.Message)
		r.warning(b, "PostHookFailed", "%s", out.Message)
	}
}

func (r *BackupReconciler) snapshot(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	d, err := r.driver(b)
	if err != nil {
		r.resume(ctx, b)
		return r.fail(ctx, b, "DriverUnavailable", err.Error())
	}
	store, err := r.Stores.Open(ctx, b.Namespace, b.Spec.Storage)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := d.Start(ctx, b); err != nil {
		if apierrors.IsInvalid(err) || apierrors.IsForbidden(err) {
			r.resume(ctx, b)
			return r.fail(ctx, b, "DriverError", err.Error())
		}
		return ctrl.Result{}, err
	}
	p, err := d.Observe(ctx, b, store)
	if err != nil {
		return ctrl.Result{}, err
	}
	if p.Failed {
		setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionSnapshotTaken, false, "SnapshotFailed", p.Message)
		r.resume(ctx, b)
		return r.fail(ctx, b, "SnapshotFailed", p.Message)
	}
	b.Status.Volumes = p.Volumes
	if !p.Cut {
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	cut := p.CutTime
	if cut.IsZero() {
		cut = r.now()
	}
	b.Status.SnapshotTime = &metav1.Time{Time: cut}
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionSnapshotTaken, true, "SnapshotCut", "point-in-time copy of every volume exists")
	r.normal(b, "SnapshotTaken", "Point-in-time copy cut for %d volume(s)", len(p.Volumes))
	b.Status.Phase = keeperv1.BackupPostHooks
	if b.Spec.Hooks == nil || len(b.Spec.Hooks.Post) == 0 {
		b.Status.Phase = keeperv1.BackupFinalizing
	}
	return ctrl.Result{}, nil
}

func (r *BackupReconciler) postHooks(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	out := r.runHooks(ctx, b, hooks.StagePost)
	if out.Failed {
		setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionPostHooksSucceeded, false, "HookFailed", out.Message)
		return r.fail(ctx, b, "PostHookFailed", out.Message)
	}
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionPostHooksSucceeded, true, "HooksSucceeded", fmt.Sprintf("%d post hook execution(s) succeeded", len(out.Results)))
	b.Status.Phase = keeperv1.BackupFinalizing
	return ctrl.Result{}, nil
}

func (r *BackupReconciler) finish(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	d, err := r.driver(b)
	if err != nil {
		return r.fail(ctx, b, "DriverUnavailable", err.Error())
	}
	store, err := r.Stores.Open(ctx, b.Namespace, b.Spec.Storage)
	if err != nil {
		return ctrl.Result{}, err
	}
	p, err := d.Observe(ctx, b, store)
	if err != nil {
		return ctrl.Result{}, err
	}
	if p.Failed {
		return r.fail(ctx, b, "SnapshotFailed", p.Message)
	}
	b.Status.Volumes = p.Volumes
	if !p.Done {
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	var total int64
	for _, v := range p.Volumes {
		total += v.SizeBytes
	}
	b.Status.SizeBytes = total
	key := storage.Layout{Prefix: b.Spec.Storage.Prefix}.BackupManifest(b.Namespace, b.Name)
	manifest := BackupManifest{
		Version: BackupManifestVersion, Namespace: b.Namespace, Name: b.Name, UID: b.UID,
		Policy: b.Labels[keeperv1.LabelPolicy], Driver: b.Spec.Driver.Type,
		SnapshotTime: b.Status.SnapshotTime.UTC(), Volumes: p.Volumes, Hooks: b.Status.Hooks,
	}
	if err := storage.PutJSON(ctx, store, key, manifest); err != nil {
		return ctrl.Result{}, err
	}
	b.Status.ManifestKey = key
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionDataStored, true, "ManifestWritten", "data and manifest are in object storage")
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionReady, true, "Completed", "backup completed")
	b.Status.Phase = keeperv1.BackupCompleted
	b.Status.CompletionTime = r.metaNow()
	b.Status.Message = ""
	driver := string(b.Spec.Driver.Type)
	metrics.BackupsTotal.WithLabelValues(driver, "completed").Inc()
	metrics.BackupBytes.WithLabelValues(driver).Add(float64(total))
	metrics.BackupDuration.WithLabelValues(driver).Observe(b.Status.CompletionTime.Sub(b.Status.StartTime.Time).Seconds())
	r.normal(b, "Completed", "Backup completed: %d volume(s), %d bytes", len(p.Volumes), total)
	return ctrl.Result{}, nil
}

func (r *BackupReconciler) fail(ctx context.Context, b *keeperv1.Backup, reason, msg string) (ctrl.Result, error) {
	b.Status.Phase = keeperv1.BackupFailed
	b.Status.Message = msg
	b.Status.CompletionTime = r.metaNow()
	if b.Status.StartTime == nil {
		b.Status.StartTime = b.Status.CompletionTime
	}
	setCondition(&b.Status.Conditions, b.Generation, keeperv1.ConditionReady, false, reason, msg)
	metrics.BackupsTotal.WithLabelValues(string(b.Spec.Driver.Type), "failed").Inc()
	r.warning(b, reason, "Backup failed: %s", msg)
	log.FromContext(ctx).Info("backup failed", "reason", reason, "message", msg)
	return ctrl.Result{}, nil
}

// hooksInFlight reports phases in which pre hooks may already have quiesced
// the application while post hooks have not yet succeeded. PreHooks counts
// even though the freeze may not have run yet: the freeze can have run with
// its status write lost (a deletion bumps resourceVersion, so the
// optimistic-lock patch conflicts). Post hooks must tolerate running without
// a matching pre hook (see DECISIONS.md).
func hooksInFlight(p keeperv1.BackupPhase) bool {
	switch p {
	case keeperv1.BackupPreHooks, keeperv1.BackupSnapshotting, keeperv1.BackupPostHooks:
		return true
	}
	return false
}

// finalize releases stored data and driver objects, then the finalizer.
func (r *BackupReconciler) finalize(ctx context.Context, b *keeperv1.Backup) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(b, keeperv1.FinalizerBackupCleanup) {
		return ctrl.Result{}, nil
	}
	if hooksInFlight(b.Status.Phase) {
		// Deleted while the application may be quiesced: resume it first,
		// then persist a terminal phase so later finalize passes (blocked
		// by a Restore or a storage outage) do not rerun the post hooks.
		// A conflict on that write reruns them, which is the documented
		// at-least-once contract.
		orig := b.DeepCopy()
		r.resume(ctx, b)
		if _, err := r.fail(ctx, b, "DeletedInProgress", fmt.Sprintf("deleted during %s; post hooks were run to resume the application", orig.Status.Phase)); err != nil {
			return ctrl.Result{}, err
		}
		conflict, err := r.patchStatus(ctx, b, orig, true)
		if err != nil {
			return ctrl.Result{}, err
		}
		if conflict {
			return ctrl.Result{RequeueAfter: r.poll()}, nil
		}
	}
	var restores keeperv1.RestoreList
	if err := r.List(ctx, &restores, client.InNamespace(b.Namespace), client.MatchingFields{IndexRestoreBackup: b.Name}); err != nil {
		return ctrl.Result{}, err
	}
	for _, rs := range restores.Items {
		if !rs.Status.Phase.Terminal() {
			r.warning(b, "DeletionBlocked", "Restore %s is still reading this backup", rs.Name)
			return ctrl.Result{RequeueAfter: r.poll()}, nil
		}
	}
	if b.Spec.DeletionPolicy != keeperv1.DeletionRetain {
		if err := r.cleanup(ctx, b); err != nil {
			r.warning(b, "CleanupFailed", "Could not delete backup data: %v", err)
			return ctrl.Result{}, err
		}
	}
	controllerutil.RemoveFinalizer(b, keeperv1.FinalizerBackupCleanup)
	return ctrl.Result{}, r.Update(ctx, b)
}

func (r *BackupReconciler) cleanup(ctx context.Context, b *keeperv1.Backup) error {
	if d, err := r.driver(b); err == nil {
		if err := d.Cleanup(ctx, b); err != nil {
			return err
		}
	}
	if b.Annotations[keeperv1.AnnotationSkipStorageCleanup] == "true" {
		return nil
	}
	store, err := r.Stores.Open(ctx, b.Namespace, b.Spec.Storage)
	if err != nil {
		return err
	}
	n, err := storage.DeletePrefix(ctx, store, storage.Layout{Prefix: b.Spec.Storage.Prefix}.BackupDir(b.Namespace, b.Name))
	if err != nil {
		return err
	}
	r.normal(b, "DataDeleted", "Deleted %d object(s) from storage", n)
	return nil
}

// SetupWithManager registers the reconciler. VolumeSnapshots are only
// watched when the API exists, so clusters without CSI snapshots still work.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&keeperv1.Backup{}).
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: max(r.MaxConcurrentReconciles, 1)}).
		Named("backup")
	if r.CSIEnabled {
		b = b.Watches(&snapv1.VolumeSnapshot{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			name := o.GetLabels()[keeperv1.LabelBackup]
			if name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: name}}}
		}))
	}
	return b.Complete(r)
}
