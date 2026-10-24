package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/metrics"
	"github.com/archit2501/keeper/internal/retention"
	"github.com/archit2501/keeper/internal/schedule"
)

// BackupNameTimeFormat renders a schedule slot into Backup names (UTC).
const BackupNameTimeFormat = "200601021504"

// BackupPolicyReconciler creates scheduled Backups and applies retention.
//
// Backups carry the policy label but no owner reference: deleting a policy
// stops scheduling without cascading into deletion of every backup it made.
type BackupPolicyReconciler struct {
	Base
	// MaxRequeue caps the wait until the next slot so clock jumps and missed
	// events are corrected within this interval.
	MaxRequeue time.Duration
}

// +kubebuilder:rbac:groups=backup.keeper.dev,resources=backuppolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=backup.keeper.dev,resources=backuppolicies/status,verbs=get;update;patch

// Reconcile implements reconcile.Reconciler.
func (r *BackupPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var p keeperv1.BackupPolicy
	if err := r.Get(ctx, req.NamespacedName, &p); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !p.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	orig := p.DeepCopy()
	res, stepErr := r.reconcilePolicy(ctx, &p)
	p.Status.ObservedGeneration = p.Generation
	if !equality.Semantic.DeepEqual(orig.Status, p.Status) {
		if err := r.Status().Patch(ctx, &p, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, errors.Join(stepErr, err)
		}
	}
	return res, stepErr
}

func (r *BackupPolicyReconciler) reconcilePolicy(ctx context.Context, p *keeperv1.BackupPolicy) (ctrl.Result, error) {
	sched, err := schedule.Parse(p.Spec.Schedule, p.Spec.TimeZone)
	if err != nil {
		setCondition(&p.Status.Conditions, p.Generation, keeperv1.ConditionScheduleValid, false, "InvalidSchedule", err.Error())
		setCondition(&p.Status.Conditions, p.Generation, keeperv1.ConditionReady, false, "InvalidSchedule", err.Error())
		p.Status.NextScheduleTime = nil
		return ctrl.Result{}, nil // wait for a spec change
	}
	setCondition(&p.Status.Conditions, p.Generation, keeperv1.ConditionScheduleValid, true, "Parsed", "schedule is valid")

	var list keeperv1.BackupList
	if err := r.List(ctx, &list, client.InNamespace(p.Namespace), client.MatchingLabels{keeperv1.LabelPolicy: p.Name}); err != nil {
		return ctrl.Result{}, err
	}
	var active, completed, failed []keeperv1.Backup
	for _, b := range list.Items {
		switch {
		case !b.DeletionTimestamp.IsZero():
		case b.Status.Phase == keeperv1.BackupCompleted:
			completed = append(completed, b)
		case b.Status.Phase == keeperv1.BackupFailed:
			failed = append(failed, b)
		default:
			active = append(active, b)
		}
	}
	p.Status.Active = nil
	for _, b := range active {
		p.Status.Active = append(p.Status.Active, corev1.LocalObjectReference{Name: b.Name})
	}
	sort.Slice(p.Status.Active, func(i, j int) bool { return p.Status.Active[i].Name < p.Status.Active[j].Name })

	if err := r.applyRetention(ctx, p, sched.Location(), completed, failed); err != nil {
		return ctrl.Result{}, err
	}
	return r.scheduleNext(ctx, p, sched, len(active))
}

// backupTime is the instant retention buckets a Backup by.
func backupTime(b *keeperv1.Backup) time.Time {
	if b.Status.SnapshotTime != nil {
		return b.Status.SnapshotTime.Time
	}
	return b.CreationTimestamp.Time
}

func (r *BackupPolicyReconciler) applyRetention(ctx context.Context, p *keeperv1.BackupPolicy, loc *time.Location, completed, failed []keeperv1.Backup) error {
	rt := p.Spec.Retention
	pol := retention.Policy{
		Last: int(rt.KeepLast), Hourly: int(rt.KeepHourly), Daily: int(rt.KeepDaily),
		Weekly: int(rt.KeepWeekly), Monthly: int(rt.KeepMonthly), Yearly: int(rt.KeepYearly),
	}
	items := make([]retention.Item, 0, len(completed))
	byName := make(map[string]*keeperv1.Backup, len(completed))
	for i := range completed {
		b := &completed[i]
		byName[b.Name] = b
		items = append(items, retention.Item{ID: b.Name, Time: backupTime(b), Pinned: b.Labels[keeperv1.LabelPin] == "true"})
	}
	decisions, err := retention.Apply(pol, items, loc)
	if err != nil {
		return err
	}
	kept, pruned := retention.Split(decisions)
	for _, d := range kept {
		if err := r.annotateRetained(ctx, byName[d.ID], d.ReasonString()); err != nil {
			return err
		}
	}
	for _, d := range pruned {
		if err := r.deleteBackup(ctx, byName[d.ID]); err != nil {
			return err
		}
		metrics.RetentionPruned.WithLabelValues("completed").Inc()
		r.normal(p, "Pruned", "Deleted backup %s (snapshot %s) by retention", d.ID, d.Time.UTC().Format(time.RFC3339))
	}

	sort.Slice(failed, func(i, j int) bool { return failed[i].CreationTimestamp.After(failed[j].CreationTimestamp.Time) })
	nFailedPruned := 0
	for i := int(p.Spec.FailedBackupsHistoryLimit); i < len(failed); i++ {
		if err := r.deleteBackup(ctx, &failed[i]); err != nil {
			return err
		}
		nFailedPruned++
		metrics.RetentionPruned.WithLabelValues("failed").Inc()
	}

	p.Status.Retention.Kept = int32(len(kept)) //nolint:gosec // bounded by list size.
	if n := len(pruned) + nFailedPruned; n > 0 {
		p.Status.Retention.PrunedTotal += int64(n)
		p.Status.Retention.LastPruneTime = r.metaNow()
	}
	if len(kept) > 0 {
		newest := metav1.NewTime(kept[0].Time)
		p.Status.LastSuccessfulTime = &newest
	}
	return nil
}

func (r *BackupPolicyReconciler) annotateRetained(ctx context.Context, b *keeperv1.Backup, reasons string) error {
	if b.Annotations[keeperv1.AnnotationRetainedBy] == reasons {
		return nil
	}
	orig := b.DeepCopy()
	if b.Annotations == nil {
		b.Annotations = map[string]string{}
	}
	b.Annotations[keeperv1.AnnotationRetainedBy] = reasons
	if err := r.Patch(ctx, b, client.MergeFrom(orig)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("annotate %s: %w", b.Name, err)
	}
	return nil
}

func (r *BackupPolicyReconciler) deleteBackup(ctx context.Context, b *keeperv1.Backup) error {
	if err := r.Delete(ctx, b, client.Preconditions{UID: &b.UID}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("delete %s: %w", b.Name, err)
	}
	return nil
}

func (r *BackupPolicyReconciler) scheduleNext(ctx context.Context, p *keeperv1.BackupPolicy, sched *schedule.Schedule, active int) (ctrl.Result, error) {
	now := r.now()
	since := p.CreationTimestamp.Time
	if p.Status.LastScheduleTime != nil {
		since = p.Status.LastScheduleTime.Time
	}
	due, err := sched.Due(since, now)
	switch {
	case errors.Is(err, schedule.ErrTooManyMissed):
		r.warning(p, "TooManyMissedSchedules", "More than %d schedule slots were missed; skipping to now", schedule.MaxMissed)
		p.Status.LastScheduleTime = &metav1.Time{Time: now}
	case err != nil:
		return ctrl.Result{}, err
	case due.Slot.IsZero() || p.Spec.Suspend:
	case p.Spec.StartingDeadlineSeconds != nil && now.Sub(due.Slot) > time.Duration(*p.Spec.StartingDeadlineSeconds)*time.Second:
		r.warning(p, "MissedSchedule", "Skipped slot %s: more than %ds late", due.Slot.UTC().Format(time.RFC3339), *p.Spec.StartingDeadlineSeconds)
		p.Status.LastScheduleTime = &metav1.Time{Time: due.Slot}
	case active > 0 && p.Spec.ConcurrencyPolicy != keeperv1.ConcurrencyAllow:
		// Leave the slot unhandled; the Backup watch re-queues us when the
		// running Backup finishes.
		setCondition(&p.Status.Conditions, p.Generation, keeperv1.ConditionReady, true, "WaitingForActiveBackup", "a scheduled slot is waiting for the running backup")
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	default:
		name, err := r.createBackup(ctx, p, due.Slot)
		if err != nil {
			return ctrl.Result{}, err
		}
		p.Status.LastScheduleTime = &metav1.Time{Time: due.Slot}
		p.Status.Active = append(p.Status.Active, corev1.LocalObjectReference{Name: name})
		if due.Missed > 1 {
			r.warning(p, "MissedSchedule", "%d slots were missed; started only the most recent", due.Missed-1)
		}
	}

	reason, msg := "Scheduling", "backups are scheduled"
	if p.Spec.Suspend {
		reason, msg = "Suspended", "scheduling is suspended; retention still runs"
	}
	setCondition(&p.Status.Conditions, p.Generation, keeperv1.ConditionReady, true, reason, msg)

	next := sched.Next(now)
	if next.IsZero() {
		p.Status.NextScheduleTime = nil
		return ctrl.Result{RequeueAfter: r.maxRequeue()}, nil
	}
	p.Status.NextScheduleTime = &metav1.Time{Time: next}
	wait := next.Sub(now) + time.Second
	return ctrl.Result{RequeueAfter: min(wait, r.maxRequeue())}, nil
}

func (r *BackupPolicyReconciler) maxRequeue() time.Duration {
	if r.MaxRequeue <= 0 {
		return 5 * time.Minute
	}
	return r.MaxRequeue
}

// createBackup stamps the template into a Backup named after the slot. The
// name is deterministic, so a retry after a lost status update finds the
// existing Backup instead of creating a duplicate.
func (r *BackupPolicyReconciler) createBackup(ctx context.Context, p *keeperv1.BackupPolicy, slot time.Time) (string, error) {
	name := p.Name + "-" + slot.UTC().Format(BackupNameTimeFormat)
	labels := map[string]string{}
	for k, v := range p.Spec.Template.Labels {
		labels[k] = v
	}
	labels[keeperv1.LabelPolicy] = p.Name
	b := &keeperv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: p.Namespace, Labels: labels,
			Annotations: map[string]string{keeperv1.AnnotationScheduledTime: slot.UTC().Format(time.RFC3339)},
		},
		Spec: *p.Spec.Template.Spec.DeepCopy(),
	}
	if err := r.Create(ctx, b); err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.FromContext(ctx).V(1).Info("backup for slot already exists", "backup", name)
			return name, nil
		}
		r.warning(p, "BackupCreateFailed", "Could not create backup %s: %v", name, err)
		return "", fmt.Errorf("create backup %s: %w", name, err)
	}
	r.normal(p, "Scheduled", "Created backup %s for slot %s", name, slot.UTC().Format(time.RFC3339))
	return name, nil
}

// SetupWithManager registers the reconciler.
func (r *BackupPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&keeperv1.BackupPolicy{}).
		Watches(&keeperv1.Backup{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			name := o.GetLabels()[keeperv1.LabelPolicy]
			if name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: name}}}
		})).
		Named("backuppolicy").
		Complete(r)
}
