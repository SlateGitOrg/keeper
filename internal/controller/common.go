// Package controller contains the reconcilers for BackupPolicy, Backup and
// Restore.
package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
)

// IndexRestoreBackup indexes Restores by spec.backupName.
const IndexRestoreBackup = ".spec.backupName"

// SetupIndexes registers the field indexes the reconcilers rely on. Call it
// once per manager, before adding the reconcilers.
func SetupIndexes(ctx context.Context, mgr manager.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &keeperv1.Restore{}, IndexRestoreBackup, func(o client.Object) []string {
		return []string{o.(*keeperv1.Restore).Spec.BackupName}
	})
}

// Base is the configuration shared by all reconcilers.
type Base struct {
	client.Client
	// APIReader reads straight from the API server, bypassing the cache. It
	// guards side effects (hooks) against stale cached objects.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
	Clock     clock.PassiveClock
	// PollInterval is the safety-net requeue while waiting on external
	// progress (snapshots, Jobs) that watches normally report.
	PollInterval time.Duration
}

func (b *Base) now() time.Time {
	if b.Clock == nil {
		return time.Now()
	}
	return b.Clock.Now()
}

func (b *Base) metaNow() *metav1.Time {
	t := metav1.NewTime(b.now())
	return &t
}

func (b *Base) poll() time.Duration {
	if b.PollInterval <= 0 {
		return 5 * time.Second
	}
	return b.PollInterval
}

func (b *Base) event(obj runtime.Object, typ, reason, action, note string, args ...any) {
	if b.Recorder != nil {
		b.Recorder.Eventf(obj, nil, typ, reason, action, note, args...)
	}
}

func (b *Base) normal(obj runtime.Object, reason, note string, args ...any) {
	b.event(obj, corev1.EventTypeNormal, reason, reason, note, args...)
}

func (b *Base) warning(obj runtime.Object, reason, note string, args ...any) {
	b.event(obj, corev1.EventTypeWarning, reason, reason, note, args...)
}

// patchStatus writes obj's status if it changed, with optimistic locking so
// a reconcile working from a stale cached object can never move status
// backwards. A conflict just means a newer version exists: requeue.
func (b *Base) patchStatus(ctx context.Context, obj, orig client.Object, changed bool) (bool, error) {
	if !changed {
		return false, nil
	}
	err := b.Status().Patch(ctx, obj, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	if apierrors.IsConflict(err) {
		return true, nil
	}
	return false, err
}

func setCondition(conds *[]metav1.Condition, gen int64, typ string, ok bool, reason, msg string) {
	st := metav1.ConditionFalse
	if ok {
		st = metav1.ConditionTrue
	}
	meta.SetStatusCondition(conds, metav1.Condition{Type: typ, Status: st, Reason: reason, Message: msg, ObservedGeneration: gen})
}
