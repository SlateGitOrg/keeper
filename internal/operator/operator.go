// Package operator wires the reconcilers, drivers and collaborators into a
// controller-runtime manager. The binary, the integration tests and the demo
// all build the operator through Setup, so they exercise identical wiring.
package operator

import (
	"context"
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/controller"
	"github.com/archit2501/keeper/internal/hooks"
	"github.com/archit2501/keeper/internal/snapshot"
	"github.com/archit2501/keeper/internal/storage"
)

// EventSource is the reporting controller name on emitted events.
const EventSource = "keeper-operator"

// NewScheme registers built-in, keeper and VolumeSnapshot types.
func NewScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, keeperv1.AddToScheme, snapv1.AddToScheme} {
		if err := add(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Options configures Setup.
type Options struct {
	// Stores opens object storage (SecretFactory in production).
	Stores storage.Factory
	// Exec runs hook commands (RemoteExecutor in production).
	Exec hooks.Executor
	// Clock is the time source (real clock if nil).
	Clock clock.PassiveClock
	// MoverImage is the image mover Jobs run.
	MoverImage string
	// PollInterval is the reconcile safety-net while waiting on progress.
	PollInterval time.Duration
	// MaxRequeue caps BackupPolicy wake-ups.
	MaxRequeue time.Duration
	// MaxConcurrentBackups bounds parallel Backup reconciles.
	MaxConcurrentBackups int
	// CSI forces VolumeSnapshot support on or off; nil auto-detects.
	CSI *bool
}

// DetectCSI reports whether the API server serves VolumeSnapshots.
func DetectCSI(mapper meta.RESTMapper) bool {
	_, err := mapper.RESTMapping(snapv1.SchemeGroupVersion.WithKind("VolumeSnapshot").GroupKind(), snapv1.SchemeGroupVersion.Version)
	return err == nil
}

// Setup registers indexes and reconcilers with mgr.
func Setup(ctx context.Context, mgr ctrl.Manager, o Options) error {
	if o.Stores == nil || o.Exec == nil {
		return fmt.Errorf("operator: Stores and Exec are required")
	}
	clk := o.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}
	csi := DetectCSI(mgr.GetRESTMapper())
	if o.CSI != nil {
		csi = *o.CSI
	}
	base := controller.Base{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), Clock: clk, PollInterval: o.PollInterval,
		Recorder: mgr.GetEventRecorder(EventSource),
	}
	deps := snapshot.Deps{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Now: clk.Now}
	drivers := map[keeperv1.DriverType]snapshot.Driver{
		keeperv1.DriverCSISnapshot: &snapshot.CSI{Deps: deps},
		keeperv1.DriverFileCopy:    &snapshot.FileCopy{Deps: deps, Image: o.MoverImage},
	}
	if err := controller.SetupIndexes(ctx, mgr); err != nil {
		return fmt.Errorf("operator: indexes: %w", err)
	}
	br := &controller.BackupReconciler{
		Base: base, Stores: o.Stores, Drivers: drivers, CSIEnabled: csi,
		Hooks:                   &hooks.Runner{Pods: mgr.GetClient(), Exec: o.Exec, Now: clk.Now},
		MaxConcurrentReconciles: o.MaxConcurrentBackups,
	}
	if err := br.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("operator: backup controller: %w", err)
	}
	pr := &controller.BackupPolicyReconciler{Base: base, MaxRequeue: o.MaxRequeue}
	if err := pr.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("operator: backuppolicy controller: %w", err)
	}
	rr := &controller.RestoreReconciler{Base: base, Drivers: drivers, CSIEnabled: csi}
	if err := rr.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("operator: restore controller: %w", err)
	}
	return nil
}
