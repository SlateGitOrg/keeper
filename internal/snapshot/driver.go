// Package snapshot defines the driver abstraction that cuts point-in-time
// copies of PersistentVolumeClaims and materialises them again, plus two
// drivers: CSI (VolumeSnapshot objects) and FileCopy (mover Jobs that stream
// volume contents to object storage).
//
// Drivers are level-triggered: Start/StartRestore create whatever is missing
// and tolerate objects that already exist, Observe/ObserveRestore only read.
// The Backup and Restore controllers call them on every reconcile, so a
// restarted operator picks up exactly where it left off.
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/SlateGitOrg/keeper/api/v1alpha1"
	"github.com/SlateGitOrg/keeper/internal/storage"
)

// Progress is a driver's view of a Backup.
type Progress struct {
	// Cut is true once every volume's point-in-time copy exists, i.e. the
	// application may be resumed (post hooks may run).
	Cut bool
	// CutTime is when the last volume was cut.
	CutTime time.Time
	// Done is true once every copy is durable and usable for restore.
	Done bool
	// Failed is terminal; Message explains it.
	Failed  bool
	Message string
	// Volumes carries updated per-volume status, in input order.
	Volumes []keeperv1.VolumeBackup
}

// RestoreVolume pairs a backed-up volume with its target claim name.
type RestoreVolume struct {
	Source keeperv1.VolumeBackup
	Target string
}

// RestoreProgress is a driver's view of a Restore.
type RestoreProgress struct {
	Done    bool
	Failed  bool
	Message string
	// Ready lists target claims whose data is in place.
	Ready map[string]bool
	// Jobs maps target claims to the mover Job populating them, if any.
	Jobs map[string]string
}

// Driver cuts and restores point-in-time copies.
type Driver interface {
	// Start creates the driver objects for every volume of b.
	Start(ctx context.Context, b *keeperv1.Backup) error
	// Observe reports progress; store holds per-volume manifests.
	Observe(ctx context.Context, b *keeperv1.Backup, store storage.ObjectStore) (Progress, error)
	// Cleanup deletes driver objects (not stored data) belonging to b.
	Cleanup(ctx context.Context, b *keeperv1.Backup) error
	// StartRestore creates target claims (and whatever populates them).
	StartRestore(ctx context.Context, r *keeperv1.Restore, b *keeperv1.Backup, vols []RestoreVolume) error
	// ObserveRestore reports restore progress.
	ObserveRestore(ctx context.Context, r *keeperv1.Restore, b *keeperv1.Backup, vols []RestoreVolume) (RestoreProgress, error)
}

// Deps are the collaborators shared by drivers.
type Deps struct {
	Client client.Client
	Scheme *runtime.Scheme
	// Now is the clock used for timeouts.
	Now func() time.Time
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// For returns the driver for t, or an error if t is unknown.
func For(t keeperv1.DriverType, drivers map[keeperv1.DriverType]Driver) (Driver, error) {
	d, ok := drivers[t]
	if !ok {
		return nil, fmt.Errorf("snapshot: no driver registered for %q", t)
	}
	return d, nil
}

// maxNameLen keeps generated names valid as label values and Job names.
const maxNameLen = 63

// ChildName joins parts with "-" into a DNS-1123 label of at most 63
// characters. Over-long names are truncated and suffixed with a hash of the
// full name, so distinct inputs stay distinct.
func ChildName(parts ...string) string {
	full := strings.Join(parts, "-")
	if len(full) <= maxNameLen {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	suffix := hex.EncodeToString(sum[:])[:8]
	head := strings.TrimRight(full[:maxNameLen-len(suffix)-1], "-.")
	return head + "-" + suffix
}

// jobState classifies a Job by its terminal conditions.
func jobState(j *batchv1.Job) (done, failed bool, msg string) {
	for _, c := range j.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, false, ""
		case batchv1.JobFailed:
			return false, true, fmt.Sprintf("job %s failed: %s: %s", j.Name, c.Reason, c.Message)
		}
	}
	return false, false, ""
}
