package snapshot

import (
	"context"
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/storage"
)

// CSI cuts one VolumeSnapshot per claim. The snapshot data stays in the
// storage system; only the backup manifest goes to object storage. Snapshots
// deliberately carry no owner reference: with DeletionPolicy=Retain they must
// outlive the Backup, and with Delete the Backup finalizer removes them.
type CSI struct {
	Deps
}

// ComponentSnapshot labels VolumeSnapshots created by the operator.
const ComponentSnapshot = "snapshot"

// VolumeSnapshotName is the VolumeSnapshot name for a volume of a Backup.
func VolumeSnapshotName(backup, pvc string) string { return ChildName(backup, pvc) }

func csiTimeout(b *keeperv1.Backup) time.Duration {
	if c := b.Spec.Driver.CSI; c != nil && c.ReadyTimeoutSeconds > 0 {
		return time.Duration(c.ReadyTimeoutSeconds) * time.Second
	}
	return 10 * time.Minute
}

// Start implements Driver.
func (d *CSI) Start(ctx context.Context, b *keeperv1.Backup) error {
	for _, v := range b.Status.Volumes {
		vs := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      VolumeSnapshotName(b.Name, v.PVCName),
				Namespace: b.Namespace,
				Labels:    map[string]string{keeperv1.LabelBackup: b.Name, keeperv1.LabelComponent: ComponentSnapshot},
			},
			Spec: snapv1.VolumeSnapshotSpec{Source: snapv1.VolumeSnapshotSource{PersistentVolumeClaimName: &v.PVCName}},
		}
		if c := b.Spec.Driver.CSI; c != nil && c.VolumeSnapshotClassName != "" {
			vs.Spec.VolumeSnapshotClassName = &c.VolumeSnapshotClassName
		}
		if err := d.Client.Create(ctx, vs); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create VolumeSnapshot %s: %w", vs.Name, err)
			}
			if err := d.verifyOwned(ctx, b, vs.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *CSI) verifyOwned(ctx context.Context, b *keeperv1.Backup, name string) error {
	var existing snapv1.VolumeSnapshot
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, &existing); err != nil {
		return fmt.Errorf("get VolumeSnapshot %s: %w", name, err)
	}
	if existing.Labels[keeperv1.LabelBackup] != b.Name {
		return fmt.Errorf("VolumeSnapshot %s already exists and does not belong to backup %s", name, b.Name)
	}
	return nil
}

// Observe implements Driver.
func (d *CSI) Observe(ctx context.Context, b *keeperv1.Backup, _ storage.ObjectStore) (Progress, error) {
	p := Progress{Cut: true, Done: true}
	for _, v := range b.Status.Volumes {
		name := VolumeSnapshotName(b.Name, v.PVCName)
		v.SnapshotName = name
		var vs snapv1.VolumeSnapshot
		err := d.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, &vs)
		if err != nil && !apierrors.IsNotFound(err) {
			return Progress{}, err
		}
		// NotFound: the cache has not seen Start's create yet (Start
		// re-creates a snapshot that is really gone); treat as not cut.
		st := vs.Status
		if st != nil && st.Error != nil && st.Error.Message != nil {
			return Progress{Failed: true, Message: fmt.Sprintf("VolumeSnapshot %s: %s", name, *st.Error.Message)}, nil
		}
		if st == nil || st.CreationTime == nil {
			p.Cut, p.Done = false, false
		} else if st.CreationTime.After(p.CutTime) {
			p.CutTime = st.CreationTime.Time
		}
		if st != nil && st.ReadyToUse != nil && *st.ReadyToUse {
			v.Ready = true
			if st.RestoreSize != nil {
				v.SizeBytes = st.RestoreSize.Value()
			}
		} else {
			p.Done = false
		}
		p.Volumes = append(p.Volumes, v)
	}
	if !p.Done && b.Status.StartTime != nil && d.now().Sub(b.Status.StartTime.Time) > csiTimeout(b) {
		return Progress{Failed: true, Message: fmt.Sprintf("VolumeSnapshots not ready after %s", csiTimeout(b))}, nil
	}
	return p, nil
}

// Cleanup implements Driver.
func (d *CSI) Cleanup(ctx context.Context, b *keeperv1.Backup) error {
	err := d.Client.DeleteAllOf(ctx, &snapv1.VolumeSnapshot{}, client.InNamespace(b.Namespace),
		client.MatchingLabels{keeperv1.LabelBackup: b.Name, keeperv1.LabelComponent: ComponentSnapshot})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete VolumeSnapshots of %s: %w", b.Name, err)
	}
	return nil
}

// StartRestore implements Driver: each target claim is provisioned from its
// VolumeSnapshot by the CSI driver (dataSource).
func (d *CSI) StartRestore(ctx context.Context, r *keeperv1.Restore, b *keeperv1.Backup, vols []RestoreVolume) error {
	for _, rv := range vols {
		pvc := targetClaim(r, rv)
		// A snapshot's restoreSize can exceed the source request (e.g. after
		// the source was expanded); the new claim must be at least that big.
		if size := resource.NewQuantity(rv.Source.SizeBytes, resource.BinarySI); size.Cmp(rv.Source.Capacity) > 0 {
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *size
		}
		pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{
			APIGroup: &snapv1.SchemeGroupVersion.Group,
			Kind:     "VolumeSnapshot",
			Name:     VolumeSnapshotName(b.Name, rv.Source.PVCName),
		}
		if err := createClaim(ctx, d.Client, r, pvc); err != nil {
			return err
		}
	}
	return nil
}

// ObserveRestore implements Driver. A claim provisioned from a snapshot is
// usable as soon as it exists; binding may be deferred to the first consumer.
func (d *CSI) ObserveRestore(ctx context.Context, r *keeperv1.Restore, _ *keeperv1.Backup, vols []RestoreVolume) (RestoreProgress, error) {
	p := RestoreProgress{Done: true, Ready: map[string]bool{}}
	for _, rv := range vols {
		var pvc corev1.PersistentVolumeClaim
		err := d.Client.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: rv.Target}, &pvc)
		switch {
		case apierrors.IsNotFound(err):
			p.Done = false
		case err != nil:
			return RestoreProgress{}, err
		case pvc.Status.Phase == corev1.ClaimLost:
			return RestoreProgress{Failed: true, Message: fmt.Sprintf("claim %s is Lost", rv.Target)}, nil
		default:
			p.Ready[rv.Target] = true
		}
	}
	return p, nil
}

// targetClaim builds the restored claim from what the Backup recorded.
func targetClaim(r *keeperv1.Restore, rv RestoreVolume) *corev1.PersistentVolumeClaim {
	src := rv.Source
	modes := src.AccessModes
	if len(modes) == 0 {
		modes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rv.Target,
			Namespace: r.Namespace,
			Labels:    map[string]string{keeperv1.LabelRestore: r.Name},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: modes,
			VolumeMode:  src.VolumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: src.Capacity},
			},
		},
	}
	if src.StorageClassName != "" {
		sc := src.StorageClassName
		pvc.Spec.StorageClassName = &sc
	}
	if r.Spec.StorageClassName != nil {
		pvc.Spec.StorageClassName = r.Spec.StorageClassName
	}
	return pvc
}

// createClaim creates pvc unless this Restore already did; a pre-existing
// claim that belongs to someone else is never adopted or overwritten.
func createClaim(ctx context.Context, c client.Client, r *keeperv1.Restore, pvc *corev1.PersistentVolumeClaim) error {
	err := c.Create(ctx, pvc)
	if err == nil || !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("create claim %s: %w", pvc.Name, err)
		}
		return nil
	}
	var existing corev1.PersistentVolumeClaim
	if err := c.Get(ctx, client.ObjectKeyFromObject(pvc), &existing); err != nil {
		return fmt.Errorf("get claim %s: %w", pvc.Name, err)
	}
	if existing.Labels[keeperv1.LabelRestore] != r.Name {
		return &ConflictError{Claim: pvc.Name}
	}
	return nil
}

// ConflictError reports a target claim that already exists and is not ours.
type ConflictError struct{ Claim string }

func (e *ConflictError) Error() string {
	return fmt.Sprintf("target claim %s already exists and was not created by this restore", e.Claim)
}
