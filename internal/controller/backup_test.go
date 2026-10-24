package controller_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/controller"
	"github.com/archit2501/keeper/internal/hooks"
	"github.com/archit2501/keeper/internal/mover"
	"github.com/archit2501/keeper/internal/snapshot"
	"github.com/archit2501/keeper/internal/storage"
)

func condTrue(conds []metav1.Condition, typ string) bool {
	return meta.IsStatusConditionTrue(conds, typ)
}

func callsIn(ns string) []string {
	var out []string
	for _, c := range execFake.Calls() {
		if c.Namespace == ns {
			out = append(out, c.Pod+": "+strings.Join(c.Command, " "))
		}
	}
	return out
}

// TestFileCopyBackupLifecycle drives a FileCopy Backup with hooks from
// creation to Completed and checks every artefact the lifecycle produces.
func TestFileCopyBackupLifecycle(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	workload(t, ns, "data")

	// Record how many hook calls had happened when the mover ran: the copy
	// must happen strictly between pre and post hooks.
	callsAtCopy := make(chan int, 1)
	sim.FailJob = func(j *batchv1.Job) error {
		if j.Namespace == ns {
			callsAtCopy <- len(callsIn(ns))
		}
		return nil
	}
	t.Cleanup(func() { sim.FailJob = nil })

	spec := backupSpec(keeperv1.DriverFileCopy, "team-a", "data")
	spec.Hooks = pgHooks()
	createBackup(t, ns, "pg-manual", spec)
	b := waitBackup(t, ns, "pg-manual", keeperv1.BackupCompleted)

	if got := <-callsAtCopy; got != 1 {
		t.Fatalf("mover ran after %d hook calls, want 1 (pre only)", got)
	}
	if got := callsIn(ns); !slices.Equal(got, []string{"pg-0: fsfreeze -f /data", "pg-0: fsfreeze -u /data"}) {
		t.Fatalf("hook calls %v", got)
	}
	for _, c := range []string{keeperv1.ConditionReady, keeperv1.ConditionPreHooksSucceeded, keeperv1.ConditionSnapshotTaken, keeperv1.ConditionPostHooksSucceeded, keeperv1.ConditionDataStored} {
		if !condTrue(b.Status.Conditions, c) {
			t.Errorf("condition %s not True: %+v", c, b.Status.Conditions)
		}
	}
	if !slices.Contains(b.Finalizers, keeperv1.FinalizerBackupCleanup) {
		t.Error("finalizer missing")
	}
	if len(b.Status.Hooks) != 2 || b.Status.Hooks[0].Stage != "pre" || b.Status.Hooks[1].Stage != "post" || !b.Status.Hooks[1].Succeeded {
		t.Fatalf("hook results %+v", b.Status.Hooks)
	}
	v := b.Status.Volumes[0]
	if !v.Ready || v.SHA256 == "" || v.SizeBytes == 0 || v.Files != 3 || v.Capacity.String() != "1Gi" || v.StorageClassName != "sim-csi" {
		t.Fatalf("volume status %+v", v)
	}
	if b.Status.SizeBytes != v.SizeBytes || b.Status.SnapshotTime == nil || b.Status.CompletionTime == nil {
		t.Fatalf("status %+v", b.Status)
	}

	// The archive, volume manifest and backup manifest are in storage.
	l := storage.Layout{Prefix: "team-a"}
	if v.ObjectKey != l.Archive(ns, "pg-manual", "data") || b.Status.ManifestKey != l.BackupManifest(ns, "pg-manual") {
		t.Fatalf("keys %s %s", v.ObjectKey, b.Status.ManifestKey)
	}
	var m controller.BackupManifest
	must(t, storage.GetJSON(ctx, store(), b.Status.ManifestKey, &m))
	if m.Name != "pg-manual" || m.Driver != keeperv1.DriverFileCopy || len(m.Volumes) != 1 || m.Volumes[0].SHA256 != v.SHA256 || len(m.Hooks) != 2 {
		t.Fatalf("manifest %+v", m)
	}

	// The mover Job is hardened, co-located with the workload and owned by the Backup.
	var job batchv1.Job
	must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshot.BackupJobName("pg-manual", "data")}, &job))
	pod := job.Spec.Template.Spec
	ctr := pod.Containers[0]
	if !pod.Volumes[0].PersistentVolumeClaim.ReadOnly || !ctr.VolumeMounts[0].ReadOnly {
		t.Error("backup mover must mount the claim read-only")
	}
	if !*ctr.SecurityContext.ReadOnlyRootFilesystem || *ctr.SecurityContext.AllowPrivilegeEscalation || !slices.Equal(ctr.SecurityContext.Capabilities.Add, []corev1.Capability{"DAC_READ_SEARCH"}) {
		t.Errorf("security context %+v", ctr.SecurityContext)
	}
	if aff := pod.Affinity; aff == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "sim-node-1" {
		t.Errorf("mover not co-located with the workload: %+v", aff)
	}
	for _, e := range ctr.Env {
		if e.Name == mover.EnvSecretKey && (e.Value != "" || e.ValueFrom.SecretKeyRef.Name != "s3") {
			t.Errorf("secret must come from secretKeyRef: %+v", e)
		}
	}
	if owner := metav1.GetControllerOf(&job); owner == nil || owner.Name != "pg-manual" || owner.Kind != "Backup" {
		t.Errorf("job owner %+v", owner)
	}
	eventually(t, "lifecycle events", func() bool {
		r := eventReasons(t, ns, "pg-manual")
		return slices.Contains(r, "Started") && slices.Contains(r, "SnapshotTaken") && slices.Contains(r, "Completed")
	})
}

// TestFileCopyRestoreIsPointInTime restores a FileCopy Backup into a new claim
// and checks the data matches the volume as it was at backup time.
func TestFileCopyRestoreIsPointInTime(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	src := workload(t, ns, "data")
	createBackup(t, ns, "pg", backupSpec(keeperv1.DriverFileCopy, "", "data"))
	waitBackup(t, ns, "pg", keeperv1.BackupCompleted)

	// Change the source after the backup; the restore must not see this.
	must(t, os.WriteFile(filepath.Join(src, "pgdata", "PG_VERSION"), []byte("18\n"), 0o600))

	createRestore(t, ns, "pg-restore", "pg")
	r := waitRestore(t, ns, "pg-restore", keeperv1.RestoreCompleted)
	if len(r.Status.Volumes) != 1 || r.Status.Volumes[0].TargetPVC != "restored-data" || !r.Status.Volumes[0].Ready || r.Status.Volumes[0].JobName == "" {
		t.Fatalf("restore status %+v", r.Status.Volumes)
	}
	dir := sim.VolumeDir(ns, "restored-data")
	got, err := os.ReadFile(filepath.Join(dir, "pgdata", "PG_VERSION")) //nolint:gosec // test path
	must(t, err)
	if string(got) != "17\n" {
		t.Fatalf("restored PG_VERSION %q, want the backed-up 17", got)
	}
	var pvc corev1.PersistentVolumeClaim
	must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "restored-data"}, &pvc))
	if pvc.Labels[keeperv1.LabelRestore] != "pg-restore" || pvc.Spec.Resources.Requests.Storage().String() != "1Gi" || *pvc.Spec.StorageClassName != "sim-csi" {
		t.Fatalf("restored claim %+v", pvc.Spec)
	}
	var job batchv1.Job
	must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: r.Status.Volumes[0].JobName}, &job))
	if job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly || job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != "restored-data" {
		t.Fatal("restore mover must mount the target claim read-write")
	}
	if !hasEvent(t, ns, "pg-restore", "Completed") {
		t.Error("missing Completed event on restore")
	}

	t.Run("second restore into the same names is refused", func(t *testing.T) {
		createRestore(t, ns, "pg-restore-again", "pg")
		r := waitRestore(t, ns, "pg-restore-again", keeperv1.RestoreFailed)
		if !strings.Contains(r.Status.Message, "already exists") {
			t.Fatalf("message %q", r.Status.Message)
		}
	})
	t.Run("custom prefix, volume filter and storage class override", func(t *testing.T) {
		createRestore(t, ns, "pg-fast", "pg", func(r *keeperv1.Restore) {
			r.Spec.PVCNamePrefix = "fast-"
			r.Spec.Volumes = []string{"data"}
			r.Spec.StorageClassName = ptr.To("nvme")
		})
		waitRestore(t, ns, "pg-fast", keeperv1.RestoreCompleted)
		must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "fast-data"}, &pvc))
		if *pvc.Spec.StorageClassName != "nvme" {
			t.Fatalf("storage class %q", *pvc.Spec.StorageClassName)
		}
	})
}

// TestCSIBackupAndRestore covers the CSI driver: VolumeSnapshots are created,
// post hooks run once the snapshot is cut, the Backup completes when ready,
// and a Restore provisions a claim from the snapshot.
func TestCSIBackupAndRestore(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	src := workload(t, ns, "data")

	spec := backupSpec(keeperv1.DriverCSISnapshot, "", "data")
	spec.Driver.CSI = &keeperv1.CSIDriverSpec{VolumeSnapshotClassName: "fast-snaps"}
	spec.Hooks = pgHooks()
	createBackup(t, ns, "pg-csi", spec)
	b := waitBackup(t, ns, "pg-csi", keeperv1.BackupCompleted)

	var vs snapv1.VolumeSnapshot
	must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshot.SnapshotName("pg-csi", "data")}, &vs))
	if *vs.Spec.Source.PersistentVolumeClaimName != "data" || *vs.Spec.VolumeSnapshotClassName != "fast-snaps" || vs.Labels[keeperv1.LabelBackup] != "pg-csi" {
		t.Fatalf("volume snapshot %+v", vs)
	}
	if len(vs.OwnerReferences) != 0 {
		t.Fatal("VolumeSnapshots must not be owned by the Backup (Retain must be able to keep them)")
	}
	v := b.Status.Volumes[0]
	if v.SnapshotName != vs.Name || !v.Ready || v.SizeBytes != 1<<30 {
		t.Fatalf("volume status %+v", v)
	}
	if !b.Status.SnapshotTime.Equal(vs.Status.CreationTime) {
		t.Fatalf("snapshot time %v, VolumeSnapshot creationTime %v", b.Status.SnapshotTime, vs.Status.CreationTime)
	}
	if got := callsIn(ns); len(got) != 2 {
		t.Fatalf("hook calls %v", got)
	}
	var m controller.BackupManifest
	must(t, storage.GetJSON(ctx, store(), b.Status.ManifestKey, &m))
	if m.Driver != keeperv1.DriverCSISnapshot || m.Volumes[0].SnapshotName != vs.Name {
		t.Fatalf("manifest %+v", m)
	}

	must(t, os.WriteFile(filepath.Join(src, "pgdata", "PG_VERSION"), []byte("changed\n"), 0o600))
	createRestore(t, ns, "from-snap", "pg-csi")
	waitRestore(t, ns, "from-snap", keeperv1.RestoreCompleted)
	var pvc corev1.PersistentVolumeClaim
	must(t, k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "restored-data"}, &pvc))
	if ds := pvc.Spec.DataSource; ds == nil || ds.Kind != "VolumeSnapshot" || ds.Name != vs.Name || *ds.APIGroup != snapv1.GroupName {
		t.Fatalf("data source %+v", pvc.Spec.DataSource)
	}
	eventually(t, "restored claim to be provisioned from the snapshot", func() bool {
		b, err := os.ReadFile(filepath.Join(sim.VolumeDir(ns, "restored-data"), "pgdata", "PG_VERSION")) //nolint:gosec // test path
		return err == nil && string(b) == "17\n"
	})

	// Deleting the Backup deletes its VolumeSnapshots and manifest.
	must(t, k8s.Delete(ctx, getBackup(t, ns, "pg-csi")))
	waitGone(t, &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pg-csi"}})
	waitGone(t, &vs)
	if objs, _ := store().List(ctx, storage.Layout{}.BackupDir(ns, "pg-csi")); len(objs) != 0 {
		t.Fatalf("objects left: %v", objs)
	}
}

// TestPreHookFailureResumesAndFails checks the safety property of hooks: a
// failing pre hook fails the Backup without taking a copy, and post hooks
// still run so the application is not left frozen.
func TestPreHookFailureResumesAndFails(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	workload(t, ns, "data")
	execFake.Respond = func(req hooks.ExecRequest) (hooks.ExecResult, error) {
		if req.Namespace == ns && slices.Contains(req.Command, "-f") {
			return hooks.ExecResult{Stderr: "fsfreeze: /data: Operation not supported", ExitCode: 1}, fmt.Errorf("%w (code 1)", hooks.ErrNonZeroExit)
		}
		return hooks.ExecResult{}, nil
	}
	t.Cleanup(func() { execFake.Respond = nil })

	spec := backupSpec(keeperv1.DriverFileCopy, "", "data")
	spec.Hooks = pgHooks()
	createBackup(t, ns, "frozen", spec)
	b := waitBackup(t, ns, "frozen", keeperv1.BackupFailed)

	if got := callsIn(ns); !slices.Equal(got, []string{"pg-0: fsfreeze -f /data", "pg-0: fsfreeze -u /data"}) {
		t.Fatalf("post hook must run after a failed pre hook; calls %v", got)
	}
	c := meta.FindStatusCondition(b.Status.Conditions, keeperv1.ConditionReady)
	if c == nil || c.Reason != "PreHookFailed" || !strings.Contains(b.Status.Message, "Operation not supported") {
		t.Fatalf("ready condition %+v message %q", c, b.Status.Message)
	}
	if meta.IsStatusConditionTrue(b.Status.Conditions, keeperv1.ConditionPreHooksSucceeded) {
		t.Fatal("PreHooksSucceeded should be False")
	}
	var jobs batchv1.JobList
	must(t, k8s.List(ctx, &jobs, client.InNamespace(ns)))
	if len(jobs.Items) != 0 {
		t.Fatal("no copy may be taken after a failed pre hook")
	}
	eventually(t, "PreHookFailed event", func() bool { return hasEvent(t, ns, "frozen", "PreHookFailed") })
}

// TestBackupFailureModes covers the ways a Backup fails before or during the
// copy, each with a distinct reason.
func TestBackupFailureModes(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	workload(t, ns, "data")
	pending := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	sim.Pause(true)
	t.Cleanup(func() { sim.Pause(false) })
	must(t, k8s.Create(ctx, pending))

	check := func(name, reason, contains string) {
		t.Helper()
		b := waitBackup(t, ns, name, keeperv1.BackupFailed)
		c := meta.FindStatusCondition(b.Status.Conditions, keeperv1.ConditionReady)
		if c == nil || c.Reason != reason || !strings.Contains(b.Status.Message, contains) {
			t.Errorf("%s: condition %+v message %q, want reason %s containing %q", name, c, b.Status.Message, reason, contains)
		}
	}
	createBackup(t, ns, "missing-claim", backupSpec(keeperv1.DriverFileCopy, "", "nope"))
	check("missing-claim", "VolumeNotFound", "claim nope not found")

	createBackup(t, ns, "unbound", backupSpec(keeperv1.DriverFileCopy, "", "pending"))
	check("unbound", "VolumeNotBound", "not Bound")
	sim.Pause(false)

	sel := backupSpec(keeperv1.DriverFileCopy, "")
	sel.Source = keeperv1.VolumeSource{PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nothing"}}}
	createBackup(t, ns, "no-match", sel)
	check("no-match", "NoVolumes", "no claims match")

	noSecret := backupSpec(keeperv1.DriverFileCopy, "", "data")
	noSecret.Storage.CredentialsSecretRef.Name = "absent"
	createBackup(t, ns, "no-secret", noSecret)
	check("no-secret", "StorageUnavailable", "absent")

	sim.FailJob = func(j *batchv1.Job) error {
		if j.Namespace == ns {
			return errors.New("mover: open /data/pgdata/table: input/output error")
		}
		return nil
	}
	createBackup(t, ns, "io-error", backupSpec(keeperv1.DriverFileCopy, "", "data"))
	check("io-error", "SnapshotFailed", "input/output error")
	sim.FailJob = nil

	sim.FailSnapshot = func(vs *snapv1.VolumeSnapshot) string {
		if vs.Namespace == ns {
			return "rpc error: snapshot quota exceeded"
		}
		return ""
	}
	csi := backupSpec(keeperv1.DriverCSISnapshot, "", "data")
	csi.Hooks = pgHooks()
	createBackup(t, ns, "quota", csi)
	check("quota", "SnapshotFailed", "quota exceeded")
	sim.FailSnapshot = nil
	if got := callsIn(ns); !slices.Contains(got, "pg-0: fsfreeze -u /data") {
		t.Errorf("post hook must run after a snapshot failure; calls %v", got)
	}

	// A selector matching several claims backs them up in name order.
	_, err := sim.CreateClaim(ctx, ns, "logs", "2Gi", map[string]string{"app": "pg"})
	must(t, err)
	multi := backupSpec(keeperv1.DriverFileCopy, "")
	multi.Source = keeperv1.VolumeSource{PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pg"}}}
	createBackup(t, ns, "multi", multi)
	b := waitBackup(t, ns, "multi", keeperv1.BackupCompleted)
	if len(b.Status.Volumes) != 2 || b.Status.Volumes[0].PVCName != "data" || b.Status.Volumes[1].PVCName != "logs" {
		t.Fatalf("volumes %+v", b.Status.Volumes)
	}
}

// TestDeletion covers the finalizer: data is deleted with the Backup, kept
// with DeletionPolicy=Retain, deletion waits for running Restores and for
// storage to be reachable, and the skip annotation bypasses storage.
func TestDeletion(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()
	workload(t, ns, "data")
	dir := func(name string) string { return storage.Layout{}.BackupDir(ns, name) }
	count := func(name string) int {
		objs, err := store().List(ctx, dir(name))
		must(t, err)
		return len(objs)
	}

	t.Run("delete removes stored objects", func(t *testing.T) {
		createBackup(t, ns, "doomed", backupSpec(keeperv1.DriverFileCopy, "", "data"))
		waitBackup(t, ns, "doomed", keeperv1.BackupCompleted)
		if n := count("doomed"); n != 3 {
			t.Fatalf("expected archive + 2 manifests, got %d objects", n)
		}
		b := getBackup(t, ns, "doomed")
		must(t, k8s.Delete(ctx, b))
		waitGone(t, b)
		if n := count("doomed"); n != 0 {
			t.Fatalf("%d objects left", n)
		}
		eventually(t, "DataDeleted event", func() bool { return hasEvent(t, ns, "doomed", "DataDeleted") })
	})

	t.Run("retain keeps stored objects", func(t *testing.T) {
		spec := backupSpec(keeperv1.DriverFileCopy, "", "data")
		spec.DeletionPolicy = keeperv1.DeletionRetain
		createBackup(t, ns, "kept", spec)
		waitBackup(t, ns, "kept", keeperv1.BackupCompleted)
		b := getBackup(t, ns, "kept")
		must(t, k8s.Delete(ctx, b))
		waitGone(t, b)
		if n := count("kept"); n != 3 {
			t.Fatalf("Retain left %d objects, want 3", n)
		}
	})

	t.Run("deletion waits for a running restore", func(t *testing.T) {
		createBackup(t, ns, "in-use", backupSpec(keeperv1.DriverFileCopy, "", "data"))
		waitBackup(t, ns, "in-use", keeperv1.BackupCompleted)
		sim.Pause(true)
		t.Cleanup(func() { sim.Pause(false) })
		createRestore(t, ns, "reader", "in-use", func(r *keeperv1.Restore) { r.Spec.PVCNamePrefix = "reader-" })
		waitRestore(t, ns, "reader", keeperv1.RestoreRestoring)
		b := getBackup(t, ns, "in-use")
		must(t, k8s.Delete(ctx, b))
		eventually(t, "DeletionBlocked event", func() bool { return hasEvent(t, ns, "in-use", "DeletionBlocked") })
		consistently(t, "backup to survive while the restore runs", 300*time.Millisecond, func() bool {
			var cur keeperv1.Backup
			var rs keeperv1.Restore
			_ = k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "reader"}, &rs)
			err := k8s.Get(ctx, client.ObjectKeyFromObject(b), &cur)
			if err != nil {
				t.Logf("backup gone: %v; restore phase=%s msg=%s", err, rs.Status.Phase, rs.Status.Message)
			}
			return err == nil
		})
		sim.Pause(false)
		waitRestore(t, ns, "reader", keeperv1.RestoreCompleted)
		waitGone(t, b)
	})

	t.Run("storage outage blocks deletion until it recovers", func(t *testing.T) {
		createBackup(t, ns, "outage", backupSpec(keeperv1.DriverFileCopy, "", "data"))
		waitBackup(t, ns, "outage", keeperv1.BackupCompleted)
		store().Inject(func(op, key string) error {
			if op == "delete" && strings.Contains(key, "/outage/") {
				return errors.New("503 Service Unavailable")
			}
			return nil
		})
		b := getBackup(t, ns, "outage")
		must(t, k8s.Delete(ctx, b))
		eventually(t, "CleanupFailed event", func() bool { return hasEvent(t, ns, "outage", "CleanupFailed") })
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(b), &keeperv1.Backup{}); err != nil {
			t.Fatalf("backup vanished during the outage: %v", err)
		}
		store().Inject(nil)
		waitGone(t, b)
		if n := count("outage"); n != 0 {
			t.Fatalf("%d objects left", n)
		}
	})

	t.Run("skip annotation bypasses unreachable storage", func(t *testing.T) {
		createBackup(t, ns, "orphaned", backupSpec(keeperv1.DriverFileCopy, "", "data"))
		waitBackup(t, ns, "orphaned", keeperv1.BackupCompleted)
		store().Inject(func(op, key string) error {
			if strings.Contains(key, "/orphaned/") {
				return errors.New("bucket deleted")
			}
			return nil
		})
		t.Cleanup(func() { store().Inject(nil) })
		b := getBackup(t, ns, "orphaned")
		must(t, update(t, b, func(b *keeperv1.Backup) {
			b.Annotations = map[string]string{keeperv1.AnnotationSkipStorageCleanup: "true"}
		}))
		must(t, k8s.Delete(ctx, b))
		waitGone(t, b)
	})
}

// TestRestoreFailureModes covers Restores that cannot proceed.
func TestRestoreFailureModes(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	workload(t, ns, "data")

	createRestore(t, ns, "ghost", "does-not-exist")
	if r := waitRestore(t, ns, "ghost", keeperv1.RestoreFailed); meta.FindStatusCondition(r.Status.Conditions, keeperv1.ConditionReady).Reason != "BackupNotFound" {
		t.Fatalf("conditions %+v", r.Status.Conditions)
	}

	createBackup(t, ns, "broken", backupSpec(keeperv1.DriverFileCopy, "", "nope"))
	waitBackup(t, ns, "broken", keeperv1.BackupFailed)
	createRestore(t, ns, "from-broken", "broken")
	waitRestore(t, ns, "from-broken", keeperv1.RestoreFailed)

	sim.Pause(true)
	t.Cleanup(func() { sim.Pause(false) })
	createBackup(t, ns, "slow", backupSpec(keeperv1.DriverFileCopy, "", "data"))
	createRestore(t, ns, "early", "slow", func(r *keeperv1.Restore) { r.Spec.PVCNamePrefix = "early-" })
	eventually(t, "restore to wait for its backup", func() bool {
		var r keeperv1.Restore
		_ = k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "early"}, &r)
		c := meta.FindStatusCondition(r.Status.Conditions, keeperv1.ConditionReady)
		return c != nil && c.Reason == "WaitingForBackup"
	})
	sim.Pause(false)
	waitRestore(t, ns, "early", keeperv1.RestoreCompleted)

	createRestore(t, ns, "unknown-volume", "slow", func(r *keeperv1.Restore) { r.Spec.Volumes = []string{"logs"} })
	if r := waitRestore(t, ns, "unknown-volume", keeperv1.RestoreFailed); !strings.Contains(r.Status.Message, `no volume "logs"`) {
		t.Fatalf("message %q", r.Status.Message)
	}

	sim.FailJob = func(j *batchv1.Job) error {
		if j.Namespace == ns && j.Labels[keeperv1.LabelRestore] != "" {
			return errors.New("mover: archive checksum mismatch")
		}
		return nil
	}
	t.Cleanup(func() { sim.FailJob = nil })
	createRestore(t, ns, "corrupt", "slow", func(r *keeperv1.Restore) { r.Spec.PVCNamePrefix = "corrupt-" })
	if r := waitRestore(t, ns, "corrupt", keeperv1.RestoreFailed); !strings.Contains(r.Status.Message, "checksum mismatch") {
		t.Fatalf("message %q", r.Status.Message)
	}
}
