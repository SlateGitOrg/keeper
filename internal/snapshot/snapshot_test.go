package snapshot

import (
	"context"
	"strings"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/mover"
	"github.com/archit2501/keeper/internal/storage"
)

func TestChildName(t *testing.T) {
	if got := ChildName("pg", "data", "backup"); got != "pg-data-backup" {
		t.Fatal(got)
	}
	long := strings.Repeat("a", 50)
	x, y := ChildName(long, "data-one", "backup"), ChildName(long, "data-two", "backup")
	if len(x) != 63 || x == y {
		t.Fatalf("long names must be 63 chars and distinct: %q %q", x, y)
	}
	if errs := validation.IsDNS1123Label(x); len(errs) > 0 {
		t.Fatalf("%q is not a DNS label: %v", x, errs)
	}
	if ChildName(long, "data-one", "backup") != x {
		t.Fatal("ChildName must be deterministic")
	}
}

func TestJobState(t *testing.T) {
	j := &batchv1.Job{}
	if d, f, _ := jobState(j); d || f {
		t.Fatal("empty job is running")
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionFalse}}
	if d, f, _ := jobState(j); d || f {
		t.Fatal("False conditions are ignored")
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "too slow"}}
	if _, f, msg := jobState(j); !f || !strings.Contains(msg, "DeadlineExceeded: too slow") {
		t.Fatalf("failed job: %q", msg)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if d, _, _ := jobState(j); !d {
		t.Fatal("complete job")
	}
}

func TestFor(t *testing.T) {
	drivers := map[keeperv1.DriverType]Driver{keeperv1.DriverCSISnapshot: &CSI{}}
	if _, err := For(keeperv1.DriverCSISnapshot, drivers); err != nil {
		t.Fatal(err)
	}
	if _, err := For(keeperv1.DriverFileCopy, drivers); err == nil {
		t.Fatal("unregistered driver accepted")
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, keeperv1.AddToScheme, snapv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func backup(driver keeperv1.DriverType) *keeperv1.Backup {
	return &keeperv1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "db", UID: "uid-1"},
		Spec: keeperv1.BackupSpec{
			Driver: keeperv1.DriverSpec{Type: driver},
			Storage: keeperv1.StorageSpec{Endpoint: "http://minio:9000", Bucket: "b", Prefix: "p",
				CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3"}},
		},
		Status: keeperv1.BackupStatus{Volumes: []keeperv1.VolumeBackup{{PVCName: "data", Capacity: resource.MustParse("10Gi")}}},
	}
}

func TestCSITimeoutAndForeignSnapshot(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := backup(keeperv1.DriverCSISnapshot)
	b.Spec.Driver.CSI = &keeperv1.CSIDriverSpec{ReadyTimeoutSeconds: 60}
	b.Status.StartTime = &metav1.Time{Time: now.Add(-2 * time.Minute)}
	foreign := &snapv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Name: VolumeSnapshotName("pg", "data"), Namespace: "db", Labels: map[string]string{keeperv1.LabelBackup: "someone-else"}}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(foreign).WithStatusSubresource(&snapv1.VolumeSnapshot{}).Build()
	d := &CSI{Deps: Deps{Client: c, Scheme: s, Now: func() time.Time { return now }}}

	if err := d.Start(ctx, b); err == nil || !strings.Contains(err.Error(), "does not belong") {
		t.Fatalf("adopting a foreign snapshot: %v", err)
	}
	p, err := d.Observe(ctx, b, nil)
	if err != nil || !p.Failed || !strings.Contains(p.Message, "not ready after 1m0s") {
		t.Fatalf("timeout: %+v %v", p, err)
	}
	if csiTimeout(backup(keeperv1.DriverCSISnapshot)) != 10*time.Minute {
		t.Fatal("default timeout")
	}
}

func TestCSIRestoreSizesClaimBySnapshot(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	d := &CSI{Deps: Deps{Client: c, Scheme: s}}
	r := &keeperv1.Restore{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "db"}, Spec: keeperv1.RestoreSpec{StorageClassName: ptr.To("fast")}}
	vol := keeperv1.VolumeBackup{PVCName: "data", Capacity: resource.MustParse("10Gi"), SizeBytes: 12 << 30, StorageClassName: "slow"}
	if err := d.StartRestore(ctx, r, backup(keeperv1.DriverCSISnapshot), []RestoreVolume{{Source: vol, Target: "restored-data"}}); err != nil {
		t.Fatal(err)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(ctx, types.NamespacedName{Namespace: "db", Name: "restored-data"}, &pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Value() != 12<<30 {
		t.Fatalf("request %s, want the 12Gi restoreSize", got.String())
	}
	if *pvc.Spec.StorageClassName != "fast" || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("claim spec %+v", pvc.Spec)
	}
	pvc.Status.Phase = corev1.ClaimLost
	if err := c.Status().Update(ctx, &pvc); err != nil {
		t.Fatal(err)
	}
	p, err := d.ObserveRestore(ctx, r, nil, []RestoreVolume{{Source: vol, Target: "restored-data"}})
	if err != nil || !p.Failed {
		t.Fatalf("lost claim: %+v %v", p, err)
	}
}

func TestFileCopyJobShape(t *testing.T) {
	d := &FileCopy{Image: "keeper:1"}
	st := backup(keeperv1.DriverFileCopy).Spec.Storage
	args := mover.Args{Mode: mover.ModeRestore, Path: mover.MountPath, ArchiveKey: "k", SHA256: "abc"}
	job := d.moverJob("db", "j", "target", false, args, st, &keeperv1.FileCopyDriverSpec{ActiveDeadlineSeconds: 120, BackoffLimit: 3})
	spec := job.Spec.Template.Spec
	ctr := spec.Containers[0]
	if *job.Spec.ActiveDeadlineSeconds != 120 || *job.Spec.BackoffLimit != 3 || spec.RestartPolicy != corev1.RestartPolicyNever || *spec.AutomountServiceAccountToken {
		t.Fatalf("job spec %+v", job.Spec)
	}
	if spec.Volumes[0].PersistentVolumeClaim.ReadOnly || ctr.VolumeMounts[0].ReadOnly {
		t.Fatal("restore mover must write")
	}
	if got := ctr.SecurityContext.Capabilities.Add; len(got) != 3 || got[0] != "CHOWN" {
		t.Fatalf("restore capabilities %v", got)
	}
	parsed, err := mover.ParseArgs(ctr.Args)
	if err != nil || parsed != args {
		t.Fatalf("args round trip %+v %v", parsed, err)
	}
	if ctr.Command[0] != "/keeper" || ctr.Image != "keeper:1" {
		t.Fatalf("container %+v", ctr)
	}
}

func TestFileCopyObserveEdgeCases(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)
	b := backup(keeperv1.DriverFileCopy)
	done := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: BackupJobName("pg", "data"), Namespace: "db"},
		Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(done).Build()
	d := &FileCopy{Deps: Deps{Client: c, Scheme: s}}
	store := storage.NewMemory()

	p, err := d.Observe(ctx, b, store)
	if err != nil || !p.Failed || !strings.Contains(p.Message, "without writing") {
		t.Fatalf("missing manifest: %+v %v", p, err)
	}

	b.Status.Volumes[0].Ready = true
	p, err = d.Observe(ctx, b, store)
	if err != nil || !p.Done || !p.Cut {
		t.Fatalf("already-ready volumes are done without the Job: %+v %v", p, err)
	}

	missing := backup(keeperv1.DriverFileCopy)
	missing.Name = "other"
	p, err = d.Observe(ctx, missing, store)
	if err != nil || p.Done || p.Failed {
		t.Fatalf("a Job not yet in the cache is pending: %+v %v", p, err)
	}
	if err := d.Cleanup(ctx, b); err != nil {
		t.Fatal(err)
	}
}
