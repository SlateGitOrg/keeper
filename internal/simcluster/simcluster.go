// Package simcluster stands in for the parts of a Kubernetes cluster that
// envtest does not run: the kubelet (mover Jobs, pod status), the CSI
// snapshot controller and the volume provisioner. Volumes are local
// directories, so data really flows through the mover and object storage.
//
// It is used by the integration tests and the offline demo. It is not part of
// the operator binary.
package simcluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/mover"
	"github.com/archit2501/keeper/internal/snapshot"
	"github.com/archit2501/keeper/internal/storage"
)

var errNotReady = errors.New("simcluster: pod cannot start yet")

// NodeName is the single simulated node.
const NodeName = "sim-node-1"

// Cluster simulates kubelet, provisioner and snapshot controller.
type Cluster struct {
	Client client.Client
	Stores *storage.MemoryFactory
	// Root holds volume and snapshot directories.
	Root string
	Now  func() time.Time
	Log  *slog.Logger
	// CSI enables the snapshot controller simulation.
	CSI bool
	// FailJob, if set, can fail a mover Job instead of running it.
	FailJob func(*batchv1.Job) error
	// FailSnapshot, if set, returns an error message for a VolumeSnapshot.
	FailSnapshot func(*snapv1.VolumeSnapshot) string

	mu      sync.Mutex
	volumes map[string]string
	paused  bool
	provMu  sync.Mutex
	stepMu  sync.Mutex
}

// Pause stops (true) or resumes (false) all simulated controllers, leaving
// Jobs and snapshots in flight.
// Pausing waits for an in-flight Step to finish, so nothing progresses
// after Pause(true) returns.
func (c *Cluster) Pause(p bool) {
	c.mu.Lock()
	c.paused = p
	c.mu.Unlock()
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
}

func (c *Cluster) isPaused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

// New returns a Cluster rooted at root.
func New(c client.Client, stores *storage.MemoryFactory, root string) *Cluster {
	return &Cluster{Client: c, Stores: stores, Root: root, Now: time.Now, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), volumes: map[string]string{}}
}

func key(ns, name string) string { return ns + "/" + name }

// VolumeDir returns the directory backing a claim ("" if unbound).
func (c *Cluster) VolumeDir(ns, claim string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.volumes[key(ns, claim)]
}

func (c *Cluster) bindDir(ns, claim, dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.volumes[key(ns, claim)] = dir
}

// CreateClaim creates a Bound claim backed by a fresh directory and returns
// the directory.
func (c *Cluster) CreateClaim(ctx context.Context, ns, name, size string, lbls map[string]string) (string, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To("sim-csi"),
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
		},
	}
	if err := c.Client.Create(ctx, pvc); err != nil {
		return "", err
	}
	return c.provision(ctx, pvc, "")
}

func (c *Cluster) provision(ctx context.Context, pvc *corev1.PersistentVolumeClaim, from string) (string, error) {
	c.provMu.Lock()
	defer c.provMu.Unlock()
	dir := filepath.Join(c.Root, "volumes", pvc.Namespace, pvc.Name)
	if c.VolumeDir(pvc.Namespace, pvc.Name) == dir {
		return dir, nil // already provisioned by a concurrent caller
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	if from != "" {
		if err := CopyTree(from, dir); err != nil {
			return "", err
		}
	}
	c.bindDir(pvc.Namespace, pvc.Name, dir)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur corev1.PersistentVolumeClaim
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(pvc), &cur); err != nil {
			return err
		}
		if cur.Status.Phase == corev1.ClaimBound {
			return nil
		}
		cur.Status.Phase = corev1.ClaimBound
		cur.Status.AccessModes = cur.Spec.AccessModes
		cur.Status.Capacity = cur.Spec.Resources.Requests
		return c.Client.Status().Update(ctx, &cur)
	})
	if err != nil {
		return "", fmt.Errorf("bind %s: %w", pvc.Name, err)
	}
	return dir, nil
}

// RunPod creates a Running pod on the simulated node mounting claim.
func (c *Cluster) RunPod(ctx context.Context, ns, name string, lbls map[string]string, claim string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls},
		Spec: corev1.PodSpec{
			NodeName:   NodeName,
			Containers: []corev1.Container{{Name: "app", Image: "registry.example/app:1"}},
		},
	}
	if claim != "" {
		pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
		}}}
	}
	if err := c.Client.Create(ctx, pod); err != nil {
		return err
	}
	pod.Status.Phase = corev1.PodRunning
	return c.Client.Status().Update(ctx, pod)
}

// Run calls Step every interval until ctx is done.
func (c *Cluster) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Step(ctx); err != nil && ctx.Err() == nil {
				c.Log.Warn("simcluster step", "err", err)
			}
		}
	}
}

// Step advances every simulated controller once.
func (c *Cluster) Step(ctx context.Context) error {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	if c.isPaused() {
		return nil
	}
	return errors.Join(c.stepClaims(ctx), c.stepSnapshots(ctx), c.stepJobs(ctx))
}

// stepClaims binds pending claims, populating those with a snapshot source.
func (c *Cluster) stepClaims(ctx context.Context) error {
	var list corev1.PersistentVolumeClaimList
	if err := c.Client.List(ctx, &list); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		pvc := &list.Items[i]
		if pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		from := ""
		if ds := pvc.Spec.DataSource; ds != nil && ds.Kind == "VolumeSnapshot" {
			from = filepath.Join(c.Root, "snapshots", pvc.Namespace, ds.Name)
			if _, err := os.Stat(from); err != nil {
				continue // snapshot not cut yet
			}
		}
		if _, err := c.provision(ctx, pvc, from); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// stepSnapshots cuts a snapshot on first sight (copying the source volume
// directory) and marks it ready on the next step, mimicking the cut/ready
// split of real CSI drivers.
func (c *Cluster) stepSnapshots(ctx context.Context) error {
	if !c.CSI {
		return nil
	}
	var list snapv1.VolumeSnapshotList
	if err := c.Client.List(ctx, &list); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		vs := &list.Items[i]
		if vs.Status != nil && (vs.Status.Error != nil || ptr.Deref(vs.Status.ReadyToUse, false)) {
			continue
		}
		if c.FailSnapshot != nil {
			if msg := c.FailSnapshot(vs); msg != "" {
				vs.Status = &snapv1.VolumeSnapshotStatus{Error: &snapv1.VolumeSnapshotError{Message: &msg, Time: &metav1.Time{Time: c.Now()}}}
				errs = append(errs, c.Client.Status().Update(ctx, vs))
				continue
			}
		}
		if vs.Status == nil || vs.Status.CreationTime == nil {
			errs = append(errs, c.cut(ctx, vs))
			continue
		}
		vs.Status.ReadyToUse = ptr.To(true)
		errs = append(errs, c.Client.Status().Update(ctx, vs))
	}
	return errors.Join(errs...)
}

func (c *Cluster) cut(ctx context.Context, vs *snapv1.VolumeSnapshot) error {
	src := ptr.Deref(vs.Spec.Source.PersistentVolumeClaimName, "")
	dir := c.VolumeDir(vs.Namespace, src)
	if dir == "" {
		return fmt.Errorf("snapshot %s: source claim %s is not bound", vs.Name, src)
	}
	dst := filepath.Join(c.Root, "snapshots", vs.Namespace, vs.Name)
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err
	}
	if err := CopyTree(dir, dst); err != nil {
		return err
	}
	var pvc corev1.PersistentVolumeClaim
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: vs.Namespace, Name: src}, &pvc); err != nil {
		return err
	}
	size := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	vs.Status = &snapv1.VolumeSnapshotStatus{
		CreationTime:                   &metav1.Time{Time: c.Now()},
		ReadyToUse:                     ptr.To(false),
		RestoreSize:                    &size,
		BoundVolumeSnapshotContentName: ptr.To("snapcontent-" + string(vs.UID)),
	}
	return c.Client.Status().Update(ctx, vs)
}

// stepJobs runs mover Jobs in-process and records their outcome in status.
func (c *Cluster) stepJobs(ctx context.Context) error {
	var list batchv1.JobList
	if err := c.Client.List(ctx, &list, client.MatchingLabels{keeperv1.LabelComponent: snapshot.ComponentMover}); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		job := &list.Items[i]
		if job.Status.StartTime != nil {
			continue
		}
		runErr := c.runJob(ctx, job)
		if errors.Is(runErr, errNotReady) {
			continue // like a pod waiting for its volume
		}
		if runErr != nil {
			c.Log.Info("mover job failed", "job", job.Name, "err", runErr)
		}
		errs = append(errs, c.finishJob(ctx, job, runErr))
	}
	return errors.Join(errs...)
}

func (c *Cluster) runJob(ctx context.Context, job *batchv1.Job) error {
	if c.FailJob != nil {
		if err := c.FailJob(job); err != nil {
			return err
		}
	}
	spec := job.Spec.Template.Spec
	if len(spec.Containers) != 1 || len(spec.Volumes) != 1 || spec.Volumes[0].PersistentVolumeClaim == nil {
		return errors.New("unexpected mover pod shape")
	}
	ctr := spec.Containers[0]
	args, err := mover.ParseArgs(ctr.Args)
	if err != nil {
		return err
	}
	claim := spec.Volumes[0].PersistentVolumeClaim.ClaimName
	dir := c.VolumeDir(job.Namespace, claim)
	if dir == "" {
		return fmt.Errorf("%w: claim %s is not bound", errNotReady, claim)
	}
	env := map[string]string{}
	for _, e := range ctr.Env {
		env[e.Name] = e.Value
	}
	args.Path = dir
	store := c.Stores.Store(env[mover.EnvEndpoint], env[mover.EnvBucket])
	return mover.Execute(ctx, store, args, mover.Runtime{Now: c.Now, Log: c.Log})
}

// finishJob writes a terminal Job status that satisfies API validation:
// startTime, counters, and the SuccessCriteriaMet/FailureTarget condition
// preceding Complete/Failed.
func (c *Cluster) finishJob(ctx context.Context, job *batchv1.Job, runErr error) error {
	now := metav1.NewTime(c.Now())
	job.Status.StartTime = &now
	if runErr == nil {
		job.Status.Succeeded = 1
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now, LastProbeTime: now, Reason: "CompletionsReached", Message: "Reached expected number of succeeded pods"},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now, LastProbeTime: now, Reason: "CompletionsReached", Message: "Reached expected number of succeeded pods"},
		}
	} else {
		msg := runErr.Error()
		job.Status.Failed = 1
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now, LastProbeTime: now, Reason: "BackoffLimitExceeded", Message: msg},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now, LastProbeTime: now, Reason: "BackoffLimitExceeded", Message: msg},
		}
	}
	return c.Client.Status().Update(ctx, job)
}

// CopyTree copies regular files, directories and symlinks from src into dst.
func CopyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil || rel == "." {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			l, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(l, target)
		case info.IsDir():
			return os.MkdirAll(target, info.Mode().Perm())
		case info.Mode().IsRegular():
			return copyFile(p, target, info.Mode().Perm())
		}
		return nil
	})
}

func copyFile(src, dst string, perm fs.FileMode) error {
	in, err := os.Open(src) //nolint:gosec // simulated volume path.
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm) //nolint:gosec // simulated volume path.
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
