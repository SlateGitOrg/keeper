// Command keeper-demo runs the operator end to end against a real API server
// (envtest: kube-apiserver + etcd binaries, no cluster, no network) with the
// simulated kubelet/snapshot controller from internal/simcluster and an
// in-memory object store. It fast-forwards a fake clock through weeks of
// scheduled backups and prints what the operator did.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-logr/logr"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/controller"
	"github.com/archit2501/keeper/internal/hooks"
	"github.com/archit2501/keeper/internal/operator"
	"github.com/archit2501/keeper/internal/retention"
	"github.com/archit2501/keeper/internal/simcluster"
	"github.com/archit2501/keeper/internal/storage"
)

const (
	ns       = "shop"
	endpoint = "http://minio.backup.svc:9000"
	bucket   = "cluster-backups"
)

type demo struct {
	c     client.Client
	clock *clocktesting.FakeClock
	sim   *simcluster.Cluster
	store *storage.Memory
	exec  *hooks.FakeExecutor
	out   io.Writer
}

func main() {
	days := flag.Int("days", 60, "simulated days of 6-hourly backups")
	crds := flag.String("crds", "config", "directory holding crd/bases and test-crds")
	flag.Parse()
	if err := run(*days, *crds, os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func section(w io.Writer, title string) {
	_, _ = fmt.Fprintf(w, "\n== %s %s\n", title, strings.Repeat("=", max(0, 70-len(title))))
}

func run(days int, crdDir string, out io.Writer) error {
	ctrl.SetLogger(logr.Discard())
	klog.SetLogger(logr.Discard()) // client-go logs request cancellations at shutdown
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return errors.New("KUBEBUILDER_ASSETS is not set; run `make demo`")
	}
	section(out, "1. Start a real API server (envtest) with the keeper CRDs")
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(crdDir, "crd", "bases"), filepath.Join(crdDir, "test-crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		return err
	}
	defer func() { _ = env.Stop() }()
	scheme, err := operator.NewScheme()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		return err
	}
	stores := storage.NewMemoryFactory()
	d := &demo{
		clock: clocktesting.NewFakeClock(time.Now().UTC()),
		exec:  &hooks.FakeExecutor{},
		store: stores.Store(endpoint, bucket),
		out:   out,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := operator.Setup(ctx, mgr, operator.Options{
		Stores: stores, Exec: d.exec, Clock: d.clock, MoverImage: "keeper:demo",
		PollInterval: 50 * time.Millisecond, MaxRequeue: 50 * time.Millisecond, MaxConcurrentBackups: 4,
	}); err != nil {
		return err
	}
	go func() { _ = mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return errors.New("cache did not sync")
	}
	if d.c, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return err
	}
	stores.Secrets = d.c
	root, err := os.MkdirTemp("", "keeper-demo-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()
	d.sim = simcluster.New(d.c, stores, root)
	d.sim.CSI = true
	d.sim.Now = d.clock.Now
	go d.sim.Run(ctx, 15*time.Millisecond)

	ver := "unknown"
	if v, err := envVersion(cfg); err == nil {
		ver = v
	}
	_, _ = fmt.Fprintf(out, "kube-apiserver %s up; CRDs: backuppolicies, backups, restores (+ VolumeSnapshot CRDs)\n", ver)

	steps := []func(context.Context) error{
		d.validation,
		func(ctx context.Context) error { return d.schedule(ctx, days) },
		d.failure,
		d.csi,
		d.deletion,
		d.summary,
	}
	for _, s := range steps {
		if err := s(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d *demo) validation(ctx context.Context) error {
	section(d.out, "2. CRD validation (CEL) rejects bad specs at admission")
	bad := map[string]*keeperv1.BackupPolicy{
		"retention that keeps nothing": policy("bad-1", func(p *keeperv1.BackupPolicy) { p.Spec.Retention = keeperv1.Retention{} }),
		"both pvcNames and pvcSelector": policy("bad-2", func(p *keeperv1.BackupPolicy) {
			p.Spec.Template.Spec.Source.PVCSelector = &metav1.LabelSelector{}
		}),
		"time zone smuggled into the cron": policy("bad-3", func(p *keeperv1.BackupPolicy) { p.Spec.Schedule = "CRON_TZ=Asia/Kolkata 0 2 * * *" }),
		"absolute storage prefix":          policy("bad-4", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Storage.Prefix = "/etc" }),
	}
	names := make([]string, 0, len(bad))
	for k := range bad {
		names = append(names, k)
	}
	sort.Strings(names)
	if err := d.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		return err
	}
	for _, n := range names {
		err := d.c.Create(ctx, bad[n])
		if err == nil {
			return fmt.Errorf("%s was accepted", n)
		}
		msg := err.Error()
		if i := strings.LastIndex(msg, ": "); i > 0 {
			msg = msg[i+2:]
		}
		_, _ = fmt.Fprintf(d.out, "  %-34s -> rejected: %s\n", n, strings.TrimSuffix(msg, "]"))
	}
	return nil
}

func policy(name string, mut func(*keeperv1.BackupPolicy)) *keeperv1.BackupPolicy {
	sel := metav1.LabelSelector{MatchLabels: map[string]string{"app": "postgres"}}
	p := &keeperv1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: keeperv1.BackupPolicySpec{
			Schedule: "0 */6 * * *",
			Retention: keeperv1.Retention{
				KeepLast: 4, KeepDaily: 7, KeepWeekly: 4, KeepMonthly: 3,
			},
			Template: keeperv1.BackupTemplate{Spec: keeperv1.BackupSpec{
				Source: keeperv1.VolumeSource{PVCNames: []string{"pg-data"}},
				Driver: keeperv1.DriverSpec{Type: keeperv1.DriverFileCopy},
				Storage: keeperv1.StorageSpec{
					Endpoint: endpoint, Bucket: bucket, Prefix: "prod",
					CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
				},
				Hooks: &keeperv1.Hooks{
					Pre: []keeperv1.Hook{
						{Name: "checkpoint", PodSelector: sel, Container: "app", Command: []string{"psql", "-c", "CHECKPOINT"}},
						{Name: "freeze", PodSelector: sel, Command: []string{"fsfreeze", "--freeze", "/var/lib/postgresql"}},
					},
					Post: []keeperv1.Hook{{Name: "thaw", PodSelector: sel, Command: []string{"fsfreeze", "--unfreeze", "/var/lib/postgresql"}}},
				},
			}},
		},
	}
	if mut != nil {
		mut(p)
	}
	return p
}

// treeHash fingerprints a directory (paths, modes, contents).
func treeHash(root string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if strings.HasPrefix(rel, ".keeper") {
			return nil
		}
		_, _ = fmt.Fprintf(h, "%s|%v|", rel, de.Type())
		if de.Type().IsRegular() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			h.Write(b)
		}
		return nil
	})
	return hex.EncodeToString(h.Sum(nil))[:16], err
}

// writeRows simulates database writes between backups.
func writeRows(dir string, slot int) error {
	f, err := os.OpenFile(filepath.Join(dir, "pgdata", "base", fmt.Sprintf("orders.%d", slot%4)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	for i := range 40 {
		_, _ = fmt.Fprintf(f, "order-%06d-%02d,sku-%03d,qty=%d\n", slot, i, (slot*7+i)%997, i%5+1)
	}
	return f.Close()
}

func (d *demo) waitBackup(ctx context.Context, name string, want ...keeperv1.BackupPhase) (*keeperv1.Backup, error) {
	deadline := time.Now().Add(30 * time.Second)
	var b keeperv1.Backup
	for time.Now().Before(deadline) {
		if err := d.c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &b); err == nil {
			for _, w := range want {
				if b.Status.Phase == w {
					return &b, nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("backup %s did not reach %v (phase %q: %s)", name, want, b.Status.Phase, b.Status.Message)
}

func (d *demo) waitRestore(ctx context.Context, name string) (*keeperv1.Restore, error) {
	deadline := time.Now().Add(30 * time.Second)
	var r keeperv1.Restore
	for time.Now().Before(deadline) {
		if err := d.c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &r); err == nil && r.Status.Phase.Terminal() {
			return &r, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("restore %s did not finish", name)
}

func (d *demo) policyBackups(ctx context.Context, name string) ([]keeperv1.Backup, error) {
	var list keeperv1.BackupList
	if err := d.c.List(ctx, &list, client.InNamespace(ns), client.MatchingLabels{keeperv1.LabelPolicy: name}); err != nil {
		return nil, err
	}
	var out []keeperv1.Backup
	for _, b := range list.Items {
		if b.DeletionTimestamp.IsZero() {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name > out[j].Name })
	return out, nil
}

func (d *demo) schedule(ctx context.Context, days int) error {
	section(d.out, "3. A Postgres workload and a 6-hourly BackupPolicy with hooks")
	if err := d.c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: ns},
		Data:       map[string][]byte{storage.SecretAccessKeyID: []byte("demo"), storage.SecretSecretAccessKey: []byte("demo-secret")},
	}); err != nil {
		return err
	}
	dir, err := d.sim.CreateClaim(ctx, ns, "pg-data", "20Gi", map[string]string{"app": "postgres"})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "pgdata", "base"), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "pgdata", "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		return err
	}
	if err := os.Symlink("pgdata/PG_VERSION", filepath.Join(dir, "VERSION")); err != nil {
		return err
	}
	if err := d.sim.RunPod(ctx, ns, "postgres-0", map[string]string{"app": "postgres"}, "pg-data"); err != nil {
		return err
	}
	p := policy("pg", nil)
	if err := d.c.Create(ctx, p); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(d.out, "policy %s/%s: schedule %q tz=%s, keepLast=4 keepDaily=7 keepWeekly=4 keepMonthly=3, driver FileCopy\n",
		ns, p.Name, p.Spec.Schedule, p.Spec.TimeZone)

	section(d.out, fmt.Sprintf("4. Fast-forward %d days of schedule slots (fake clock)", days))
	start := d.clock.Now().Truncate(6 * time.Hour).Add(6 * time.Hour)
	slots := days * 4
	fingerprints := map[string]string{}
	var items []retention.Item
	var lastNext time.Time
	began := time.Now()
	for i := range slots {
		slot := start.Add(time.Duration(i) * 6 * time.Hour)
		if err := writeRows(dir, i); err != nil {
			return err
		}
		fp, err := treeHash(dir)
		if err != nil {
			return err
		}
		d.clock.SetTime(slot.Add(20 * time.Second))
		name := "pg-" + slot.Format(controller.BackupNameTimeFormat)
		b, err := d.waitBackup(ctx, name, keeperv1.BackupCompleted)
		if err != nil {
			return err
		}
		fingerprints[name] = fp
		items = append(items, retention.Item{ID: name, Time: b.Status.SnapshotTime.Time})
		if (i+1)%(7*4) == 0 || i == slots-1 {
			if err := d.progress(ctx, i+1, slot, items); err != nil {
				return err
			}
		}
		lastNext = slot.Add(6 * time.Hour)
	}
	_, _ = fmt.Fprintf(d.out, "%d backups ran in %.1fs of wall time\n", slots, time.Since(began).Seconds())
	if calls := d.exec.CommandLines(); len(calls) >= 3 {
		_, _ = fmt.Fprintf(d.out, "hook executions per backup (first backup): %q, %q, %q\n", calls[0], calls[1], calls[2])
	}

	section(d.out, "5. What retention kept (newest first)")
	var bs []keeperv1.Backup
	want := map[string]bool{}
	decisions, err := retention.Apply(retention.Policy{Last: 4, Daily: 7, Weekly: 4, Monthly: 3}, items, time.UTC)
	if err != nil {
		return err
	}
	for _, dd := range decisions {
		if dd.Keep {
			want[dd.ID] = true
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if bs, err = d.policyBackups(ctx, "pg"); err != nil {
			return err
		}
		if len(bs) == len(want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	tw := tabwriter.NewWriter(d.out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "BACKUP\tSNAPSHOT (UTC)\tWEEKDAY\tARCHIVE\tRETAINED BY")
	match := len(bs) == len(want)
	for _, b := range bs {
		match = match && want[b.Name]
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", b.Name, b.Status.SnapshotTime.UTC().Format("2006-01-02 15:04"),
			b.Status.SnapshotTime.UTC().Weekday().String()[:3], human(b.Status.SizeBytes), b.Annotations[keeperv1.AnnotationRetainedBy])
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	var pol keeperv1.BackupPolicy
	if err := d.c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "pg"}, &pol); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(d.out, "kept %d of %d (bound keepLast+keepDaily+keepWeekly+keepMonthly = 18); policy status: kept=%d prunedTotal=%d next=%s\n",
		len(bs), slots, pol.Status.Retention.Kept, pol.Status.Retention.PrunedTotal, pol.Status.NextScheduleTime.UTC().Format(time.RFC3339))
	_, _ = fmt.Fprintf(d.out, "survivors identical to retention.Apply over the same snapshot times: %v\n", match)
	if !match {
		return errors.New("retention mismatch")
	}
	if !pol.Status.NextScheduleTime.Time.Equal(lastNext) {
		return fmt.Errorf("next schedule %v, want %v", pol.Status.NextScheduleTime, lastNext)
	}
	objs, _ := d.store.List(ctx, "prod/")
	_, _ = fmt.Fprintf(d.out, "object store: %d objects, %s (only retained backups remain)\n", len(objs), human(d.store.TotalBytes()))

	section(d.out, "6. Restore the oldest retained backup and verify it bit-for-bit")
	oldest := bs[len(bs)-1]
	if err := d.c.Create(ctx, &keeperv1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-restore", Namespace: ns},
		Spec:       keeperv1.RestoreSpec{BackupName: oldest.Name, PVCNamePrefix: "restored-"},
	}); err != nil {
		return err
	}
	r, err := d.waitRestore(ctx, "pg-restore")
	if err != nil {
		return err
	}
	got, err := treeHash(d.sim.VolumeDir(ns, "restored-pg-data"))
	if err != nil {
		return err
	}
	live, _ := treeHash(dir)
	_, _ = fmt.Fprintf(d.out, "restore %s from %s: phase=%s target=%s job=%s\n", r.Name, oldest.Name, r.Status.Phase, r.Status.Volumes[0].TargetPVC, r.Status.Volumes[0].JobName)
	_, _ = fmt.Fprintf(d.out, "fingerprint at backup time %s | restored %s | live volume now %s\n", fingerprints[oldest.Name], got, live)
	if got != fingerprints[oldest.Name] {
		return errors.New("restored data does not match the backup")
	}
	_, _ = fmt.Fprintln(d.out, "restored data matches the point-in-time copy: true")
	return nil
}

// progress waits for retention to settle on the expected survivor count for
// the backups taken so far, then prints a one-line summary.
func (d *demo) progress(ctx context.Context, n int, slot time.Time, items []retention.Item) error {
	decisions, err := retention.Apply(retention.Policy{Last: 4, Daily: 7, Weekly: 4, Monthly: 3}, items, time.UTC)
	if err != nil {
		return err
	}
	kept, _ := retention.Split(decisions)
	var bs []keeperv1.Backup
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if bs, err = d.policyBackups(ctx, "pg"); err != nil {
			return err
		}
		if objs, _ := d.store.List(ctx, "prod/"); len(bs) == len(kept) && len(objs) == 3*len(kept) {
			break
		}
	}
	objs, _ := d.store.List(ctx, "prod/")
	_, _ = fmt.Fprintf(d.out, "  day %3d (%s): %3d backups taken, %2d retained, %3d pruned, store %3d objects %s\n",
		n/4, slot.Format("2006-01-02"), n, len(bs), n-len(bs), len(objs), human(d.store.TotalBytes()))
	return nil
}

func (d *demo) failure(ctx context.Context) error {
	section(d.out, "7. A failing pre hook: no copy is taken and the app is thawed")
	d.exec.Respond = func(req hooks.ExecRequest) (hooks.ExecResult, error) {
		if strings.Contains(strings.Join(req.Command, " "), "--freeze") {
			return hooks.ExecResult{Stderr: "fsfreeze: /var/lib/postgresql: Operation not supported", ExitCode: 1}, fmt.Errorf("%w (code 1)", hooks.ErrNonZeroExit)
		}
		return hooks.ExecResult{Stdout: "CHECKPOINT"}, nil
	}
	defer func() { d.exec.Respond = nil }()
	spec := policy("x", nil).Spec.Template.Spec
	b := &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "pg-manual", Namespace: ns}, Spec: spec}
	if err := d.c.Create(ctx, b); err != nil {
		return err
	}
	b, err := d.waitBackup(ctx, "pg-manual", keeperv1.BackupFailed)
	if err != nil {
		return err
	}
	ready := meta.FindStatusCondition(b.Status.Conditions, keeperv1.ConditionReady)
	_, _ = fmt.Fprintf(d.out, "phase=%s reason=%s\nmessage: %s\n", b.Status.Phase, ready.Reason, b.Status.Message)
	for _, h := range b.Status.Hooks {
		code := "-"
		if h.ExitCode != nil {
			code = fmt.Sprint(*h.ExitCode)
		}
		_, _ = fmt.Fprintf(d.out, "  hook %-5s %-10s pod=%s exit=%s ok=%v\n", h.Stage, h.Hook, h.Pod, code, h.Succeeded)
	}
	return nil
}

func (d *demo) csi(ctx context.Context) error {
	section(d.out, "8. CSI VolumeSnapshot driver: snapshot, then restore via dataSource")
	spec := policy("x", nil).Spec.Template.Spec
	spec.Driver = keeperv1.DriverSpec{Type: keeperv1.DriverCSISnapshot, CSI: &keeperv1.CSIDriverSpec{VolumeSnapshotClassName: "csi-hostpath-snapclass"}}
	spec.Hooks = nil
	if err := d.c.Create(ctx, &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "pg-snap", Namespace: ns}, Spec: spec}); err != nil {
		return err
	}
	b, err := d.waitBackup(ctx, "pg-snap", keeperv1.BackupCompleted)
	if err != nil {
		return err
	}
	before, _ := treeHash(d.sim.VolumeDir(ns, "pg-data"))
	_, _ = fmt.Fprintf(d.out, "backup %s: phase=%s snapshot=%s restoreSize=%s manifest=%s\n", b.Name, b.Status.Phase,
		b.Status.Volumes[0].SnapshotName, human(b.Status.Volumes[0].SizeBytes), b.Status.ManifestKey)
	if err := writeRows(d.sim.VolumeDir(ns, "pg-data"), 9999); err != nil {
		return err
	}
	if err := d.c.Create(ctx, &keeperv1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "from-snap", Namespace: ns},
		Spec:       keeperv1.RestoreSpec{BackupName: "pg-snap", PVCNamePrefix: "snap-"},
	}); err != nil {
		return err
	}
	if _, err := d.waitRestore(ctx, "from-snap"); err != nil {
		return err
	}
	var pvc corev1.PersistentVolumeClaim
	if err := d.c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "snap-pg-data"}, &pvc); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for d.sim.VolumeDir(ns, "snap-pg-data") == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got, err := treeHash(d.sim.VolumeDir(ns, "snap-pg-data"))
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(d.out, "restored claim %s dataSource=%s/%s; content matches snapshot: %v\n", pvc.Name, pvc.Spec.DataSource.Kind, pvc.Spec.DataSource.Name, got == before)
	if got != before {
		return errors.New("CSI restore mismatch")
	}
	return nil
}

func (d *demo) deletion(ctx context.Context) error {
	section(d.out, "9. Deleting a Backup removes its data (finalizer)")
	bs, err := d.policyBackups(ctx, "pg")
	if err != nil {
		return err
	}
	victim := bs[0]
	before, _ := d.store.List(ctx, "prod/")
	if err := d.c.Delete(ctx, &victim); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := d.c.Get(ctx, client.ObjectKeyFromObject(&victim), &keeperv1.Backup{}); err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	after, _ := d.store.List(ctx, "prod/")
	_, _ = fmt.Fprintf(d.out, "deleted %s: store objects %d -> %d\n", victim.Name, len(before), len(after))
	return nil
}

func (d *demo) summary(ctx context.Context) error {
	section(d.out, "10. Events and metrics recorded by the operator")
	var evs eventsv1.EventList
	if err := d.c.List(ctx, &evs, client.InNamespace(ns)); err != nil {
		return err
	}
	counts := map[string]int{}
	for _, e := range evs.Items {
		n := 1
		if e.Series != nil {
			n = int(e.Series.Count)
		}
		counts[e.Type+"/"+e.Reason] += n
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(d.out, "  event %-32s x%d\n", k, counts[k])
	}
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		return err
	}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "keeper_") || strings.HasSuffix(f.GetName(), "_seconds") {
			continue
		}
		for _, m := range f.GetMetric() {
			_, _ = fmt.Fprintf(d.out, "  metric %s{%s} %g\n", f.GetName(), labels(m), value(m))
		}
	}
	return nil
}

func labels(m *dto.Metric) string {
	var parts []string
	for _, l := range m.GetLabel() {
		parts = append(parts, l.GetName()+"="+l.GetValue())
	}
	return strings.Join(parts, ",")
}

func value(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	return m.GetGauge().GetValue()
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}

func envVersion(cfg *rest.Config) (string, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return "", err
	}
	v, err := dc.ServerVersion()
	if err != nil {
		return "", err
	}
	return v.GitVersion, nil
}
