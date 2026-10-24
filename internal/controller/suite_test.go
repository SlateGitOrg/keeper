package controller_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/hooks"
	"github.com/archit2501/keeper/internal/operator"
	"github.com/archit2501/keeper/internal/simcluster"
	"github.com/archit2501/keeper/internal/storage"
)

// The suite runs one API server (envtest), one manager with the real
// reconcilers, and the simulated kubelet/snapshot controller.
var (
	envReady  bool
	envErr    error
	k8s       client.Client
	scheme    *runtime.Scheme
	stores    *storage.MemoryFactory
	execFake  *hooks.FakeExecutor
	clock     *clocktesting.FakeClock
	sim       *simcluster.Cluster
	nsCounter atomic.Int64
)

const (
	endpoint = "http://minio.test:9000"
	bucket   = "keeper-it"
	timeout  = 20 * time.Second
)

func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

func runSuite(m *testing.M) int {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		envErr = fmt.Errorf("KUBEBUILDER_ASSETS is not set; run `make test` to install envtest binaries")
		if os.Getenv("KEEPER_REQUIRE_ENVTEST") != "" {
			fmt.Fprintln(os.Stderr, envErr)
			return 1
		}
		return m.Run()
	}
	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(false), zap.Level(zapLevel())))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases"), filepath.Join("..", "..", "config", "test-crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest:", err)
		return 1
	}
	defer func() { _ = env.Stop() }()

	scheme, err = operator.NewScheme()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	stores = storage.NewMemoryFactory()
	execFake = &hooks.FakeExecutor{}
	clock = clocktesting.NewFakeClock(time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := operator.Setup(ctx, mgr, operator.Options{
		Stores: stores, Exec: execFake, Clock: clock, MoverImage: "keeper:test",
		PollInterval: 100 * time.Millisecond, MaxRequeue: 100 * time.Millisecond, MaxConcurrentBackups: 2,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "manager:", err)
		}
	}()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		fmt.Fprintln(os.Stderr, "cache did not sync")
		return 1
	}
	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	root, err := os.MkdirTemp("", "keeper-it-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(root) }()
	stores.Secrets = k8s
	sim = simcluster.New(k8s, stores, root)
	sim.CSI = true
	sim.Now = clock.Now
	go sim.Run(ctx, 20*time.Millisecond)

	envReady = true
	return m.Run()
}

func zapLevel() zapcore.Level {
	if os.Getenv("KEEPER_TEST_VERBOSE") != "" {
		return zapcore.DebugLevel
	}
	return zapcore.ErrorLevel
}

func requireEnv(t *testing.T) {
	t.Helper()
	if !envReady {
		t.Skip(envErr)
	}
}

// newNamespace creates a namespace with a storage Secret.
func newNamespace(t *testing.T) string {
	t.Helper()
	ns := fmt.Sprintf("it-%d", nsCounter.Add(1))
	ctx := context.Background()
	must(t, k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	must(t, k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: ns},
		Data:       map[string][]byte{storage.SecretAccessKeyID: []byte("id"), storage.SecretSecretAccessKey: []byte("secret")},
	}))
	return ns
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// consistently asserts f stays true for d.
func consistently(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !f() {
			t.Fatalf("%s stopped holding", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func storageSpec(prefix string) keeperv1.StorageSpec {
	return keeperv1.StorageSpec{
		Endpoint: endpoint, Bucket: bucket, Prefix: prefix,
		CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3"},
	}
}

func store() *storage.Memory { return stores.Store(endpoint, bucket) }

func pgHooks() *keeperv1.Hooks {
	sel := metav1.LabelSelector{MatchLabels: map[string]string{"app": "pg"}}
	return &keeperv1.Hooks{
		Pre:  []keeperv1.Hook{{Name: "freeze", PodSelector: sel, Command: []string{"fsfreeze", "-f", "/data"}}},
		Post: []keeperv1.Hook{{Name: "unfreeze", PodSelector: sel, Command: []string{"fsfreeze", "-u", "/data"}}},
	}
}

func backupSpec(driver keeperv1.DriverType, prefix string, claims ...string) keeperv1.BackupSpec {
	return keeperv1.BackupSpec{
		Source:  keeperv1.VolumeSource{PVCNames: claims},
		Driver:  keeperv1.DriverSpec{Type: driver},
		Storage: storageSpec(prefix),
	}
}

// workload creates a claim with sample files and a running pod using it.
func workload(t *testing.T, ns, claim string) string {
	t.Helper()
	ctx := context.Background()
	dir, err := sim.CreateClaim(ctx, ns, claim, "1Gi", map[string]string{"app": "pg"})
	must(t, err)
	must(t, os.MkdirAll(filepath.Join(dir, "pgdata"), 0o750))
	must(t, os.WriteFile(filepath.Join(dir, "pgdata", "PG_VERSION"), []byte("17\n"), 0o600))
	must(t, os.WriteFile(filepath.Join(dir, "pgdata", "table"), []byte(strings.Repeat("row\n", 1000)), 0o600))
	must(t, sim.RunPod(ctx, ns, "pg-0", map[string]string{"app": "pg"}, claim))
	return dir
}

func getBackup(t *testing.T, ns, name string) *keeperv1.Backup {
	t.Helper()
	var b keeperv1.Backup
	must(t, k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &b))
	return &b
}

func waitBackup(t *testing.T, ns, name string, phase keeperv1.BackupPhase) *keeperv1.Backup {
	t.Helper()
	var b keeperv1.Backup
	eventually(t, fmt.Sprintf("backup %s/%s to be %s", ns, name, phase), func() bool {
		if err := k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &b); err != nil {
			return false
		}
		if b.Status.Phase.Terminal() && b.Status.Phase != phase {
			t.Fatalf("backup %s ended %s (%s), want %s", name, b.Status.Phase, b.Status.Message, phase)
		}
		return b.Status.Phase == phase
	})
	return &b
}

func waitRestore(t *testing.T, ns, name string, phase keeperv1.RestorePhase) *keeperv1.Restore {
	t.Helper()
	var r keeperv1.Restore
	eventually(t, fmt.Sprintf("restore %s/%s to be %s", ns, name, phase), func() bool {
		if err := k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &r); err != nil {
			return false
		}
		if r.Status.Phase.Terminal() && r.Status.Phase != phase {
			t.Fatalf("restore %s ended %s (%s), want %s", name, r.Status.Phase, r.Status.Message, phase)
		}
		return r.Status.Phase == phase
	})
	return &r
}

func waitGone(t *testing.T, obj client.Object) {
	t.Helper()
	eventually(t, "deletion of "+obj.GetName(), func() bool {
		err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
		return err != nil && client.IgnoreNotFound(err) == nil
	})
}

// eventReasons lists reasons of events regarding name in ns.
func eventReasons(t *testing.T, ns, name string) []string {
	t.Helper()
	var list eventsv1.EventList
	must(t, k8s.List(context.Background(), &list, client.InNamespace(ns)))
	var out []string
	for _, e := range list.Items {
		if e.Regarding.Name == name {
			out = append(out, e.Reason)
		}
	}
	return out
}

func hasEvent(t *testing.T, ns, name, reason string) bool {
	t.Helper()
	for _, r := range eventReasons(t, ns, name) {
		if r == reason {
			return true
		}
	}
	return false
}

// update re-reads obj, applies mutate and writes it, retrying on conflicts
// with the controllers' own writes.
func update[T client.Object](t *testing.T, obj T, mutate func(T)) error {
	t.Helper()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		mutate(obj)
		return k8s.Update(context.Background(), obj)
	})
}

func createBackup(t *testing.T, ns, name string, spec keeperv1.BackupSpec) *keeperv1.Backup {
	t.Helper()
	b := &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: spec}
	must(t, k8s.Create(context.Background(), b))
	return b
}

func createRestore(t *testing.T, ns, name, backup string, mut ...func(*keeperv1.Restore)) *keeperv1.Restore {
	t.Helper()
	r := &keeperv1.Restore{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: keeperv1.RestoreSpec{BackupName: backup}}
	for _, m := range mut {
		m(r)
	}
	must(t, k8s.Create(context.Background(), r))
	return r
}
