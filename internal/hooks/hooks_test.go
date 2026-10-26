package hooks

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keeperv1 "github.com/SlateGitOrg/keeper/api/v1alpha1"
)

func pod(name string, phase corev1.PodPhase, lbls map[string]string, containers ...string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db", Labels: lbls}, Status: corev1.PodStatus{Phase: phase}}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c, Image: "x"})
	}
	return p
}

func runner(t *testing.T, exec *FakeExecutor, pods ...*corev1.Pod) *Runner {
	t.Helper()
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	b := fake.NewClientBuilder().WithScheme(s)
	for _, p := range pods {
		b = b.WithObjects(p)
	}
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &Runner{Pods: b.Build(), Exec: exec, Now: func() time.Time { return fixed }}
}

func hook(name string, cmd string, onError keeperv1.HookErrorPolicy) keeperv1.Hook {
	return keeperv1.Hook{
		Name: name, Command: strings.Fields(cmd), OnError: onError, TimeoutSeconds: 5,
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "pg"}},
	}
}

var pgLabels = map[string]string{"app": "pg"}

func TestRunPreHooksInEveryRunningPod(t *testing.T) {
	exec := &FakeExecutor{}
	r := runner(t, exec,
		pod("pg-1", corev1.PodRunning, pgLabels, "postgres", "sidecar"),
		pod("pg-0", corev1.PodRunning, pgLabels, "postgres"),
		pod("pg-2", corev1.PodPending, pgLabels, "postgres"),
		pod("other", corev1.PodRunning, map[string]string{"app": "web"}, "web"),
	)
	out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{hook("checkpoint", "psql -c CHECKPOINT", keeperv1.HookFail)})
	if out.Failed || len(out.Results) != 2 {
		t.Fatalf("outcome %+v", out)
	}
	if got := exec.CommandLines(); !slices.Equal(got, []string{"pg-0: psql -c CHECKPOINT", "pg-1: psql -c CHECKPOINT"}) {
		t.Fatalf("calls %v", got)
	}
	c := exec.Calls()[1]
	if c.Container != "postgres" || c.Timeout != 5*time.Second || c.Namespace != "db" {
		t.Fatalf("request %+v", c)
	}
	if *out.Results[0].ExitCode != 0 || !out.Results[0].Succeeded || out.Results[0].Stage != "pre" {
		t.Fatalf("result %+v", out.Results[0])
	}
}

func TestPreStopsAtFirstFailurePostRunsAll(t *testing.T) {
	exec := &FakeExecutor{Respond: FailWhen("freeze", "fsfreeze: Operation not permitted")}
	r := runner(t, exec, pod("pg-0", corev1.PodRunning, pgLabels, "postgres"))
	hs := []keeperv1.Hook{hook("freeze", "fsfreeze -f /data", keeperv1.HookFail), hook("never", "true", keeperv1.HookFail)}

	pre := r.Run(context.Background(), "db", StagePre, hs)
	if !pre.Failed || len(pre.Results) != 1 || !strings.Contains(pre.Message, `pre hook "freeze"`) {
		t.Fatalf("pre %+v", pre)
	}
	if pre.Results[0].Message != "fsfreeze: Operation not permitted" || *pre.Results[0].ExitCode != 1 {
		t.Fatalf("result %+v", pre.Results[0])
	}

	post := r.Run(context.Background(), "db", StagePost, hs)
	if !post.Failed || len(post.Results) != 2 {
		t.Fatalf("post should run every hook: %+v", post)
	}
}

func TestContinuePolicyAndMissingPods(t *testing.T) {
	exec := &FakeExecutor{Respond: FailWhen("flaky", "boom")}
	r := runner(t, exec, pod("pg-0", corev1.PodRunning, pgLabels, "postgres"))
	nomatch := hook("orphan", "true", keeperv1.HookContinue)
	nomatch.PodSelector.MatchLabels = map[string]string{"app": "missing"}
	out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{hook("flaky", "flaky", keeperv1.HookContinue), nomatch, hook("ok", "true", keeperv1.HookFail)})
	if out.Failed || len(out.Results) != 3 {
		t.Fatalf("outcome %+v", out)
	}
	if out.Results[1].Pod != "" || out.Results[1].Succeeded || !strings.Contains(out.Results[1].Message, "no running pods") {
		t.Fatalf("no-pod result %+v", out.Results[1])
	}
}

func TestContainerAndSelectorErrors(t *testing.T) {
	exec := &FakeExecutor{}
	r := runner(t, exec, pod("pg-0", corev1.PodRunning, pgLabels, "postgres"), pod("bare", corev1.PodRunning, map[string]string{"app": "bare"}))
	h := hook("c", "true", keeperv1.HookFail)
	h.Container = "nope"
	if out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{h}); !out.Failed || !strings.Contains(out.Message, `container "nope" not found`) {
		t.Fatalf("outcome %+v", out)
	}
	bare := hook("bare", "true", keeperv1.HookFail)
	bare.PodSelector.MatchLabels = map[string]string{"app": "bare"}
	if out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{bare}); !out.Failed || !strings.Contains(out.Message, "no containers") {
		t.Fatalf("outcome %+v", out)
	}
	empty := hook("all", "true", keeperv1.HookFail)
	empty.PodSelector = metav1.LabelSelector{}
	if out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{empty}); !out.Failed {
		t.Fatal("an empty selector must match nothing")
	}
	invalid := hook("bad", "true", keeperv1.HookFail)
	invalid.PodSelector = metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: "Sideways"}}}
	if out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{invalid}); !out.Failed || !strings.Contains(out.Message, "invalid pod selector") {
		t.Fatalf("outcome %+v", out)
	}
	if len(exec.Calls()) != 0 {
		t.Fatal("no command should have run")
	}
}

func TestExecErrorWithoutExitCode(t *testing.T) {
	exec := &FakeExecutor{Respond: func(ExecRequest) (ExecResult, error) { return ExecResult{}, errors.New("connection refused") }}
	r := runner(t, exec, pod("pg-0", corev1.PodRunning, pgLabels, "postgres"))
	h := hook("x", "true", keeperv1.HookFail)
	h.TimeoutSeconds = 0
	out := r.Run(context.Background(), "db", StagePre, []keeperv1.Hook{h})
	if !out.Failed || out.Results[0].ExitCode != nil || out.Results[0].Message != "connection refused" {
		t.Fatalf("outcome %+v", out.Results[0])
	}
	if exec.Calls()[0].Timeout != 30*time.Second {
		t.Fatal("default timeout not applied")
	}
}

func TestTruncateKeepsTail(t *testing.T) {
	long := strings.Repeat("a", 1000) + "ERROR"
	got := truncate(long)
	if len(got) != MaxMessage || !strings.HasSuffix(got, "ERROR") || !strings.HasPrefix(got, "...") {
		t.Fatalf("truncate len=%d", len(got))
	}
	if truncate("short") != "short" {
		t.Fatal("short strings unchanged")
	}
}

func TestExecURL(t *testing.T) {
	u, err := ExecURL(&rest.Config{Host: "https://10.0.0.1:6443"}, ExecRequest{Namespace: "db", Pod: "pg-0", Container: "postgres", Command: []string{"sh", "-c", "echo hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/api/v1/namespaces/db/pods/pg-0/exec" {
		t.Fatalf("path %s", u.Path)
	}
	q := u.Query()
	if !slices.Equal(q["command"], []string{"sh", "-c", "echo hi"}) || q.Get("container") != "postgres" || q.Get("stdout") != "true" {
		t.Fatalf("query %v", q)
	}
	if _, err := ExecURL(&rest.Config{Host: "http://[::1"}, ExecRequest{}); err == nil {
		t.Fatal("bad host accepted")
	}
}

func TestRemoteExecutorSurfacesTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()
	e := &RemoteExecutor{Config: &rest.Config{Host: srv.URL}}
	_, err := e.Exec(context.Background(), ExecRequest{Namespace: "db", Pod: "pg-0", Command: []string{"true"}, Timeout: 2 * time.Second})
	if err == nil || errors.Is(err, ErrNonZeroExit) {
		t.Fatalf("err = %v", err)
	}
}

func TestTailBuffer(t *testing.T) {
	var b tailBuffer
	_, _ = b.Write([]byte(strings.Repeat("x", 2000)))
	_, _ = b.Write([]byte("end"))
	if len(b.b) != MaxMessage*2 || !strings.HasSuffix(string(b.b), "end") {
		t.Fatalf("len %d", len(b.b))
	}
}
