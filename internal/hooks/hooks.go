// Package hooks runs the pre/post snapshot commands that make a Backup
// application-consistent (for example CHECKPOINT + fsfreeze before the cut,
// fsfreeze -u after it) by exec-ing into the workload's pods.
package hooks

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
)

// Stage is "pre" or "post".
type Stage string

// Hook stages.
const (
	StagePre  Stage = "pre"
	StagePost Stage = "post"
)

// MaxMessage bounds the hook output kept in status.
const MaxMessage = 512

// ExecRequest is one command execution in one container.
type ExecRequest struct {
	Namespace, Pod, Container string
	Command                   []string
	Timeout                   time.Duration
}

// ExecResult is the captured output of a finished command.
type ExecResult struct {
	Stdout, Stderr string
	// ExitCode is 0 on success. It is meaningful whenever the command ran.
	ExitCode int
}

// ErrNonZeroExit is wrapped by executors when the command ran but failed.
var ErrNonZeroExit = errors.New("hooks: command exited non-zero")

// Executor runs commands inside pods.
type Executor interface {
	// Exec runs req. A command that ran and exited non-zero returns a result
	// with ExitCode set and an error wrapping ErrNonZeroExit; any other error
	// means the command could not be run.
	Exec(ctx context.Context, req ExecRequest) (ExecResult, error)
}

// Runner selects pods and runs hooks with their error policies.
type Runner struct {
	Pods client.Reader
	Exec Executor
	Now  func() time.Time
}

// Outcome is the result of running one stage.
type Outcome struct {
	// Results has one entry per hook per pod (or per hook when no pod matched).
	Results []keeperv1.HookResult
	// Failed is true if a hook with OnError=Fail failed.
	Failed bool
	// Message describes the first Fail-policy failure.
	Message string
}

// Run executes hooks in order in namespace. Pre hooks stop at the first
// Fail-policy failure, because quiescing more of the application after a
// failure is pointless. Post hooks always all run, because each one may be
// resuming something a pre hook stopped.
func (r *Runner) Run(ctx context.Context, namespace string, stage Stage, hs []keeperv1.Hook) Outcome {
	var out Outcome
	for _, h := range hs {
		results, err := r.runHook(ctx, namespace, stage, h)
		out.Results = append(out.Results, results...)
		if err == nil || h.OnError == keeperv1.HookContinue {
			continue
		}
		if !out.Failed {
			out.Failed = true
			out.Message = fmt.Sprintf("%s hook %q: %v", stage, h.Name, err)
		}
		if stage == StagePre {
			break
		}
	}
	return out
}

func (r *Runner) now() metav1.Time {
	if r.Now != nil {
		return metav1.NewTime(r.Now())
	}
	return metav1.Now()
}

// runHook runs h in every matching running pod (sorted by name) and returns
// the first error.
func (r *Runner) runHook(ctx context.Context, namespace string, stage Stage, h keeperv1.Hook) ([]keeperv1.HookResult, error) {
	pods, err := r.selectPods(ctx, namespace, h.PodSelector)
	if err != nil || len(pods) == 0 {
		if err == nil {
			err = errors.New("no running pods match the selector")
		}
		return []keeperv1.HookResult{{Hook: h.Name, Stage: string(stage), Succeeded: false, Message: truncate(err.Error()), Time: r.now()}}, err
	}
	timeout := time.Duration(h.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	var results []keeperv1.HookResult
	var firstErr error
	for _, pod := range pods {
		res := keeperv1.HookResult{Hook: h.Name, Stage: string(stage), Pod: pod.Name}
		container, cerr := pickContainer(pod, h.Container)
		var execErr error
		if cerr != nil {
			execErr = cerr
		} else {
			er, err := r.Exec.Exec(ctx, ExecRequest{Namespace: namespace, Pod: pod.Name, Container: container, Command: h.Command, Timeout: timeout})
			execErr = err
			if err == nil || errors.Is(err, ErrNonZeroExit) {
				code := int32(er.ExitCode) //nolint:gosec // exit codes fit in int32.
				res.ExitCode = &code
			}
			res.Message = truncate(firstNonEmpty(er.Stderr, er.Stdout))
		}
		res.Succeeded = execErr == nil
		if execErr != nil {
			res.Message = truncate(firstNonEmpty(res.Message, execErr.Error()))
			if firstErr == nil {
				firstErr = fmt.Errorf("pod %s: %w", pod.Name, execErr)
				if res.Message != execErr.Error() {
					firstErr = fmt.Errorf("%w: %s", firstErr, res.Message)
				}
			}
		}
		res.Time = r.now()
		results = append(results, res)
	}
	return results, firstErr
}

func (r *Runner) selectPods(ctx context.Context, namespace string, sel metav1.LabelSelector) ([]corev1.Pod, error) {
	s, err := metav1.LabelSelectorAsSelector(&sel)
	if err != nil {
		return nil, fmt.Errorf("invalid pod selector: %w", err)
	}
	if s.Empty() {
		s = labels.Nothing() // an empty selector must not exec into every pod
	}
	var list corev1.PodList
	if err := r.Pods.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: s}); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	var pods []corev1.Pod
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			pods = append(pods, p)
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

func pickContainer(pod corev1.Pod, want string) (string, error) {
	if len(pod.Spec.Containers) == 0 {
		return "", errors.New("pod has no containers")
	}
	if want == "" {
		return pod.Spec.Containers[0].Name, nil
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == want {
			return want, nil
		}
	}
	return "", fmt.Errorf("container %q not found", want)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// truncate keeps the tail of s, where errors usually are.
func truncate(s string) string {
	if len(s) <= MaxMessage {
		return s
	}
	return "..." + s[len(s)-MaxMessage+3:]
}
