package hooks

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// FakeExecutor records requests and answers them with a programmable
// function. It is used by tests and the offline demo, where no kubelet exists.
type FakeExecutor struct {
	mu    sync.Mutex
	calls []ExecRequest
	// Respond decides the outcome; nil means "exit 0, no output".
	Respond func(ExecRequest) (ExecResult, error)
}

// Exec implements Executor.
func (f *FakeExecutor) Exec(_ context.Context, req ExecRequest) (ExecResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	respond := f.Respond
	f.mu.Unlock()
	if respond == nil {
		return ExecResult{}, nil
	}
	return respond(req)
}

// Calls returns a copy of the recorded requests.
func (f *FakeExecutor) Calls() []ExecRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ExecRequest(nil), f.calls...)
}

// CommandLines renders recorded calls as "pod: cmd args" strings.
func (f *FakeExecutor) CommandLines() []string {
	var out []string
	for _, c := range f.Calls() {
		out = append(out, c.Pod+": "+strings.Join(c.Command, " "))
	}
	return out
}

// FailWhen returns a Respond function that exits 1 with stderr msg for
// commands whose joined text contains substr.
func FailWhen(substr, msg string) func(ExecRequest) (ExecResult, error) {
	return func(req ExecRequest) (ExecResult, error) {
		if strings.Contains(strings.Join(req.Command, " "), substr) {
			return ExecResult{Stderr: msg, ExitCode: 1}, fmt.Errorf("%w (code 1)", ErrNonZeroExit)
		}
		return ExecResult{Stdout: "ok"}, nil
	}
}
