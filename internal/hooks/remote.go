package hooks

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream"
)

// RemoteExecutor runs commands through the pods/exec subresource, preferring
// the WebSocket protocol and falling back to SPDY for older API servers.
type RemoteExecutor struct {
	Config *rest.Config
}

// ExecURL builds the pods/exec URL for req.
func ExecURL(cfg *rest.Config, req ExecRequest) (*url.URL, error) {
	base, _, err := rest.DefaultServerUrlFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("hooks: api server url: %w", err)
	}
	u := base.JoinPath("api", "v1", "namespaces", req.Namespace, "pods", req.Pod, "exec")
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	q := url.Values{"stdout": {"true"}, "stderr": {"true"}}
	if req.Container != "" {
		q.Set("container", req.Container)
	}
	for _, c := range req.Command {
		q.Add("command", c)
	}
	u.RawQuery = q.Encode()
	return u, nil
}

// tailBuffer keeps the last MaxMessage*2 bytes written to it.
type tailBuffer struct{ b []byte }

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if limit := MaxMessage * 2; len(t.b) > limit {
		t.b = t.b[len(t.b)-limit:]
	}
	return len(p), nil
}

// Exec implements Executor.
func (e *RemoteExecutor) Exec(ctx context.Context, req ExecRequest) (ExecResult, error) {
	u, err := ExecURL(e.Config, req)
	if err != nil {
		return ExecResult{}, err
	}
	ws, err := remotecommand.NewWebSocketExecutor(e.Config, "GET", u.String())
	if err != nil {
		return ExecResult{}, fmt.Errorf("hooks: websocket executor: %w", err)
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.Config, "POST", u)
	if err != nil {
		return ExecResult{}, fmt.Errorf("hooks: spdy executor: %w", err)
	}
	ex, err := remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return ExecResult{}, fmt.Errorf("hooks: executor: %w", err)
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	var stdout, stderr tailBuffer
	err = ex.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	res := ExecResult{Stdout: string(stdout.b), Stderr: string(stderr.b)}
	var exitErr utilexec.ExitError
	switch {
	case err == nil:
		return res, nil
	case errors.As(err, &exitErr):
		res.ExitCode = exitErr.ExitStatus()
		return res, fmt.Errorf("%w (code %d)", ErrNonZeroExit, res.ExitCode)
	case ctx.Err() != nil:
		return res, fmt.Errorf("hooks: timed out after %s: %w", req.Timeout, ctx.Err())
	default:
		return res, fmt.Errorf("hooks: exec: %w", err)
	}
}
