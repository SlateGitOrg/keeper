// Command keeper is the operator binary. It has three modes:
//
//	keeper manager [flags]          run the controllers
//	keeper mover --mode=... [flags] run inside a mover Job (FileCopy driver)
//	keeper retention [flags]        preview a GFS policy over timestamps on stdin
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata" // schedules and retention need zone data in distroless images

	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/SlateGitOrg/keeper/internal/hooks"
	"github.com/SlateGitOrg/keeper/internal/mover"
	"github.com/SlateGitOrg/keeper/internal/operator"
	"github.com/SlateGitOrg/keeper/internal/retention"
	"github.com/SlateGitOrg/keeper/internal/storage"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "keeper:", err)
		os.Exit(1)
	}
}

const usage = `usage: keeper <command> [flags]

commands:
  manager     run the BackupPolicy, Backup and Restore controllers
  mover       run the FileCopy data mover (used inside mover Jobs)
  retention   preview which timestamps (RFC 3339, one per line on stdin) a GFS policy keeps
  version     print the version`

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return errors.New("missing command")
	}
	switch args[0] {
	case "manager":
		return runManager(args[1:])
	case "mover":
		return runMover(args[1:], stderr)
	case "retention":
		return runRetention(args[1:], stdin, stdout)
	case "version":
		_, err := fmt.Fprintln(stdout, version)
		return err
	case "-h", "--help", "help":
		_, err := fmt.Fprintln(stdout, usage)
		return err
	}
	_, _ = fmt.Fprintln(stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func runManager(args []string) error {
	fs := flag.NewFlagSet("manager", flag.ContinueOnError)
	metricsAddr := fs.String("metrics-bind-address", ":8080", "address the metrics endpoint binds to (0 disables)")
	probeAddr := fs.String("health-probe-bind-address", ":8081", "address the health probes bind to")
	leaderElect := fs.Bool("leader-elect", true, "enable leader election so only one replica reconciles")
	moverImage := fs.String("mover-image", envOr("KEEPER_MOVER_IMAGE", "ghcr.io/SlateGitOrg/keeper:"+version), "image used by mover Jobs")
	concurrency := fs.Int("max-concurrent-backups", 4, "Backups reconciled in parallel (hooks run inline)")
	poll := fs.Duration("poll-interval", 5*time.Second, "safety-net requeue while waiting for snapshots and Jobs")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	level := zapcore.InfoLevel
	if *debug {
		level = zapcore.DebugLevel
	}
	ctrl.SetLogger(zap.New(zap.Level(level)))
	log := ctrl.Log.WithName("setup")

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	scheme, err := operator.NewScheme()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       "keeper.backup.keeper.dev",
	})
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := operator.Setup(ctx, mgr, operator.Options{
		Stores:               storage.SecretFactory{Client: mgr.GetClient()},
		Exec:                 &hooks.RemoteExecutor{Config: cfg},
		MoverImage:           *moverImage,
		PollInterval:         *poll,
		MaxConcurrentBackups: *concurrency,
	}); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	log.Info("starting manager", "version", version, "moverImage", *moverImage, "csi", operator.DetectCSI(mgr.GetRESTMapper()))
	return mgr.Start(ctx)
}

func runMover(args []string, stderr io.Writer) error {
	a, err := mover.ParseArgs(args)
	if err != nil {
		return err
	}
	store, err := mover.StoreFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return mover.Execute(ctx, store, a, mover.Runtime{Log: slog.New(slog.NewJSONHandler(stderr, nil))})
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runRetention(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("retention", flag.ContinueOnError)
	var p retention.Policy
	fs.IntVar(&p.Last, "keep-last", 0, "keep the N newest")
	fs.IntVar(&p.Hourly, "keep-hourly", 0, "keep the newest of the last N hours")
	fs.IntVar(&p.Daily, "keep-daily", 0, "keep the newest of the last N days")
	fs.IntVar(&p.Weekly, "keep-weekly", 0, "keep the newest of the last N ISO weeks")
	fs.IntVar(&p.Monthly, "keep-monthly", 0, "keep the newest of the last N months")
	fs.IntVar(&p.Yearly, "keep-yearly", 0, "keep the newest of the last N years")
	tz := fs.String("time-zone", "UTC", "IANA zone periods are computed in")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return previewRetention(p, *tz, stdin, stdout)
}
