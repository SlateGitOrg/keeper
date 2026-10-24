package mover

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/archit2501/keeper/internal/storage"
)

// Mode selects the mover direction.
type Mode string

// Mover modes.
const (
	ModeBackup  Mode = "backup"
	ModeRestore Mode = "restore"
)

// Environment variables the mover reads its storage configuration from. The
// operator injects them into mover Jobs; credentials come from the Secret via
// secretKeyRef so they never appear in the Job spec.
const (
	EnvEndpoint  = "KEEPER_S3_ENDPOINT"
	EnvBucket    = "KEEPER_S3_BUCKET"
	EnvRegion    = "KEEPER_S3_REGION"
	EnvAccessKey = "AWS_ACCESS_KEY_ID"
	EnvSecretKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // variable name, not a credential.
)

// MountPath is where mover Jobs mount the volume.
const MountPath = "/data"

// Args are the command-line arguments of a mover Job. Args/ParseArgs are the
// single source of truth shared by the operator (which builds Jobs) and the
// mover binary (which runs them).
type Args struct {
	Mode        Mode
	Path        string
	PVCName     string
	ArchiveKey  string
	ManifestKey string
	SHA256      string
}

// Argv renders a as arguments for "keeper mover".
func (a Args) Argv() []string {
	argv := []string{"--mode=" + string(a.Mode), "--path=" + a.Path, "--archive-key=" + a.ArchiveKey}
	if a.PVCName != "" {
		argv = append(argv, "--pvc="+a.PVCName)
	}
	if a.ManifestKey != "" {
		argv = append(argv, "--manifest-key="+a.ManifestKey)
	}
	if a.SHA256 != "" {
		argv = append(argv, "--sha256="+a.SHA256)
	}
	return argv
}

// ParseArgs parses "keeper mover" arguments.
func ParseArgs(argv []string) (Args, error) {
	var a Args
	fs := flag.NewFlagSet("mover", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", "", "backup or restore")
	fs.StringVar(&a.Path, "path", MountPath, "volume mount path")
	fs.StringVar(&a.PVCName, "pvc", "", "source claim name (recorded in the manifest)")
	fs.StringVar(&a.ArchiveKey, "archive-key", "", "object key of the archive")
	fs.StringVar(&a.ManifestKey, "manifest-key", "", "object key of the volume manifest (backup)")
	fs.StringVar(&a.SHA256, "sha256", "", "expected archive SHA-256 (restore)")
	if err := fs.Parse(argv); err != nil {
		return a, fmt.Errorf("mover: %w", err)
	}
	a.Mode = Mode(*mode)
	switch {
	case a.Mode != ModeBackup && a.Mode != ModeRestore:
		return a, fmt.Errorf("mover: --mode must be backup or restore, got %q", *mode)
	case a.ArchiveKey == "":
		return a, errors.New("mover: --archive-key is required")
	case a.Mode == ModeBackup && a.ManifestKey == "":
		return a, errors.New("mover: --manifest-key is required for backup")
	}
	return a, nil
}

// StoreFromEnv builds the S3 store a mover Job was configured with.
func StoreFromEnv(getenv func(string) string) (storage.ObjectStore, error) {
	return storage.NewS3(storage.S3Config{
		Endpoint:        getenv(EnvEndpoint),
		Bucket:          getenv(EnvBucket),
		Region:          getenv(EnvRegion),
		AccessKeyID:     getenv(EnvAccessKey),
		SecretAccessKey: getenv(EnvSecretKey),
	})
}

// Execute runs one mover invocation against store.
func Execute(ctx context.Context, store storage.ObjectStore, a Args, log *slog.Logger) error {
	switch a.Mode {
	case ModeBackup:
		m, err := Backup(ctx, store, BackupOptions{Source: a.Path, PVCName: a.PVCName, ArchiveKey: a.ArchiveKey, ManifestKey: a.ManifestKey})
		if err != nil {
			return err
		}
		log.Info("backup complete", "pvc", m.PVCName, "entries", m.Entries, "dataBytes", m.DataBytes,
			"archiveBytes", m.ArchiveBytes, "skipped", m.Skipped, "sha256", m.SHA256)
	case ModeRestore:
		st, err := Restore(ctx, store, RestoreOptions{ArchiveKey: a.ArchiveKey, ExpectedSHA256: a.SHA256, Dest: a.Path})
		if err != nil {
			return err
		}
		log.Info("restore complete", "entries", st.Entries, "dataBytes", st.DataBytes, "skipped", st.Skipped)
	default:
		return fmt.Errorf("mover: unknown mode %q", a.Mode)
	}
	return nil
}
