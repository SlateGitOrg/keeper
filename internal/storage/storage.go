// Package storage abstracts the S3-compatible object store that receives
// backup data and manifests. Two implementations ship: Memory (tests, demo)
// and S3 (any S3/MinIO-compatible endpoint via minio-go).
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

// ErrNotFound is returned (possibly wrapped) when an object does not exist.
var ErrNotFound = errors.New("storage: object not found")

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// ObjectStore is the minimal object-storage surface the operator needs.
// Implementations must be safe for concurrent use.
type ObjectStore interface {
	// Put stores r under key. size is the exact length, or -1 if unknown.
	Put(ctx context.Context, key string, r io.Reader, size int64) (ObjectInfo, error)
	// Get opens key for reading. It returns ErrNotFound for a missing key.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Stat describes key. It returns ErrNotFound for a missing key.
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	// List returns every object whose key starts with prefix, sorted by key.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// Delete removes key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// PutJSON stores v as indented JSON.
func PutJSON(ctx context.Context, s ObjectStore, key string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("storage: encode %s: %w", key, err)
	}
	if _, err := s.Put(ctx, key, bytes.NewReader(b), int64(len(b))); err != nil {
		return fmt.Errorf("storage: put %s: %w", key, err)
	}
	return nil
}

// maxJSONObject bounds how much GetJSON reads, so a corrupt or hostile
// manifest cannot exhaust operator memory.
const maxJSONObject = 8 << 20

// GetJSON decodes the JSON object stored under key into v.
func GetJSON(ctx context.Context, s ObjectStore, key string, v any) error {
	rc, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, maxJSONObject+1))
	if err != nil {
		return fmt.Errorf("storage: read %s: %w", key, err)
	}
	if len(b) > maxJSONObject {
		return fmt.Errorf("storage: %s exceeds %d bytes", key, maxJSONObject)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("storage: decode %s: %w", key, err)
	}
	return nil
}

// DeletePrefix deletes every object under prefix and returns how many it
// deleted. It stops at the first error.
func DeletePrefix(ctx context.Context, s ObjectStore, prefix string) (int, error) {
	if prefix == "" || !strings.HasSuffix(prefix, "/") {
		return 0, fmt.Errorf("storage: refusing to delete prefix %q (must be non-empty and end in /)", prefix)
	}
	objs, err := s.List(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("storage: list %s: %w", prefix, err)
	}
	for i, o := range objs {
		if err := s.Delete(ctx, o.Key); err != nil {
			return i, fmt.Errorf("storage: delete %s: %w", o.Key, err)
		}
	}
	return len(objs), nil
}

// Layout maps backups to object keys:
//
//	<prefix>/backups/<namespace>/<backup>/backup.json
//	<prefix>/backups/<namespace>/<backup>/volumes/<pvc>.tar.gz
//	<prefix>/backups/<namespace>/<backup>/volumes/<pvc>.json
type Layout struct {
	Prefix string
}

// BackupDir is the key prefix holding everything that belongs to a backup.
func (l Layout) BackupDir(namespace, backup string) string {
	return path.Join(strings.Trim(l.Prefix, "/"), "backups", namespace, backup) + "/"
}

// BackupManifest is the key of a backup's manifest.
func (l Layout) BackupManifest(namespace, backup string) string {
	return l.BackupDir(namespace, backup) + "backup.json"
}

// Archive is the key of a volume archive.
func (l Layout) Archive(namespace, backup, pvc string) string {
	return l.BackupDir(namespace, backup) + "volumes/" + pvc + ".tar.gz"
}

// VolumeManifest is the key of the mover's per-volume manifest.
func (l Layout) VolumeManifest(namespace, backup, pvc string) string {
	return l.BackupDir(namespace, backup) + "volumes/" + pvc + ".json"
}
