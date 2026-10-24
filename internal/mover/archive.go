// Package mover is the data path of the FileCopy driver. Inside a mover Job it
// streams a volume into a gzip-compressed tar archive in object storage
// (Backup) or extracts such an archive into an empty volume (Restore).
//
// Archives are written in a single pass: the tar stream is gzip-compressed,
// hashed (SHA-256) and uploaded concurrently through an io.Pipe, so memory use
// is bounded by the upload part size regardless of volume size.
package mover

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/archit2501/keeper/internal/storage"
)

// ManifestVersion is the schema version of VolumeManifest.
const ManifestVersion = 1

// VolumeManifest is written next to each archive and read back by the
// operator to fill Backup status.
type VolumeManifest struct {
	Version      int       `json:"version"`
	PVCName      string    `json:"pvcName"`
	ArchiveKey   string    `json:"archiveKey"`
	ArchiveBytes int64     `json:"archiveBytes"`
	DataBytes    int64     `json:"dataBytes"`
	Entries      int64     `json:"entries"`
	Skipped      int64     `json:"skipped"`
	SHA256       string    `json:"sha256"`
	StartedAt    time.Time `json:"startedAt"`
	FinishedAt   time.Time `json:"finishedAt"`
}

// BackupOptions configures Backup.
type BackupOptions struct {
	// Source is the mounted volume root.
	Source string
	// PVCName is recorded in the manifest.
	PVCName string
	// ArchiveKey and ManifestKey are the object keys to write.
	ArchiveKey, ManifestKey string
	// Now is the clock (time.Now if nil).
	Now func() time.Time
}

// ErrFileChanged is returned when a file shrinks while it is being archived.
// Run pre hooks that quiesce writers to avoid it.
var ErrFileChanged = errors.New("mover: file changed while being archived")

// skipAtRoot lists filesystem artefacts that are never archived.
var skipAtRoot = map[string]bool{"lost+found": true}

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// Backup archives opts.Source into store and writes the manifest.
func Backup(ctx context.Context, store storage.ObjectStore, opts BackupOptions) (VolumeManifest, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	m := VolumeManifest{Version: ManifestVersion, PVCName: opts.PVCName, ArchiveKey: opts.ArchiveKey, StartedAt: now().UTC()}
	if fi, err := os.Stat(opts.Source); err != nil || !fi.IsDir() {
		return m, fmt.Errorf("mover: source %q is not a directory: %w", opts.Source, err)
	}

	pr, pw := io.Pipe()
	hasher := sha256.New()
	counter := &countingWriter{}
	type result struct {
		stats archiveStats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		stats, err := writeArchive(ctx, opts.Source, io.MultiWriter(pw, hasher, counter))
		_ = pw.CloseWithError(err)
		done <- result{stats, err}
	}()

	_, putErr := store.Put(ctx, opts.ArchiveKey, pr, -1)
	// Unblock the writer if the upload stopped reading early.
	_ = pr.CloseWithError(fmt.Errorf("mover: upload ended: %w", putErr))
	res := <-done
	if res.err != nil {
		return m, res.err
	}
	if putErr != nil {
		return m, fmt.Errorf("mover: upload %s: %w", opts.ArchiveKey, putErr)
	}

	m.ArchiveBytes = counter.n
	m.DataBytes = res.stats.dataBytes
	m.Entries = res.stats.entries
	m.Skipped = res.stats.skipped
	m.SHA256 = hex.EncodeToString(hasher.Sum(nil))
	m.FinishedAt = now().UTC()
	if err := storage.PutJSON(ctx, store, opts.ManifestKey, m); err != nil {
		return m, err
	}
	return m, nil
}

type archiveStats struct {
	entries, skipped, dataBytes int64
}

// writeArchive walks root in lexical order and writes a PAX tar, gzip
// compressed, to w. Symlinks are stored, never followed.
func writeArchive(ctx context.Context, root string, w io.Writer) (archiveStats, error) {
	var st archiveStats
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	walkErr := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skipAtRoot[rel] {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		if !mode.IsRegular() && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
			st.skipped++ // sockets, devices, pipes
			return nil
		}
		link := ""
		if mode&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if mode.IsDir() {
			hdr.Name += "/"
		}
		hdr.Format = tar.FormatPAX
		// Access/change times churn on every read; dropping them makes archives
		// of an unchanged tree byte-identical.
		hdr.AccessTime, hdr.ChangeTime = time.Time{}, time.Time{}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		st.entries++
		if mode.IsRegular() {
			if err := copyFile(p, tw, hdr.Size); err != nil {
				return err
			}
			st.dataBytes += hdr.Size
		}
		return nil
	})
	if walkErr != nil {
		return st, fmt.Errorf("mover: archive %s: %w", root, walkErr)
	}
	if err := tw.Close(); err != nil {
		return st, fmt.Errorf("mover: finish tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return st, fmt.Errorf("mover: finish gzip: %w", err)
	}
	return st, nil
}

// copyFile copies exactly size bytes of p (the size recorded in the header).
// Growth after the header was written is ignored; shrinkage is an error.
func copyFile(p string, w io.Writer, size int64) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.CopyN(w, f, size); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: %s", ErrFileChanged, p)
		}
		return err
	}
	return nil
}
