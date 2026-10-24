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
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/archit2501/keeper/internal/storage"
)

// RestoreOptions configures Restore.
type RestoreOptions struct {
	// ArchiveKey is the object to extract.
	ArchiveKey string
	// ExpectedSHA256 must match the archive; empty skips verification.
	ExpectedSHA256 string
	// Dest is the mounted (empty) target volume root.
	Dest string
}

// RestoreStats summarises an extraction.
type RestoreStats struct {
	Entries   int64
	DataBytes int64
	Skipped   int64
}

// ErrChecksum is returned when the archive does not match its recorded hash.
var ErrChecksum = errors.New("mover: archive checksum mismatch")

// ErrUnsafePath is returned for archive entries that would escape Dest.
var ErrUnsafePath = errors.New("mover: unsafe path in archive")

// SafePath validates a tar entry name and returns it as a relative,
// OS-specific path that cannot escape the extraction root.
func SafePath(name string) (string, error) {
	if name == "" || strings.ContainsRune(name, 0) || strings.HasPrefix(name, "/") || strings.Contains(name, `\`) {
		return "", fmt.Errorf("%w: %q", ErrUnsafePath, name)
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: %q", ErrUnsafePath, name)
	}
	return filepath.FromSlash(clean), nil
}

// checkParents refuses to write beneath a symlink: an archive could otherwise
// plant "a -> /etc" and then write "a/passwd".
func checkParents(root, rel string) error {
	dir := filepath.Dir(rel)
	if dir == "." {
		return nil
	}
	cur := root
	for _, part := range strings.Split(dir, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
			return fmt.Errorf("%w: %q has a non-directory parent", ErrUnsafePath, rel)
		}
	}
	return nil
}

// Restore downloads an archive and extracts it into opts.Dest. Entries are
// first extracted into a staging directory inside Dest; only after the whole
// stream was read and its SHA-256 verified are they moved into place, so a
// corrupt archive leaves Dest untouched. Existing entries in Dest are never
// overwritten.
func Restore(ctx context.Context, store storage.ObjectStore, opts RestoreOptions) (RestoreStats, error) {
	var st RestoreStats
	rc, err := store.Get(ctx, opts.ArchiveKey)
	if err != nil {
		return st, fmt.Errorf("mover: open %s: %w", opts.ArchiveKey, err)
	}
	defer func() { _ = rc.Close() }()

	staging, err := os.MkdirTemp(opts.Dest, ".keeper-restore-")
	if err != nil {
		return st, fmt.Errorf("mover: staging dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()

	hasher := sha256.New()
	st, err = extract(ctx, io.TeeReader(rc, hasher), staging)
	if err != nil {
		return st, err
	}
	// Hash any trailing bytes (gzip may stop before EOF of the object).
	if _, err := io.Copy(hasher, rc); err != nil {
		return st, fmt.Errorf("mover: read %s: %w", opts.ArchiveKey, err)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); opts.ExpectedSHA256 != "" && sum != opts.ExpectedSHA256 {
		return st, fmt.Errorf("%w: got %s, want %s", ErrChecksum, sum, opts.ExpectedSHA256)
	}

	entries, err := os.ReadDir(staging)
	if err != nil {
		return st, err
	}
	for _, e := range entries {
		dst := filepath.Join(opts.Dest, e.Name())
		if _, err := os.Lstat(dst); err == nil {
			return st, fmt.Errorf("mover: refusing to overwrite existing %s", dst)
		}
		if err := os.Rename(filepath.Join(staging, e.Name()), dst); err != nil {
			return st, fmt.Errorf("mover: move %s into place: %w", e.Name(), err)
		}
	}
	return st, nil
}

type dirTime struct {
	path string
	mod  time.Time
}

func extract(ctx context.Context, r io.Reader, root string) (RestoreStats, error) {
	var st RestoreStats
	gz, err := gzip.NewReader(r)
	if err != nil {
		return st, fmt.Errorf("mover: gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	var dirs []dirTime
	asRoot := os.Geteuid() == 0
	for {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return st, fmt.Errorf("mover: tar: %w", err)
		}
		rel, err := SafePath(hdr.Name)
		if err != nil {
			return st, err
		}
		if err := checkParents(root, rel); err != nil {
			return st, err
		}
		target := filepath.Join(root, rel)
		// Strip setuid/setgid/sticky bits; restored data should not gain privileges.
		perm := os.FileMode(hdr.Mode) & os.ModePerm //nolint:gosec // masked to permission bits.
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return st, err
			}
			if err := os.Chmod(target, perm|0o700); err != nil {
				return st, err
			}
			dirs = append(dirs, dirTime{target, hdr.ModTime})
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return st, err
			}
			if err := writeFile(target, tr, perm, hdr.Size); err != nil {
				return st, err
			}
			_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
			st.DataBytes += hdr.Size
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return st, err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return st, err
			}
		default:
			st.Skipped++ // hard links, devices, fifos: never produced by Backup
			continue
		}
		if asRoot {
			_ = os.Lchown(target, hdr.Uid, hdr.Gid)
		}
		st.Entries++
	}
	// Directory mtimes last, deepest first, because writing children bumps them.
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Chtimes(dirs[i].path, dirs[i].mod, dirs[i].mod)
	}
	return st, nil
}

func writeFile(target string, r io.Reader, perm os.FileMode, size int64) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, perm)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(f, r, size); err != nil {
		_ = f.Close()
		return fmt.Errorf("mover: write %s: %w", target, err)
	}
	return f.Close()
}
