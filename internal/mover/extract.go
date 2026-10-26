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

	"golang.org/x/sys/unix"

	"github.com/SlateGitOrg/keeper/internal/storage"
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

// Restore downloads an archive and extracts it into opts.Dest.
//
// With ExpectedSHA256 set, the object is first streamed through SHA-256
// without touching the filesystem, so a tampered or corrupt archive is
// rejected before a single entry is extracted. The second read extracts into
// a staging directory inside Dest and is hashed again, which catches an
// object replaced between the two reads. Only then are entries moved into
// place, so a bad archive leaves Dest untouched. Existing entries in Dest are
// never overwritten.
func Restore(ctx context.Context, store storage.ObjectStore, opts RestoreOptions) (RestoreStats, error) {
	var st RestoreStats
	if opts.ExpectedSHA256 != "" {
		if err := verifyObject(ctx, store, opts.ArchiveKey, opts.ExpectedSHA256); err != nil {
			return st, err
		}
	}
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
	if err := checkSum(hasher.Sum(nil), opts.ExpectedSHA256); err != nil {
		return st, err
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

// verifyObject streams key through SHA-256 and compares it with want.
func verifyObject(ctx context.Context, store storage.ObjectStore, key, want string) error {
	rc, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("mover: open %s: %w", key, err)
	}
	defer func() { _ = rc.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, rc); err != nil {
		return fmt.Errorf("mover: read %s: %w", key, err)
	}
	return checkSum(hasher.Sum(nil), want)
}

func checkSum(sum []byte, want string) error {
	if got := hex.EncodeToString(sum); want != "" && got != want {
		return fmt.Errorf("%w: got %s, want %s", ErrChecksum, got, want)
	}
	return nil
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
			if err := makeDir(target, rel, perm|0o700); err != nil {
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
			_ = setTimes(target, hdr.ModTime)
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
		_ = setTimes(dirs[i].path, dirs[i].mod)
	}
	return st, nil
}

// makeDir creates target as a directory, or accepts an existing real
// directory, and sets its mode. It never follows a symlink: an archive could
// otherwise plant "d -> /somewhere" and then send a directory entry "d" to
// chmod the link's target. The mode is set through a descriptor opened with
// O_NOFOLLOW|O_DIRECTORY, so only the directory itself is changed.
func makeDir(target, rel string, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(target, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	fd, err := unix.Open(target, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		return fmt.Errorf("%w: %q exists and is not a directory", ErrUnsafePath, rel)
	}
	if err != nil {
		return &os.PathError{Op: "open", Path: target, Err: err}
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Fchmod(fd, uint32(perm)); err != nil {
		return &os.PathError{Op: "chmod", Path: target, Err: err}
	}
	return nil
}

// setTimes sets atime and mtime of p itself, never of a symlink's target.
func setTimes(p string, t time.Time) error {
	ts := unix.NsecToTimespec(t.UnixNano())
	return unix.UtimesNanoAt(unix.AT_FDCWD, p, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
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
