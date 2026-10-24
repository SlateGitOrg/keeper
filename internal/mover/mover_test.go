package mover

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/archit2501/keeper/internal/storage"
)

// tree returns path -> description for every entry under root (excluding root).
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		info, _ := d.Info()
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			l, _ := os.Readlink(p)
			out[rel] = "link:" + l
		case info.IsDir():
			out[rel] = "dir:" + info.Mode().Perm().String()
		case !info.Mode().IsRegular():
			out[rel] = "special"
		default:
			b, _ := os.ReadFile(p) //nolint:gosec // test fixture
			out[rel] = "file:" + info.Mode().Perm().String() + ":" + string(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func writeFixture(t *testing.T, root string) {
	t.Helper()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(root, "pgdata", "base"), 0o750))
	must(os.WriteFile(filepath.Join(root, "pgdata", "PG_VERSION"), []byte("17\n"), 0o600))
	must(os.WriteFile(filepath.Join(root, "pgdata", "base", "16384"), bytes.Repeat([]byte("row"), 5000), 0o640))
	must(os.WriteFile(filepath.Join(root, "empty"), nil, 0o644))
	must(os.Symlink("pgdata/PG_VERSION", filepath.Join(root, "version-link")))
	must(os.MkdirAll(filepath.Join(root, "lost+found"), 0o700))
	must(os.WriteFile(filepath.Join(root, "lost+found", "junk"), []byte("x"), 0o600))
	must(syscall.Mkfifo(filepath.Join(root, "fifo"), 0o600))
	old := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	must(os.Chtimes(filepath.Join(root, "pgdata", "PG_VERSION"), old, old))
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, dst := t.TempDir(), t.TempDir()
	writeFixture(t, src)
	store := storage.NewMemory()
	clock := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	m, err := Backup(ctx, store, BackupOptions{Source: src, PVCName: "data", ArchiveKey: "a.tar.gz", ManifestKey: "a.json", Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	if m.Entries != 6 || m.Skipped != 1 || m.DataBytes != 15003 {
		t.Fatalf("manifest counts = %+v", m)
	}
	info, _ := store.Stat(ctx, "a.tar.gz")
	if m.ArchiveBytes != info.Size || m.ArchiveBytes >= m.DataBytes {
		t.Fatalf("archive bytes %d (stored %d) should be compressed below %d", m.ArchiveBytes, info.Size, m.DataBytes)
	}
	var stored VolumeManifest
	if err := storage.GetJSON(ctx, store, "a.json", &stored); err != nil || stored.SHA256 != m.SHA256 || !stored.StartedAt.Equal(clock) {
		t.Fatalf("stored manifest %+v, %v", stored, err)
	}

	st, err := Restore(ctx, store, RestoreOptions{ArchiveKey: "a.tar.gz", ExpectedSHA256: m.SHA256, Dest: dst})
	if err != nil {
		t.Fatal(err)
	}
	if st.Entries != m.Entries || st.DataBytes != m.DataBytes {
		t.Fatalf("restore stats %+v vs manifest %+v", st, m)
	}
	want := tree(t, src)
	delete(want, "fifo")
	delete(want, "lost+found")
	delete(want, filepath.Join("lost+found", "junk"))
	if got := tree(t, dst); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored tree differs\n got %v\nwant %v", got, want)
	}
	fi, _ := os.Stat(filepath.Join(dst, "pgdata", "PG_VERSION"))
	if !fi.ModTime().Equal(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("mtime not preserved: %v", fi.ModTime())
	}
}

func TestBackupIsDeterministic(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFixture(t, src)
	store := storage.NewMemory()
	a, err := Backup(ctx, store, BackupOptions{Source: src, ArchiveKey: "1", ManifestKey: "1.json"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Backup(ctx, store, BackupOptions{Source: src, ArchiveKey: "2", ManifestKey: "2.json"})
	if err != nil {
		t.Fatal(err)
	}
	if a.SHA256 != b.SHA256 {
		t.Fatal("archiving an unchanged tree twice should give identical bytes")
	}
}

func TestBackupErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := Backup(ctx, storage.NewMemory(), BackupOptions{Source: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("missing source accepted")
	}
	src := t.TempDir()
	writeFixture(t, src)
	failing := storage.NewMemory()
	failing.Inject(func(op, _ string) error {
		if op == "put" {
			return errors.New("bucket gone")
		}
		return nil
	})
	if _, err := Backup(ctx, failing, BackupOptions{Source: src, ArchiveKey: "a", ManifestKey: "m"}); err == nil || !strings.Contains(err.Error(), "bucket gone") {
		t.Fatalf("upload failure not surfaced: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Backup(cctx, storage.NewMemory(), BackupOptions{Source: src, ArchiveKey: "a", ManifestKey: "m"}); err == nil {
		t.Fatal("cancelled context ignored")
	}
}

func TestCopyFileDetectsShrink(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(p, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(p, io.Discard, 10); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("err = %v", err)
	}
}

func TestRestoreChecksumMismatchLeavesDestUntouched(t *testing.T) {
	ctx := context.Background()
	src, dst := t.TempDir(), t.TempDir()
	writeFixture(t, src)
	store := storage.NewMemory()
	if _, err := Backup(ctx, store, BackupOptions{Source: src, ArchiveKey: "a", ManifestKey: "m"}); err != nil {
		t.Fatal(err)
	}
	_, err := Restore(ctx, store, RestoreOptions{ArchiveKey: "a", ExpectedSHA256: strings.Repeat("0", 64), Dest: dst})
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v", err)
	}
	if entries, _ := os.ReadDir(dst); len(entries) != 0 {
		t.Fatalf("dest not empty after failed restore: %v", entries)
	}
}

func TestRestoreRefusesToOverwrite(t *testing.T) {
	ctx := context.Background()
	src, dst := t.TempDir(), t.TempDir()
	writeFixture(t, src)
	store := storage.NewMemory()
	if _, err := Backup(ctx, store, BackupOptions{Source: src, ArchiveKey: "a", ManifestKey: "m"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "empty"), []byte("precious"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, store, RestoreOptions{ArchiveKey: "a", Dest: dst}); err == nil {
		t.Fatal("overwrote an existing file")
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "empty")); string(b) != "precious" { //nolint:gosec // test path
		t.Fatal("existing file modified")
	}
}

type entry struct {
	name, body, link string
	typ              byte
	mode             int64
}

func archiveOf(t testing.TB, entries ...entry) []byte {
	t.Helper()
	b, err := tryArchive(entries...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func tryArchive(entries ...entry) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o644
		}
		hdr := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: mode, Size: int64(len(e.body)), Linkname: e.link}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write([]byte(e.body)); err != nil {
			return nil, err
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes(), nil
}

func restoreBytes(t testing.TB, data []byte, dst string) error {
	t.Helper()
	store := storage.NewMemory()
	if _, err := store.Put(context.Background(), "x", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	_, err := Restore(context.Background(), store, RestoreOptions{ArchiveKey: "x", ExpectedSHA256: hex.EncodeToString(sum[:]), Dest: dst})
	return err
}

func TestRestoreRejectsHostileArchives(t *testing.T) {
	cases := map[string][]entry{
		"dotdot":         {{name: "../escape", typ: tar.TypeReg, body: "x"}},
		"nested dotdot":  {{name: "a/../../escape", typ: tar.TypeReg, body: "x"}},
		"absolute":       {{name: "/etc/passwd", typ: tar.TypeReg, body: "x"}},
		"symlink parent": {{name: "a", typ: tar.TypeSymlink, link: "/tmp"}, {name: "a/owned", typ: tar.TypeReg, body: "x"}},
		"file parent":    {{name: "a", typ: tar.TypeReg, body: "x"}, {name: "a/b", typ: tar.TypeReg, body: "x"}},
		"backslash":      {{name: `..\escape`, typ: tar.TypeReg, body: "x"}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			dst := filepath.Join(parent, "dst")
			if err := os.Mkdir(dst, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := restoreBytes(t, archiveOf(t, entries...), dst); !errors.Is(err, ErrUnsafePath) {
				t.Fatalf("err = %v, want ErrUnsafePath", err)
			}
			if got := tree(t, parent); len(got) != 1 {
				t.Fatalf("filesystem modified: %v", got)
			}
		})
	}
}

func TestRestoreStripsSpecialBitsAndSkipsHardlinks(t *testing.T) {
	dst := t.TempDir()
	data := archiveOf(t,
		entry{name: "suid", typ: tar.TypeReg, body: "#!/bin/sh", mode: 0o4755},
		entry{name: "hard", typ: tar.TypeLink, link: "suid"},
	)
	if err := restoreBytes(t, data, dst); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dst, "suid"))
	if err != nil || fi.Mode()&os.ModeSetuid != 0 || fi.Mode().Perm() != 0o755 {
		t.Fatalf("mode = %v, %v", fi.Mode(), err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "hard")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("hard link should be skipped")
	}
}

func TestRestoreErrors(t *testing.T) {
	store := storage.NewMemory()
	if _, err := Restore(context.Background(), store, RestoreOptions{ArchiveKey: "missing", Dest: t.TempDir()}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing archive: %v", err)
	}
	if err := restoreBytes(t, []byte("not gzip"), t.TempDir()); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestSafePath(t *testing.T) {
	ok := map[string]string{"a": "a", "a/b/": filepath.Join("a", "b"), "./a//b": filepath.Join("a", "b"), "a/../b": "b", "..a": "..a"}
	for in, want := range ok {
		if got, err := SafePath(in); err != nil || got != want {
			t.Errorf("SafePath(%q) = %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", ".", "..", "../x", "/x", "a/../..", "a\x00b", `a\b`} {
		if _, err := SafePath(bad); err == nil {
			t.Errorf("SafePath(%q) accepted", bad)
		}
	}
}

func TestArgsRoundTrip(t *testing.T) {
	for _, a := range []Args{
		{Mode: ModeBackup, Path: "/data", PVCName: "pg", ArchiveKey: "k.tar.gz", ManifestKey: "k.json"},
		{Mode: ModeRestore, Path: "/data", ArchiveKey: "k.tar.gz", SHA256: "abc"},
	} {
		got, err := ParseArgs(a.Argv())
		if err != nil || got != a {
			t.Fatalf("round trip %+v -> %+v, %v", a, got, err)
		}
	}
	for _, bad := range [][]string{
		{"--mode=sideways", "--archive-key=k"},
		{"--mode=restore"},
		{"--mode=backup", "--archive-key=k"},
		{"--bogus"},
	} {
		if _, err := ParseArgs(bad); err == nil {
			t.Errorf("ParseArgs(%v) accepted", bad)
		}
	}
}

func TestExecuteAndStoreFromEnv(t *testing.T) {
	ctx := context.Background()
	src, dst := t.TempDir(), t.TempDir()
	writeFixture(t, src)
	store := storage.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := Execute(ctx, store, Args{Mode: ModeBackup, Path: src, ArchiveKey: "a", ManifestKey: "m"}, log); err != nil {
		t.Fatal(err)
	}
	var m VolumeManifest
	_ = storage.GetJSON(ctx, store, "m", &m)
	if err := Execute(ctx, store, Args{Mode: ModeRestore, Path: dst, ArchiveKey: "a", SHA256: m.SHA256}, log); err != nil {
		t.Fatal(err)
	}
	if err := Execute(ctx, store, Args{Mode: "x"}, log); err == nil {
		t.Fatal("unknown mode accepted")
	}
	if err := Execute(ctx, store, Args{Mode: ModeBackup, Path: filepath.Join(src, "nope"), ArchiveKey: "a", ManifestKey: "m"}, log); err == nil {
		t.Fatal("bad backup path accepted")
	}
	if err := Execute(ctx, store, Args{Mode: ModeRestore, Path: dst, ArchiveKey: "nope"}, log); err == nil {
		t.Fatal("missing archive accepted")
	}
	env := map[string]string{EnvEndpoint: "http://minio:9000", EnvBucket: "b", EnvAccessKey: "a", EnvSecretKey: "s"}
	if _, err := StoreFromEnv(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	if _, err := StoreFromEnv(func(string) string { return "" }); err == nil {
		t.Fatal("empty env accepted")
	}
}

// FuzzSafePath: any accepted path, joined to a root, stays inside it.
func FuzzSafePath(f *testing.F) {
	for _, s := range []string{"a", "../a", "a/../../b", "/abs", "a/./b/", "..", "....//x"} {
		f.Add(s)
	}
	root := filepath.FromSlash("/restore/root")
	f.Fuzz(func(t *testing.T, name string) {
		rel, err := SafePath(name)
		if err != nil {
			return
		}
		joined := filepath.Join(root, rel)
		if !strings.HasPrefix(joined, root+string(filepath.Separator)) {
			t.Fatalf("%q escapes to %q", name, joined)
		}
	})
}

// FuzzRestore feeds arbitrary entry names and types to the extractor and
// checks nothing is ever created outside the destination.
func FuzzRestore(f *testing.F) {
	f.Add("a/b", "c", byte(tar.TypeReg), "")
	f.Add("../x", "y", byte(tar.TypeReg), "")
	f.Add("l", "l/x", byte(tar.TypeSymlink), "../..")
	f.Add("d/", "d/../../e", byte(tar.TypeDir), "")
	f.Fuzz(func(t *testing.T, n1, n2 string, typ byte, link string) {
		if typ != tar.TypeReg && typ != tar.TypeDir && typ != tar.TypeSymlink {
			typ = tar.TypeReg
		}
		parent := t.TempDir()
		dst := filepath.Join(parent, "dst")
		if err := os.Mkdir(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		data, err := tryArchive(
			entry{name: n1, typ: typ, link: link, body: bodyFor(typ)},
			entry{name: n2, typ: tar.TypeReg, body: "payload"},
		)
		if err != nil {
			t.Skip("header not encodable by archive/tar")
		}
		_ = restoreBytes(t, data, dst)
		var names []string
		entries, _ := os.ReadDir(parent)
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)
		if !reflect.DeepEqual(names, []string{"dst"}) {
			t.Fatalf("entries outside dest: %v", names)
		}
	})
}

func bodyFor(typ byte) string {
	if typ == tar.TypeReg {
		return "data"
	}
	return ""
}
