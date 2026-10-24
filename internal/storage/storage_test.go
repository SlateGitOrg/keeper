package storage_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/storage"
	"github.com/archit2501/keeper/internal/storage/s3test"
	"github.com/archit2501/keeper/internal/storage/storagetest"
)

func TestMemoryConformance(t *testing.T) {
	storagetest.Run(t, storage.NewMemory(), "suite/")
}

func TestS3ConformanceAgainstFakeServer(t *testing.T) {
	srv := s3test.New("keeper-test", "AKIDTEST")
	defer srv.Close()
	st, err := storage.NewS3(storage.S3Config{
		Endpoint: srv.URL, Bucket: "keeper-test", AccessKeyID: "AKIDTEST", SecretAccessKey: "secret",
		PartSize: 5 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	storagetest.Run(t, st, "suite/")
	if srv.Requests("UploadPart") < 2 || srv.Requests("CompleteMultipartUpload") != 1 {
		t.Fatalf("expected a multipart upload, got parts=%d completes=%d", srv.Requests("UploadPart"), srv.Requests("CompleteMultipartUpload"))
	}
}

func TestS3WrongCredentials(t *testing.T) {
	srv := s3test.New("keeper-test", "AKIDTEST")
	defer srv.Close()
	st, err := storage.NewS3(storage.S3Config{Endpoint: srv.URL, Bucket: "keeper-test", AccessKeyID: "WRONG", SecretAccessKey: "x"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Put(context.Background(), "k", strings.NewReader("v"), 1)
	if err == nil || errors.Is(err, storage.ErrNotFound) || !strings.Contains(err.Error(), "access key") {
		t.Fatalf("Put with wrong key: %v", err)
	}
	if _, err := st.List(context.Background(), ""); err == nil {
		t.Fatal("List with wrong key should fail")
	}
}

func TestNewS3Validation(t *testing.T) {
	ok := storage.S3Config{Endpoint: "https://minio.example:9000", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"}
	if _, err := storage.NewS3(ok); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mut := range map[string]func(*storage.S3Config){
		"bad scheme":     func(c *storage.S3Config) { c.Endpoint = "ftp://x" },
		"path":           func(c *storage.S3Config) { c.Endpoint = "https://x/bucket" },
		"unparseable":    func(c *storage.S3Config) { c.Endpoint = "http://[::1" },
		"no bucket":      func(c *storage.S3Config) { c.Bucket = "" },
		"no credentials": func(c *storage.S3Config) { c.SecretAccessKey = "" },
	} {
		c := ok
		mut(&c)
		if _, err := storage.NewS3(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDeletePrefixGuards(t *testing.T) {
	m := storage.NewMemory()
	for _, p := range []string{"", "backups/ns/b"} {
		if _, err := storage.DeletePrefix(context.Background(), m, p); err == nil {
			t.Errorf("DeletePrefix(%q) accepted", p)
		}
	}
	m.Inject(func(op, _ string) error {
		if op == "delete" {
			return errors.New("boom")
		}
		return nil
	})
	_, _ = m.Put(context.Background(), "p/x", bytes.NewReader([]byte("1")), 1)
	if n, err := storage.DeletePrefix(context.Background(), m, "p/"); err == nil || n != 0 {
		t.Fatalf("DeletePrefix with failing delete = %d, %v", n, err)
	}
	m.Inject(func(op, _ string) error {
		if op == "list" {
			return errors.New("boom")
		}
		return nil
	})
	if _, err := storage.DeletePrefix(context.Background(), m, "p/"); err == nil {
		t.Fatal("expected list failure")
	}
}

func TestMemoryRejectsSizeMismatchAndEmptyKey(t *testing.T) {
	m := storage.NewMemory()
	if _, err := m.Put(context.Background(), "k", strings.NewReader("abc"), 5); err == nil {
		t.Fatal("size mismatch accepted")
	}
	if _, err := m.Put(context.Background(), "", strings.NewReader("abc"), 3); err == nil {
		t.Fatal("empty key accepted")
	}
	_, _ = m.Put(context.Background(), "k", strings.NewReader("abc"), 3)
	if m.Len() != 1 || m.TotalBytes() != 3 {
		t.Fatalf("Len=%d TotalBytes=%d", m.Len(), m.TotalBytes())
	}
}

func TestLayout(t *testing.T) {
	l := storage.Layout{Prefix: "/team-a/"}
	if got := l.BackupManifest("db", "pg-1"); got != "team-a/backups/db/pg-1/backup.json" {
		t.Fatal(got)
	}
	if got := l.Archive("db", "pg-1", "data"); got != "team-a/backups/db/pg-1/volumes/data.tar.gz" {
		t.Fatal(got)
	}
	if got := (storage.Layout{}).VolumeManifest("db", "pg-1", "data"); got != "backups/db/pg-1/volumes/data.json" {
		t.Fatal(got)
	}
}

func TestSecretFactory(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	good := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "ns"}, Data: map[string][]byte{
		storage.SecretAccessKeyID: []byte("id"), storage.SecretSecretAccessKey: []byte("secret"),
	}}
	partial := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: "ns"}, Data: map[string][]byte{storage.SecretAccessKeyID: []byte("id")}}
	f := storage.SecretFactory{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(good, partial).Build()}
	spec := keeperv1.StorageSpec{Endpoint: "http://minio:9000", Bucket: "bkt", CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3"}}
	if _, err := f.Open(context.Background(), "ns", spec); err != nil {
		t.Fatal(err)
	}
	spec.CredentialsSecretRef.Name = "partial"
	if _, err := f.Open(context.Background(), "ns", spec); err == nil {
		t.Fatal("partial secret accepted")
	}
	spec.CredentialsSecretRef.Name = "missing"
	if _, err := f.Open(context.Background(), "ns", spec); err == nil {
		t.Fatal("missing secret accepted")
	}
}

func TestMemoryFactorySharesStores(t *testing.T) {
	f := storage.NewMemoryFactory()
	spec := keeperv1.StorageSpec{Endpoint: "http://e", Bucket: "b"}
	a, _ := f.Open(context.Background(), "x", spec)
	b, _ := f.Open(context.Background(), "y", spec)
	if a != b || f.Store("http://e", "b") != a {
		t.Fatal("same endpoint+bucket should share a store")
	}
	if c, _ := f.Open(context.Background(), "x", keeperv1.StorageSpec{Endpoint: "http://e", Bucket: "c"}); c == a {
		t.Fatal("different buckets must not share")
	}
}

func TestDecodeAWSChunked(t *testing.T) {
	in := "5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=0\r\n\r\n"
	got, err := s3test.DecodeAWSChunked(strings.NewReader(in))
	if err != nil || string(got) != "hello world" {
		t.Fatalf("got %q, %v", got, err)
	}
	for _, bad := range []string{"zz\r\nx", "5\r\nhel", "5\r\nhelloXX", ""} {
		if _, err := s3test.DecodeAWSChunked(strings.NewReader(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
