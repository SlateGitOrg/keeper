//go:build integration

package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/archit2501/keeper/internal/mover"
	"github.com/archit2501/keeper/internal/storage"
	"github.com/archit2501/keeper/internal/storage/storagetest"
)

// TestRealMinIO runs the conformance suite and a mover round trip against a
// real MinIO server (started by hack/minio-integration.sh). This is the test
// that exercises genuine SigV4 signing and streaming multipart uploads.
func TestRealMinIO(t *testing.T) {
	endpoint, host := os.Getenv("KEEPER_IT_S3_ENDPOINT"), os.Getenv("KEEPER_IT_S3_HOST")
	if endpoint == "" {
		t.Skip("KEEPER_IT_S3_ENDPOINT not set; run `make integration`")
	}
	const bucket, id, secret = "keeper-it", "keeperadmin", "keeper-secret-key"
	ctx := context.Background()
	admin, err := minio.New(host, &minio.Options{Creds: credentials.NewStaticV4(id, secret, "")})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := admin.BucketExists(ctx, bucket); !ok {
		if err := admin.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	store, err := storage.NewS3(storage.S3Config{Endpoint: endpoint, Bucket: bucket, AccessKeyID: id, SecretAccessKey: secret, PartSize: 5 << 20})
	if err != nil {
		t.Fatal(err)
	}
	storagetest.Run(t, store, "conformance/")

	t.Run("mover round trip", func(t *testing.T) {
		src, dst := t.TempDir(), t.TempDir()
		for i, size := range []int{0, 1, 4 << 20, 9 << 20} {
			b := make([]byte, size)
			for j := range b {
				b[j] = byte((j * 31) ^ i)
			}
			if err := os.WriteFile(filepath.Join(src, "f"+string(rune('a'+i))), b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		m, err := mover.Backup(ctx, store, mover.BackupOptions{Source: src, ArchiveKey: "mover/a.tar.gz", ManifestKey: "mover/a.json"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mover.Restore(ctx, store, mover.RestoreOptions{ArchiveKey: m.ArchiveKey, ExpectedSHA256: m.SHA256, Dest: dst}); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"fa", "fb", "fc", "fd"} {
			a, _ := os.ReadFile(filepath.Join(src, name))
			b, _ := os.ReadFile(filepath.Join(dst, name))
			if string(a) != string(b) {
				t.Fatalf("%s differs after round trip", name)
			}
		}
		if _, err := storage.DeletePrefix(ctx, store, "mover/"); err != nil {
			t.Fatal(err)
		}
	})
}
