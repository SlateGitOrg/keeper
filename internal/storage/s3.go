package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config configures an S3Store.
type S3Config struct {
	// Endpoint is an http(s) URL; the scheme selects TLS.
	Endpoint string
	Bucket   string
	Region   string
	// AccessKeyID and SecretAccessKey are static V4 credentials.
	AccessKeyID     string
	SecretAccessKey string
	// Transport overrides the HTTP transport (tests, custom CAs).
	Transport http.RoundTripper
	// PartSize is the multipart chunk size for uploads of unknown length.
	// Zero selects 16 MiB.
	PartSize uint64
}

// S3Store is an ObjectStore backed by an S3-compatible endpoint. It uses
// path-style addressing, which every MinIO deployment supports.
type S3Store struct {
	client   *minio.Client
	bucket   string
	partSize uint64
}

// NewS3 validates cfg and builds a client. It performs no network I/O.
func NewS3(cfg S3Config) (*S3Store, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: endpoint %q: %w", cfg.Endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("s3: endpoint %q must use http or https", cfg.Endpoint)
	}
	if u.Host == "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("s3: endpoint %q must be scheme://host[:port] with no path", cfg.Endpoint)
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("s3: access key id and secret access key are required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:       u.Scheme == "https",
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
		Transport:    cfg.Transport,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: client: %w", err)
	}
	ps := cfg.PartSize
	if ps == 0 {
		ps = 16 << 20
	}
	return &S3Store{client: client, bucket: cfg.Bucket, partSize: ps}, nil
}

func translate(key string, err error) error {
	if err == nil {
		return nil
	}
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NotFound":
		return fmt.Errorf("s3: %s: %w", key, ErrNotFound)
	}
	return fmt.Errorf("s3: %s: %w", key, err)
}

// Put implements ObjectStore. Unknown sizes are uploaded in PartSize chunks.
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) (ObjectInfo, error) {
	info, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType(key),
		PartSize:    s.partSize,
	})
	if err != nil {
		return ObjectInfo{}, translate(key, err)
	}
	return ObjectInfo{Key: key, Size: info.Size, ETag: info.ETag, LastModified: info.LastModified}, nil
}

func contentType(key string) string {
	switch {
	case strings.HasSuffix(key, ".json"):
		return "application/json"
	case strings.HasSuffix(key, ".tar.gz"):
		return "application/gzip"
	}
	return "application/octet-stream"
}

// Get implements ObjectStore. It stats the object first so that a missing key
// is reported here rather than on the first Read.
func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, translate(key, err)
	}
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, translate(key, err)
	}
	return obj, nil
}

// Stat implements ObjectStore.
func (s *S3Store) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	oi, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, translate(key, err)
	}
	return ObjectInfo{Key: oi.Key, Size: oi.Size, ETag: oi.ETag, LastModified: oi.LastModified}, nil
}

// List implements ObjectStore.
func (s *S3Store) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for oi := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if oi.Err != nil {
			return nil, translate(prefix, oi.Err)
		}
		out = append(out, ObjectInfo{Key: oi.Key, Size: oi.Size, ETag: oi.ETag, LastModified: oi.LastModified})
	}
	return out, nil
}

// Delete implements ObjectStore.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil && errors.Is(translate(key, err), ErrNotFound) {
		return nil
	}
	return translate(key, err)
}
