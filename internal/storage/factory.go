package storage

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
)

// Secret keys read from StorageSpec.CredentialsSecretRef.
const (
	SecretAccessKeyID     = "accessKeyID"
	SecretSecretAccessKey = "secretAccessKey"
)

// Factory opens the ObjectStore a StorageSpec points at.
type Factory interface {
	Open(ctx context.Context, namespace string, spec keeperv1.StorageSpec) (ObjectStore, error)
}

// SecretFactory builds S3 stores using credentials from a Secret in the
// object's namespace.
type SecretFactory struct {
	Client client.Reader
}

// Open implements Factory.
func (f SecretFactory) Open(ctx context.Context, namespace string, spec keeperv1.StorageSpec) (ObjectStore, error) {
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: spec.CredentialsSecretRef.Name}
	if err := f.Client.Get(ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("storage credentials secret %s: %w", key, err)
	}
	id, secret := string(sec.Data[SecretAccessKeyID]), string(sec.Data[SecretSecretAccessKey])
	if id == "" || secret == "" {
		return nil, fmt.Errorf("storage credentials secret %s must contain %s and %s", key, SecretAccessKeyID, SecretSecretAccessKey)
	}
	return NewS3(S3Config{
		Endpoint:        spec.Endpoint,
		Bucket:          spec.Bucket,
		Region:          spec.Region,
		AccessKeyID:     id,
		SecretAccessKey: secret,
	})
}

// MemoryFactory hands out one Memory store per endpoint+bucket. It is used by
// tests and the offline demo.
type MemoryFactory struct {
	mu     sync.Mutex
	stores map[string]*Memory
}

// NewMemoryFactory returns an empty MemoryFactory.
func NewMemoryFactory() *MemoryFactory { return &MemoryFactory{stores: map[string]*Memory{}} }

// Open implements Factory.
func (f *MemoryFactory) Open(_ context.Context, _ string, spec keeperv1.StorageSpec) (ObjectStore, error) {
	return f.Store(spec.Endpoint, spec.Bucket), nil
}

// Store returns (creating if needed) the store for endpoint and bucket.
func (f *MemoryFactory) Store(endpoint, bucket string) *Memory {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := endpoint + "|" + bucket
	m, ok := f.stores[k]
	if !ok {
		m = NewMemory()
		f.stores[k] = m
	}
	return m
}
