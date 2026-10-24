package storage

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 by definition; not used for security.
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory ObjectStore. Inject lets tests fail operations.
type Memory struct {
	mu      sync.RWMutex
	objects map[string]memObject
	now     func() time.Time
	inject  func(op, key string) error
}

type memObject struct {
	data []byte
	etag string
	mod  time.Time
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{objects: map[string]memObject{}, now: time.Now}
}

// Inject installs a fault hook consulted before every operation ("put",
// "get", "stat", "list", "delete"). A non-nil error fails the operation.
func (m *Memory) Inject(f func(op, key string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inject = f
}

func (m *Memory) fault(op, key string) error {
	m.mu.RLock()
	f := m.inject
	m.mu.RUnlock()
	if f == nil {
		return nil
	}
	return f(op, key)
}

// Put implements ObjectStore.
func (m *Memory) Put(ctx context.Context, key string, r io.Reader, size int64) (ObjectInfo, error) {
	if err := m.fault("put", key); err != nil {
		return ObjectInfo{}, err
	}
	if key == "" {
		return ObjectInfo{}, fmt.Errorf("memory: empty key")
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("memory: read body for %s: %w", key, err)
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if size >= 0 && int64(len(data)) != size {
		return ObjectInfo{}, fmt.Errorf("memory: %s: declared size %d, got %d bytes", key, size, len(data))
	}
	sum := md5.Sum(data) //nolint:gosec // ETag, see import.
	obj := memObject{data: data, etag: hex.EncodeToString(sum[:]), mod: m.now()}
	m.mu.Lock()
	m.objects[key] = obj
	m.mu.Unlock()
	return ObjectInfo{Key: key, Size: int64(len(data)), ETag: obj.etag, LastModified: obj.mod}, nil
}

// Get implements ObjectStore.
func (m *Memory) Get(_ context.Context, key string) (io.ReadCloser, error) {
	if err := m.fault("get", key); err != nil {
		return nil, err
	}
	m.mu.RLock()
	obj, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("memory: %s: %w", key, ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

// Stat implements ObjectStore.
func (m *Memory) Stat(_ context.Context, key string) (ObjectInfo, error) {
	if err := m.fault("stat", key); err != nil {
		return ObjectInfo{}, err
	}
	m.mu.RLock()
	obj, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return ObjectInfo{}, fmt.Errorf("memory: %s: %w", key, ErrNotFound)
	}
	return ObjectInfo{Key: key, Size: int64(len(obj.data)), ETag: obj.etag, LastModified: obj.mod}, nil
}

// List implements ObjectStore.
func (m *Memory) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if err := m.fault("list", prefix); err != nil {
		return nil, err
	}
	m.mu.RLock()
	out := make([]ObjectInfo, 0, len(m.objects))
	for k, o := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.mod})
		}
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Delete implements ObjectStore.
func (m *Memory) Delete(_ context.Context, key string) error {
	if err := m.fault("delete", key); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

// Len returns the number of stored objects.
func (m *Memory) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
}

// TotalBytes returns the summed size of stored objects.
func (m *Memory) TotalBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for _, o := range m.objects {
		n += int64(len(o.data))
	}
	return n
}
