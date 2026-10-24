// Package storagetest holds a conformance suite every storage.ObjectStore
// implementation must pass.
package storagetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/archit2501/keeper/internal/storage"
)

// Run exercises store. prefix isolates the suite's keys so it can run against
// a shared bucket; it must end in "/".
func Run(t *testing.T, store storage.ObjectStore, prefix string) {
	t.Helper()
	ctx := context.Background()

	t.Run("missing key is ErrNotFound", func(t *testing.T) {
		if _, err := store.Get(ctx, prefix+"nope"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("Get: %v", err)
		}
		if _, err := store.Stat(ctx, prefix+"nope"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("Stat: %v", err)
		}
		if err := store.Delete(ctx, prefix+"nope"); err != nil {
			t.Fatalf("Delete of missing key: %v", err)
		}
	})

	t.Run("put get stat round trip", func(t *testing.T) {
		payload := []byte("hello keeper")
		info, err := store.Put(ctx, prefix+"a/hello.txt", bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size != int64(len(payload)) {
			t.Fatalf("Put size %d", info.Size)
		}
		got := readAll(t, store, prefix+"a/hello.txt")
		if !bytes.Equal(got, payload) {
			t.Fatalf("Get = %q", got)
		}
		st, err := store.Stat(ctx, prefix+"a/hello.txt")
		if err != nil || st.Size != int64(len(payload)) {
			t.Fatalf("Stat = %+v, %v", st, err)
		}
	})

	t.Run("unknown size upload", func(t *testing.T) {
		payload := make([]byte, 6<<20+123)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(ctx, prefix+"a/big.bin", io.MultiReader(bytes.NewReader(payload)), -1); err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, store, prefix+"a/big.bin"); !bytes.Equal(got, payload) {
			t.Fatalf("round trip of %d bytes returned %d bytes", len(payload), len(got))
		}
	})

	t.Run("json helpers", func(t *testing.T) {
		in := map[string]int{"answer": 42}
		if err := storage.PutJSON(ctx, store, prefix+"b/doc.json", in); err != nil {
			t.Fatal(err)
		}
		var out map[string]int
		if err := storage.GetJSON(ctx, store, prefix+"b/doc.json", &out); err != nil || out["answer"] != 42 {
			t.Fatalf("GetJSON = %v, %v", out, err)
		}
		if err := storage.GetJSON(ctx, store, prefix+"a/hello.txt", &out); err == nil {
			t.Fatal("decoding non-JSON should fail")
		}
	})

	t.Run("list and delete prefix", func(t *testing.T) {
		objs, err := store.List(ctx, prefix+"a/")
		if err != nil {
			t.Fatal(err)
		}
		if len(objs) != 2 || objs[0].Key != prefix+"a/big.bin" || objs[1].Key != prefix+"a/hello.txt" {
			t.Fatalf("List = %+v", objs)
		}
		n, err := storage.DeletePrefix(ctx, store, prefix)
		if err != nil || n != 3 {
			t.Fatalf("DeletePrefix = %d, %v", n, err)
		}
		if objs, _ := store.List(ctx, prefix); len(objs) != 0 {
			t.Fatalf("objects left after DeletePrefix: %+v", objs)
		}
	})
}

func readAll(t *testing.T, store storage.ObjectStore, key string) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
