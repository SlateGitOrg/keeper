package controller_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestSamplesAreValid server-side dry-runs every sample manifest, so the
// samples cannot drift from the CRD schema and CEL rules.
func TestSamplesAreValid(t *testing.T) {
	requireEnv(t)
	ctx := context.Background()
	must(t, k8s.Create(ctx, namespaceObj("shop")))
	files, err := filepath.Glob(filepath.Join("..", "..", "config", "samples", "*.yaml"))
	must(t, err)
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	seen := 0
	for _, f := range files {
		if filepath.Base(f) == "kustomization.yaml" {
			continue
		}
		raw, err := os.ReadFile(f)
		must(t, err)
		r := utilyaml.NewYAMLReader(bufioReader(raw))
		for {
			doc, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			must(t, err)
			if len(bytes.TrimSpace(doc)) == 0 {
				continue
			}
			obj, _, err := decoder.Decode(doc, nil, nil)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if err := k8s.Create(ctx, obj.(client.Object), client.DryRunAll); err != nil {
				t.Errorf("%s: rejected by the API server: %v", filepath.Base(f), err)
			}
			seen++
		}
	}
	if seen < 5 {
		t.Fatalf("validated only %d sample objects", seen)
	}
}
