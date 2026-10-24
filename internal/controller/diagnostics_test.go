package controller_test

import (
	"context"
	"encoding/json"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
)

func dumpOnFailure(t *testing.T, ns string) {
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		var ps keeperv1.BackupPolicyList
		_ = k8s.List(context.Background(), &ps, client.InNamespace(ns))
		for _, p := range ps.Items {
			b, _ := json.MarshalIndent(p.Status, "", " ")
			t.Logf("policy %s spec.suspend=%v status=%s", p.Name, p.Spec.Suspend, b)
		}
		var bs keeperv1.BackupList
		_ = k8s.List(context.Background(), &bs, client.InNamespace(ns))
		for _, b := range bs.Items {
			t.Logf("backup %s phase=%s msg=%s", b.Name, b.Status.Phase, b.Status.Message)
		}
		t.Logf("clock=%v", clock.Now())
	})
}
