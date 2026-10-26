package controller_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	keeperv1 "github.com/SlateGitOrg/keeper/api/v1alpha1"
)

func validPolicy(ns, name string) *keeperv1.BackupPolicy {
	return &keeperv1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: keeperv1.BackupPolicySpec{
			Schedule:  "0 */6 * * *",
			Retention: keeperv1.Retention{KeepLast: 3, KeepDaily: 7},
			Template:  keeperv1.BackupTemplate{Spec: backupSpec(keeperv1.DriverFileCopy, "", "data")},
		},
	}
}

// TestCRDValidation checks that the CEL rules and OpenAPI constraints in the
// generated CRDs reject invalid objects with the intended messages.
func TestCRDValidation(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	ctx := context.Background()

	cases := []struct {
		name string
		mut  func(*keeperv1.BackupPolicy)
		want string
	}{
		{"retention keeps nothing", func(p *keeperv1.BackupPolicy) { p.Spec.Retention = keeperv1.Retention{} }, "retention must keep at least one backup"},
		{"both source fields", func(p *keeperv1.BackupPolicy) {
			p.Spec.Template.Spec.Source.PVCSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}}
		}, "exactly one of pvcNames or pvcSelector must be set"},
		{"no source fields", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Source = keeperv1.VolumeSource{} }, "exactly one of pvcNames or pvcSelector must be set"},
		{"csi block on FileCopy", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Driver.CSI = &keeperv1.CSIDriverSpec{} }, "csi may only be set when type is CSISnapshot"},
		{"fileCopy block on CSI", func(p *keeperv1.BackupPolicy) {
			p.Spec.Template.Spec.Driver = keeperv1.DriverSpec{Type: keeperv1.DriverCSISnapshot, FileCopy: &keeperv1.FileCopyDriverSpec{}}
		}, "fileCopy may only be set when type is FileCopy"},
		{"unknown driver", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Driver.Type = "Rsync" }, "Unsupported value"},
		{"ftp endpoint", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Storage.Endpoint = "ftp://minio:21" }, "endpoint must be an http(s) URL"},
		{"absolute prefix", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Storage.Prefix = "/etc" }, "prefix must be relative"},
		{"dotdot prefix", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Storage.Prefix = "a/../../b" }, "prefix must be relative"},
		{"bucket name", func(p *keeperv1.BackupPolicy) { p.Spec.Template.Spec.Storage.Bucket = "Not_A_Bucket" }, "spec.template.spec.storage.bucket"},
		{"TZ in schedule", func(p *keeperv1.BackupPolicy) { p.Spec.Schedule = "CRON_TZ=UTC 0 * * * *" }, "set the time zone with spec.timeZone"},
		{"four-field schedule", func(p *keeperv1.BackupPolicy) { p.Spec.Schedule = "0 * * *" }, "schedule must have exactly 5 fields"},
		{"starting deadline", func(p *keeperv1.BackupPolicy) { p.Spec.StartingDeadlineSeconds = ptr.To[int64](5) }, "startingDeadlineSeconds must be at least 10"},
		{"long name", func(p *keeperv1.BackupPolicy) { p.Name = strings.Repeat("p", 51) }, "limited to 50 characters"},
		{"duplicate hook names", func(p *keeperv1.BackupPolicy) {
			h := pgHooks().Pre[0]
			p.Spec.Template.Spec.Hooks = &keeperv1.Hooks{Pre: []keeperv1.Hook{h, h}}
		}, "Duplicate value"},
		{"hook timeout", func(p *keeperv1.BackupPolicy) {
			h := pgHooks().Pre[0]
			h.TimeoutSeconds = 7200
			p.Spec.Template.Spec.Hooks = &keeperv1.Hooks{Pre: []keeperv1.Hook{h}}
		}, "timeoutSeconds"},
		{"empty hook command", func(p *keeperv1.BackupPolicy) {
			h := pgHooks().Pre[0]
			h.Command = nil
			p.Spec.Template.Spec.Hooks = &keeperv1.Hooks{Pre: []keeperv1.Hook{h}}
		}, "command"},
		{"negative retention", func(p *keeperv1.BackupPolicy) { p.Spec.Retention.KeepDaily = -1 }, "keepDaily"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validPolicy(ns, "p"+string(rune('a'+i)))
			tc.mut(p)
			err := k8s.Create(ctx, p)
			if err == nil {
				t.Fatalf("accepted an invalid policy")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	t.Run("defaults are applied", func(t *testing.T) {
		p := validPolicy(ns, "defaults")
		p.Spec.Template.Spec.Hooks = pgHooks()
		must(t, k8s.Create(ctx, p))
		s := p.Spec
		if s.TimeZone != "UTC" || s.ConcurrencyPolicy != keeperv1.ConcurrencyForbid || ptr.Deref(s.FailedBackupsHistoryLimit, 0) != 3 {
			t.Fatalf("policy defaults %+v", s)
		}
		ts := s.Template.Spec
		if ts.Storage.Region != "us-east-1" || ts.DeletionPolicy != keeperv1.DeletionDelete {
			t.Fatalf("template defaults %+v", ts)
		}
		if h := ts.Hooks.Pre[0]; h.OnError != keeperv1.HookFail || h.TimeoutSeconds != 30 {
			t.Fatalf("hook defaults %+v", h)
		}
		must(t, k8s.Delete(ctx, p))
	})

	t.Run("backup and restore specs are immutable", func(t *testing.T) {
		createBackup(t, ns, "immutable", backupSpec(keeperv1.DriverFileCopy, "", "missing"))
		err := update(t, &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "immutable"}}, func(b *keeperv1.Backup) {
			b.Spec.Source.PVCNames = []string{"other"}
		})
		if err == nil || !strings.Contains(err.Error(), "spec is immutable") {
			t.Fatalf("backup update: %v", err)
		}
		r := createRestore(t, ns, "immutable", "immutable")
		if err := update(t, r.DeepCopy(), func(r *keeperv1.Restore) { r.Spec.BackupName = "other" }); err == nil || !strings.Contains(err.Error(), "spec is immutable") {
			t.Fatalf("restore update: %v", err)
		}
		if r.Spec.PVCNamePrefix != "restored-" {
			t.Fatalf("restore prefix default = %q", r.Spec.PVCNamePrefix)
		}
		bad := &keeperv1.Restore{ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: ns}, Spec: keeperv1.RestoreSpec{BackupName: "x", PVCNamePrefix: "UPPER-"}}
		if err := k8s.Create(ctx, bad); err == nil {
			t.Fatal("invalid prefix accepted")
		}
	})

	t.Run("secret reference is required", func(t *testing.T) {
		b := &keeperv1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "nosecret", Namespace: ns}, Spec: backupSpec(keeperv1.DriverFileCopy, "", "data")}
		b.Spec.Storage.CredentialsSecretRef = corev1.LocalObjectReference{}
		if err := k8s.Create(ctx, b); err == nil || !strings.Contains(err.Error(), "credentialsSecretRef.name is required") {
			t.Fatalf("create without secret: %v", err)
		}
	})
}
