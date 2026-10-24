package controller_test

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	keeperv1 "github.com/archit2501/keeper/api/v1alpha1"
	"github.com/archit2501/keeper/internal/controller"
	"github.com/archit2501/keeper/internal/retention"
)

// resetClock puts the fake clock at real "now" so new policies (whose
// creationTimestamp is real time) start scheduling from the fake clock.
func resetClock(t *testing.T) time.Time {
	t.Helper()
	now := time.Now().UTC()
	clock.SetTime(now)
	return now
}

func policyBackups(t *testing.T, ns, policy string) []keeperv1.Backup {
	t.Helper()
	var list keeperv1.BackupList
	must(t, k8s.List(context.Background(), &list, client.InNamespace(ns), client.MatchingLabels{keeperv1.LabelPolicy: policy}))
	var out []keeperv1.Backup
	for _, b := range list.Items {
		if b.DeletionTimestamp.IsZero() {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func getPolicy(t *testing.T, ns, name string) *keeperv1.BackupPolicy {
	t.Helper()
	var p keeperv1.BackupPolicy
	must(t, k8s.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &p))
	return &p
}

func backupName(policy string, slot time.Time) string {
	return policy + "-" + slot.UTC().Format(controller.BackupNameTimeFormat)
}

// TestPolicySchedulesAndPrunesGFS runs 30 hourly slots through a policy and
// checks that every slot produced a Backup and that the survivors are exactly
// what the retention engine prescribes for those snapshot times.
func TestPolicySchedulesAndPrunesGFS(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	dumpOnFailure(t, ns)
	ctx := context.Background()
	workload(t, ns, "data")
	start := resetClock(t)

	p := validPolicy(ns, "hourly")
	p.Spec.Schedule = "0 * * * *"
	p.Spec.Retention = keeperv1.Retention{KeepLast: 2, KeepDaily: 2}
	p.Spec.Template.Labels = map[string]string{"team": "db"}
	p.Spec.Template.Spec.Hooks = pgHooks()
	must(t, k8s.Create(ctx, p))

	eventually(t, "first NextScheduleTime", func() bool { return getPolicy(t, ns, "hourly").Status.NextScheduleTime != nil })
	slot := start.Truncate(time.Hour).Add(time.Hour)
	var slots []time.Time
	for range 30 {
		clock.SetTime(slot.Add(5 * time.Second))
		name := backupName("hourly", slot)
		waitBackup(t, ns, name, keeperv1.BackupCompleted)
		slots = append(slots, slot)
		slot = slot.Add(time.Hour)
	}

	// Expected survivors: the retention engine over the same snapshot times.
	items := make([]retention.Item, len(slots))
	for i, s := range slots {
		items[i] = retention.Item{ID: backupName("hourly", s), Time: s.Add(5 * time.Second)}
	}
	decisions, err := retention.Apply(retention.Policy{Last: 2, Daily: 2}, items, time.UTC)
	must(t, err)
	var want []string
	wantReasons := map[string]string{}
	for _, d := range decisions {
		if d.Keep {
			want = append(want, d.ID)
			wantReasons[d.ID] = d.ReasonString()
		}
	}
	sort.Strings(want)
	eventually(t, "retention to converge", func() bool {
		var got []string
		for _, b := range policyBackups(t, ns, "hourly") {
			got = append(got, b.Name)
		}
		return slices.Equal(got, want)
	})

	for _, b := range policyBackups(t, ns, "hourly") {
		if b.Labels["team"] != "db" {
			t.Errorf("%s: template labels not copied: %v", b.Name, b.Labels)
		}
		if b.Annotations[keeperv1.AnnotationScheduledTime] == "" {
			t.Errorf("%s: missing scheduled-time annotation", b.Name)
		}
		eventually(t, "retained-by annotation on "+b.Name, func() bool {
			return getBackup(t, ns, b.Name).Annotations[keeperv1.AnnotationRetainedBy] == wantReasons[b.Name]
		})
	}
	st := getPolicy(t, ns, "hourly").Status
	if st.Retention.Kept != int32(len(want)) || st.Retention.PrunedTotal != int64(30-len(want)) || st.Retention.LastPruneTime == nil {
		t.Errorf("retention status %+v, want kept=%d pruned=%d", st.Retention, len(want), 30-len(want))
	}
	if !st.LastScheduleTime.Equal(ptrTime(slots[len(slots)-1])) {
		t.Errorf("lastScheduleTime %v, want %v", st.LastScheduleTime, slots[len(slots)-1])
	}
	if !st.NextScheduleTime.Equal(ptrTime(slot)) {
		t.Errorf("nextScheduleTime %v, want %v", st.NextScheduleTime, slot)
	}
	if !meta.IsStatusConditionTrue(st.Conditions, keeperv1.ConditionReady) || !meta.IsStatusConditionTrue(st.Conditions, keeperv1.ConditionScheduleValid) {
		t.Errorf("conditions %+v", st.Conditions)
	}
	eventually(t, "Scheduled and Pruned events", func() bool {
		r := eventReasons(t, ns, "hourly")
		return slices.Contains(r, "Scheduled") && slices.Contains(r, "Pruned")
	})

	// Pinned backups are never pruned.
	oldest := policyBackups(t, ns, "hourly")[0]
	must(t, update(t, &oldest, func(b *keeperv1.Backup) { b.Labels[keeperv1.LabelPin] = "true" }))
	for range 3 {
		clock.SetTime(slot.Add(5 * time.Second))
		waitBackup(t, ns, backupName("hourly", slot), keeperv1.BackupCompleted)
		slot = slot.Add(time.Hour)
	}
	eventually(t, "pinned annotation", func() bool {
		return strings.Contains(getBackup(t, ns, oldest.Name).Annotations[keeperv1.AnnotationRetainedBy], "pinned")
	})
	must(t, k8s.Delete(ctx, getPolicy(t, ns, "hourly")))
	if n := len(policyBackups(t, ns, "hourly")); n == 0 {
		t.Fatal("deleting a policy must not delete its backups")
	}
}

func ptrTime(t time.Time) *metav1.Time { return &metav1.Time{Time: t} }

// TestPolicyConcurrencyAndSuspend: Forbid delays a slot while a Backup runs;
// suspend stops scheduling.
func TestPolicyConcurrencyAndSuspend(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	dumpOnFailure(t, ns)
	ctx := context.Background()
	workload(t, ns, "data")
	start := resetClock(t)

	p := validPolicy(ns, "forbid")
	p.Spec.Schedule = "*/10 * * * *"
	must(t, k8s.Create(ctx, p))
	eventually(t, "policy to be scheduled", func() bool { return getPolicy(t, ns, "forbid").Status.NextScheduleTime != nil })

	sim.Pause(true)
	t.Cleanup(func() { sim.Pause(false) })
	first := start.Truncate(10 * time.Minute).Add(10 * time.Minute)
	clock.SetTime(first.Add(time.Second))
	eventually(t, "first backup", func() bool { return len(policyBackups(t, ns, "forbid")) == 1 })
	second := first.Add(10 * time.Minute)
	clock.SetTime(second.Add(time.Second))
	eventually(t, "policy to report the waiting slot", func() bool {
		c := meta.FindStatusCondition(getPolicy(t, ns, "forbid").Status.Conditions, keeperv1.ConditionReady)
		return c != nil && c.Reason == "WaitingForActiveBackup"
	})
	if n := len(policyBackups(t, ns, "forbid")); n != 1 {
		t.Fatalf("Forbid started %d backups while one was running", n)
	}
	if a := getPolicy(t, ns, "forbid").Status.Active; len(a) != 1 || a[0].Name != backupName("forbid", first) {
		t.Fatalf("active %v", a)
	}
	sim.Pause(false)
	waitBackup(t, ns, backupName("forbid", first), keeperv1.BackupCompleted)
	waitBackup(t, ns, backupName("forbid", second), keeperv1.BackupCompleted)

	must(t, update(t, getPolicy(t, ns, "forbid"), func(p *keeperv1.BackupPolicy) { p.Spec.Suspend = true }))
	eventually(t, "Suspended condition", func() bool {
		c := meta.FindStatusCondition(getPolicy(t, ns, "forbid").Status.Conditions, keeperv1.ConditionReady)
		return c != nil && c.Reason == "Suspended"
	})
	clock.SetTime(second.Add(30*time.Minute + time.Second))
	consistently(t, "no backups while suspended", 400*time.Millisecond, func() bool { return len(policyBackups(t, ns, "forbid")) == 2 })
}

// TestPolicyMissedSlots: a slot later than startingDeadlineSeconds is skipped;
// without a deadline only the most recent of several missed slots runs.
func TestPolicyMissedSlots(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	dumpOnFailure(t, ns)
	ctx := context.Background()
	workload(t, ns, "data")
	start := resetClock(t)

	late := validPolicy(ns, "deadline")
	late.Spec.Schedule = "0 * * * *"
	late.Spec.StartingDeadlineSeconds = ptr.To[int64](60)
	must(t, k8s.Create(ctx, late))
	lazy := validPolicy(ns, "catchup")
	lazy.Spec.Schedule = "0 * * * *"
	must(t, k8s.Create(ctx, lazy))
	eventually(t, "policies to be scheduled", func() bool {
		return getPolicy(t, ns, "deadline").Status.NextScheduleTime != nil && getPolicy(t, ns, "catchup").Status.NextScheduleTime != nil
	})

	slot := start.Truncate(time.Hour).Add(3 * time.Hour)
	clock.SetTime(slot.Add(5 * time.Minute)) // three slots passed, the last one 5 minutes ago
	eventually(t, "MissedSchedule event", func() bool { return hasEvent(t, ns, "deadline", "MissedSchedule") })
	eventually(t, "deadline policy to record the skipped slot", func() bool {
		st := getPolicy(t, ns, "deadline").Status
		return st.LastScheduleTime != nil && st.LastScheduleTime.Equal(ptrTime(slot))
	})
	if n := len(policyBackups(t, ns, "deadline")); n != 0 {
		t.Fatalf("a slot past its deadline created %d backups", n)
	}
	waitBackup(t, ns, backupName("catchup", slot), keeperv1.BackupCompleted)
	if n := len(policyBackups(t, ns, "catchup")); n != 1 {
		t.Fatalf("catch-up created %d backups, want only the most recent slot", n)
	}
	eventually(t, "missed-slot warning", func() bool { return hasEvent(t, ns, "catchup", "MissedSchedule") })
}

// TestPolicyInvalidScheduleAndFailedHistory: schedules that pass CEL but not
// the cron parser surface as a condition; failed Backups are capped.
func TestPolicyInvalidScheduleAndFailedHistory(t *testing.T) {
	requireEnv(t)
	ns := newNamespace(t)
	dumpOnFailure(t, ns)
	ctx := context.Background()
	start := resetClock(t)

	for name, mut := range map[string]func(*keeperv1.BackupPolicy){
		"bad-minute": func(p *keeperv1.BackupPolicy) { p.Spec.Schedule = "61 * * * *" },
		"bad-zone":   func(p *keeperv1.BackupPolicy) { p.Spec.TimeZone = "Mars/Olympus_Mons" },
		"too-fast":   func(p *keeperv1.BackupPolicy) { p.Spec.Schedule = "@every 10s" },
	} {
		p := validPolicy(ns, name)
		mut(p)
		must(t, k8s.Create(ctx, p))
		eventually(t, name+" ScheduleValid=False", func() bool {
			c := meta.FindStatusCondition(getPolicy(t, ns, name).Status.Conditions, keeperv1.ConditionScheduleValid)
			return c != nil && c.Status == "False" && c.Reason == "InvalidSchedule"
		})
	}

	workload(t, ns, "data")
	sim.FailJob = func(j *batchv1.Job) error {
		if j.Namespace == ns {
			return errors.New("disk full")
		}
		return nil
	}
	t.Cleanup(func() { sim.FailJob = nil })
	p := validPolicy(ns, "flaky")
	p.Spec.Schedule = "*/5 * * * *"
	p.Spec.FailedBackupsHistoryLimit = ptr.To[int32](1)
	must(t, k8s.Create(ctx, p))
	eventually(t, "policy to be scheduled", func() bool { return getPolicy(t, ns, "flaky").Status.NextScheduleTime != nil })
	slot := start.Truncate(5 * time.Minute).Add(5 * time.Minute)
	for range 3 {
		clock.SetTime(slot.Add(time.Second))
		waitBackup(t, ns, backupName("flaky", slot), keeperv1.BackupFailed)
		slot = slot.Add(5 * time.Minute)
	}
	eventually(t, "failed history to be capped at 1", func() bool {
		bs := policyBackups(t, ns, "flaky")
		return len(bs) == 1 && bs[0].Name == backupName("flaky", slot.Add(-5*time.Minute))
	})
}
