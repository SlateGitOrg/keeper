package schedule

import (
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func mustParse(t testing.TB, expr, tz string) *Schedule {
	t.Helper()
	s, err := Parse(expr, tz)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseErrors(t *testing.T) {
	for _, tc := range []struct{ expr, tz string }{
		{"CRON_TZ=UTC 0 * * * *", ""},
		{"0 * * *", ""},
		{"61 * * * *", ""},
		{"@fortnightly", ""},
		{"@every 30s", ""},
		{"0 * * * *", "Mars/Olympus_Mons"},
	} {
		if _, err := Parse(tc.expr, tc.tz); err == nil {
			t.Errorf("Parse(%q, %q) succeeded", tc.expr, tc.tz)
		}
	}
}

func TestNextInTimeZone(t *testing.T) {
	s := mustParse(t, "0 2 * * *", "Asia/Kolkata")
	got := s.Next(utc("2026-06-01T00:00:00Z"))
	if want := utc("2026-06-01T20:30:00Z"); !got.Equal(want) {
		t.Fatalf("Next = %v, want %v", got, want)
	}
	if s.String() != "0 2 * * *" || s.Location().String() != "Asia/Kolkata" {
		t.Fatal("accessors")
	}
	if mustParse(t, "@daily", "").Location() != time.UTC {
		t.Fatal("empty tz should be UTC")
	}
}

func TestDue(t *testing.T) {
	s := mustParse(t, "0 */6 * * *", "UTC")
	cases := []struct {
		name       string
		since, now string
		slot       string
		missed     int
		next       string
	}{
		{"nothing due", "2026-01-01T00:00:00Z", "2026-01-01T05:59:00Z", "", 0, "2026-01-01T06:00:00Z"},
		{"exactly on slot", "2026-01-01T00:00:00Z", "2026-01-01T06:00:00Z", "2026-01-01T06:00:00Z", 1, "2026-01-01T12:00:00Z"},
		{"missed three returns latest", "2026-01-01T00:00:00Z", "2026-01-01T19:00:00Z", "2026-01-01T18:00:00Z", 3, "2026-01-02T00:00:00Z"},
		{"since is exclusive", "2026-01-01T06:00:00Z", "2026-01-01T07:00:00Z", "", 0, "2026-01-01T12:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := s.Due(utc(tc.since), utc(tc.now))
			if err != nil {
				t.Fatal(err)
			}
			if tc.slot == "" {
				if !d.Slot.IsZero() {
					t.Fatalf("slot %v, want none", d.Slot)
				}
			} else if !d.Slot.Equal(utc(tc.slot)) {
				t.Fatalf("slot %v, want %v", d.Slot, tc.slot)
			}
			if d.Missed != tc.missed || !d.Next.Equal(utc(tc.next)) {
				t.Fatalf("missed=%d next=%v", d.Missed, d.Next)
			}
		})
	}
}

func TestDueTooManyMissed(t *testing.T) {
	s := mustParse(t, "* * * * *", "")
	d, err := s.Due(utc("2026-01-01T00:00:00Z"), utc("2026-02-01T00:00:00Z"))
	if !errors.Is(err, ErrTooManyMissed) {
		t.Fatalf("err = %v", err)
	}
	if d.Next.IsZero() {
		t.Fatal("next should still be reported")
	}
}

func TestDueNeverFires(t *testing.T) {
	s := mustParse(t, "0 0 30 2 *", "")
	d, err := s.Due(utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"))
	if err != nil || !d.Slot.IsZero() || !d.Next.IsZero() {
		t.Fatalf("d=%+v err=%v", d, err)
	}
}

// Feeding each returned slot back as "since" visits every slot exactly once
// when the controller wakes up at least once per slot.
func TestPropertyDueVisitsEverySlotOnce(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		expr := rapid.SampledFrom([]string{"0 * * * *", "*/15 * * * *", "30 2 * * *", "0 0 * * 1", "@every 45m"}).Draw(rt, "expr")
		tz := rapid.SampledFrom([]string{"UTC", "America/New_York", "Europe/London"}).Draw(rt, "tz")
		s := mustParse(t, expr, tz)
		start := utc("2026-03-01T00:00:00Z").Add(time.Duration(rapid.IntRange(0, 86400*30).Draw(rt, "start")) * time.Second)
		since := start
		var seen []time.Time
		for range 50 {
			// "@every" slots are anchored to the previous slot, so derive the
			// expected slot from since rather than from the wall clock.
			next := s.Next(since)
			// Wake at a random instant between this slot and the following one.
			after := s.Next(next)
			now := next.Add(time.Duration(rapid.Int64Range(0, int64(after.Sub(next))-1).Draw(rt, "jitter")))
			d, err := s.Due(since, now)
			if err != nil {
				rt.Fatal(err)
			}
			if d.Missed != 1 || !d.Slot.Equal(next) {
				rt.Fatalf("expected exactly slot %v, got %+v", next, d)
			}
			seen = append(seen, d.Slot)
			since = d.Slot
		}
		for i := 1; i < len(seen); i++ {
			if !seen[i].After(seen[i-1]) {
				rt.Fatal("slots not strictly increasing")
			}
		}
	})
}
