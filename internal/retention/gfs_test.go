package retention

import (
	"slices"
	"testing"
	"time"
)

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func keptIDs(t *testing.T, p Policy, items []Item, loc *time.Location) []string {
	t.Helper()
	ds, err := Apply(p, items, loc)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, d := range ds {
		if d.Keep {
			out = append(out, d.ID)
		}
	}
	return out
}

// hourlyItems returns n items one hour apart, newest last, named h0..h(n-1).
func hourlyItems(start time.Time, n int, step time.Duration) []Item {
	items := make([]Item, n)
	for i := range n {
		items[i] = Item{ID: start.Add(time.Duration(i) * step).Format("2006-01-02T15"), Time: start.Add(time.Duration(i) * step)}
	}
	return items
}

func TestApplyTable(t *testing.T) {
	sixHourly := hourlyItems(at("2026-01-01T00:00:00Z"), 4*20, 6*time.Hour) // 20 days, 4 per day

	cases := []struct {
		name  string
		p     Policy
		items []Item
		want  []string
	}{
		{"empty input", Policy{Last: 3}, nil, nil},
		{"zero policy prunes all", Policy{}, sixHourly[:3], nil},
		{"keep last 2", Policy{Last: 2}, sixHourly[:5], []string{"2026-01-02T00", "2026-01-01T18"}},
		{
			"daily keeps newest per day",
			Policy{Daily: 3},
			sixHourly,
			[]string{"2026-01-20T18", "2026-01-19T18", "2026-01-18T18"},
		},
		{
			"weekly uses ISO weeks",
			Policy{Weekly: 2},
			sixHourly,
			// 2026-01-20 is a Tuesday (ISO week 4); week 3 ends Sunday 2026-01-18.
			[]string{"2026-01-20T18", "2026-01-18T18"},
		},
		{
			"rules union",
			Policy{Last: 1, Daily: 2, Monthly: 1},
			sixHourly,
			[]string{"2026-01-20T18", "2026-01-19T18"},
		},
		{
			"more periods requested than exist keeps one per period",
			Policy{Daily: 100},
			sixHourly[:8],
			[]string{"2026-01-02T18", "2026-01-01T18"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := keptIDs(t, tc.p, tc.items, time.UTC)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("kept %v, want %v", got, tc.want)
			}
		})
	}
}

func TestApplyReasonsAndPins(t *testing.T) {
	items := []Item{
		{ID: "a", Time: at("2026-03-01T10:00:00Z")},
		{ID: "b", Time: at("2026-03-01T09:00:00Z")},
		{ID: "c", Time: at("2026-02-27T09:00:00Z"), Pinned: true},
	}
	ds, err := Apply(Policy{Last: 1, Daily: 1, Monthly: 2}, items, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "last,daily,monthly", "b": "", "c": "monthly,pinned"}
	for _, d := range ds {
		if got := d.ReasonString(); got != want[d.ID] {
			t.Errorf("%s reasons = %q, want %q", d.ID, got, want[d.ID])
		}
		if d.Keep != (want[d.ID] != "") {
			t.Errorf("%s keep = %v", d.ID, d.Keep)
		}
	}
	kept, pruned := Split(ds)
	if len(kept) != 2 || len(pruned) != 1 || pruned[0].ID != "b" {
		t.Fatalf("split kept=%v pruned=%v", kept, pruned)
	}
}

func TestApplyTimeZoneChangesDayBoundaries(t *testing.T) {
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	// 20:00Z and 17:00Z are the same UTC day but different days in UTC+5:30.
	items := []Item{
		{ID: "late", Time: at("2026-05-01T20:00:00Z")},
		{ID: "early", Time: at("2026-05-01T17:00:00Z")},
	}
	if got := keptIDs(t, Policy{Daily: 5}, items, time.UTC); !slices.Equal(got, []string{"late"}) {
		t.Fatalf("UTC kept %v", got)
	}
	if got := keptIDs(t, Policy{Daily: 5}, items, kolkata); !slices.Equal(got, []string{"late", "early"}) {
		t.Fatalf("Kolkata kept %v", got)
	}
}

func TestApplyFallBackAcrossMidnight(t *testing.T) {
	// Until 2019 Sao Paulo ended DST at 00:00 by moving clocks back to 23:00,
	// so the evening of 2018-02-17 happened twice.
	sp, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatal(err)
	}
	items := []Item{
		{ID: "t0", Time: at("2018-02-18T01:50:00Z")}, // 23:50 -02 on the 17th
		{ID: "t1", Time: at("2018-02-18T02:10:00Z")}, // 23:10 -03 on the 17th, again
		{ID: "t2", Time: at("2018-02-18T03:30:00Z")}, // 00:30 -03 on the 18th
	}
	got := keptIDs(t, Policy{Daily: 2}, items, sp)
	if !slices.Equal(got, []string{"t2", "t1"}) {
		t.Fatalf("kept %v, want newest of the 18th and of the 17th", got)
	}
}

func TestApplyRejectsDuplicateIDs(t *testing.T) {
	_, err := Apply(Policy{Last: 1}, []Item{{ID: "x", Time: at("2026-01-01T00:00:00Z")}, {ID: "x", Time: at("2026-01-02T00:00:00Z")}}, nil)
	if err == nil {
		t.Fatal("expected duplicate id error")
	}
}

func TestPolicyHelpers(t *testing.T) {
	if !(Policy{Last: -3}).Empty() {
		t.Error("negative counts should be empty")
	}
	if got := (Policy{Last: 1, Daily: 7, Weekly: -1}).MaxKept(); got != 8 {
		t.Errorf("MaxKept = %d", got)
	}
}

func BenchmarkApply10k(b *testing.B) {
	items := hourlyItems(at("2024-01-01T00:00:00Z"), 10_000, time.Hour)
	p := Policy{Last: 24, Hourly: 48, Daily: 30, Weekly: 12, Monthly: 24, Yearly: 5}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Apply(p, items, time.UTC); err != nil {
			b.Fatal(err)
		}
	}
}
