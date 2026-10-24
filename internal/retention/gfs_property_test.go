package retention

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// zones exercises fixed offsets, half-hour offsets, 30-minute DST shifts and
// a historical fall-back across midnight.
var zones = []string{"UTC", "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe", "America/Sao_Paulo", "Pacific/Chatham"}

func genPolicy() *rapid.Generator[Policy] {
	return rapid.Custom(func(t *rapid.T) Policy {
		n := func(label string) int { return rapid.IntRange(0, 12).Draw(t, label) }
		return Policy{Last: n("last"), Hourly: n("hourly"), Daily: n("daily"), Weekly: n("weekly"), Monthly: n("monthly"), Yearly: n("yearly")}
	})
}

func genItems() *rapid.Generator[[]Item] {
	return rapid.Custom(func(t *rapid.T) []Item {
		n := rapid.IntRange(0, 120).Draw(t, "n")
		// Concentrated around 2018 so the Sao Paulo transition is exercised.
		base := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
		span := rapid.SampledFrom([]int64{3600 * 6, 86400 * 3, 86400 * 90, 86400 * 365 * 4}).Draw(t, "span")
		items := make([]Item, n)
		for i := range n {
			items[i] = Item{
				ID:     fmt.Sprintf("b%03d", i),
				Time:   time.Unix(base+rapid.Int64Range(0, span).Draw(t, "offset"), 0).UTC(),
				Pinned: rapid.IntRange(0, 9).Draw(t, "pin") == 0,
			}
		}
		return items
	})
}

func genLocation(t *rapid.T) *time.Location {
	loc, err := time.LoadLocation(rapid.SampledFrom(zones).Draw(t, "zone"))
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func keptSet(ds []Decision) map[string]bool {
	s := map[string]bool{}
	for _, d := range ds {
		if d.Keep {
			s[d.ID] = true
		}
	}
	return s
}

// oracle is a deliberately naive restatement of the definition: group items by
// period, take each period's newest item, rank periods by that item, keep the
// first N.
func oracle(p Policy, items []Item, loc *time.Location) map[string]bool {
	sorted := slices.Clone(items)
	SortNewestFirst(sorted)
	kept := map[string]bool{}
	for i := 0; i < len(sorted) && i < p.Last; i++ {
		kept[sorted[i].ID] = true
	}
	for _, r := range []struct {
		n   int
		key periodKey
	}{{p.Hourly, hourKey}, {p.Daily, dayKey}, {p.Weekly, weekKey}, {p.Monthly, monthKey}, {p.Yearly, yearKey}} {
		newest := map[int64]Item{}
		for _, it := range sorted {
			k := r.key(it.Time.In(loc))
			if _, ok := newest[k]; !ok {
				newest[k] = it
			}
		}
		heads := slices.Collect(maps.Values(newest))
		SortNewestFirst(heads)
		for i := 0; i < len(heads) && i < r.n; i++ {
			kept[heads[i].ID] = true
		}
	}
	for _, it := range items {
		if it.Pinned {
			kept[it.ID] = true
		}
	}
	return kept
}

func TestPropertyMatchesOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		ds, err := Apply(p, items, loc)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := keptSet(ds), oracle(p, items, loc); !maps.Equal(got, want) {
			t.Fatalf("kept %v, oracle %v", got, want)
		}
	})
}

func TestPropertyPartitionAndOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		ds, _ := Apply(p, items, loc)
		if len(ds) != len(items) {
			t.Fatalf("got %d decisions for %d items", len(ds), len(items))
		}
		kept, pruned := Split(ds)
		if len(kept)+len(pruned) != len(items) {
			t.Fatal("kept and pruned do not partition the input")
		}
		for i := 1; i < len(ds); i++ {
			if ds[i].Time.After(ds[i-1].Time) {
				t.Fatal("decisions are not newest first")
			}
		}
		for _, d := range ds {
			if d.Keep != (len(d.Reasons) > 0) {
				t.Fatalf("%s keep=%v reasons=%v", d.ID, d.Keep, d.Reasons)
			}
			if d.Pinned && !d.Keep {
				t.Fatalf("pinned %s was pruned", d.ID)
			}
		}
	})
}

func TestPropertyBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		ds, _ := Apply(p, items, loc)
		unpinnedKept := 0
		for _, d := range ds {
			if d.Keep && !d.Pinned {
				unpinnedKept++
			}
		}
		if unpinnedKept > p.MaxKept() {
			t.Fatalf("kept %d unpinned items, bound %d", unpinnedKept, p.MaxKept())
		}
		if len(ds) > 0 && !p.Empty() && !ds[0].Keep {
			t.Fatal("the newest item must survive a non-empty policy")
		}
		if p.Empty() {
			for _, d := range ds {
				if d.Keep && !d.Pinned {
					t.Fatalf("empty policy kept %s", d.ID)
				}
			}
		}
	})
}

// Re-applying the policy to the survivors must keep every survivor: pruning is
// stable, so a controller that runs retention on every reconcile converges.
func TestPropertyIdempotent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		ds, _ := Apply(p, items, loc)
		var survivors []Item
		for _, d := range ds {
			if d.Keep {
				survivors = append(survivors, d.Item)
			}
		}
		again, _ := Apply(p, survivors, loc)
		for _, d := range again {
			if !d.Keep {
				t.Fatalf("second pass pruned survivor %s", d.ID)
			}
		}
	})
}

// Raising any count never prunes something a smaller count kept.
func TestPropertyMonotonic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		bigger := p
		switch rapid.IntRange(0, 5).Draw(t, "field") {
		case 0:
			bigger.Last++
		case 1:
			bigger.Hourly++
		case 2:
			bigger.Daily++
		case 3:
			bigger.Weekly++
		case 4:
			bigger.Monthly++
		default:
			bigger.Yearly++
		}
		small, _ := Apply(p, items, loc)
		large, _ := Apply(bigger, items, loc)
		ls := keptSet(large)
		for id := range keptSet(small) {
			if !ls[id] {
				t.Fatalf("%s kept by %+v but pruned by %+v", id, p, bigger)
			}
		}
	})
}

// Adding a new, newest item prunes at most one previously kept item per rule.
func TestPropertyNewItemChurnBounded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p, items, loc := genPolicy().Draw(t, "policy"), genItems().Draw(t, "items"), genLocation(t)
		latest := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
		for _, it := range items {
			if it.Time.After(latest) {
				latest = it.Time
			}
		}
		next := Item{ID: "new", Time: latest.Add(time.Duration(rapid.IntRange(1, 86400*40).Draw(t, "gap")) * time.Second)}
		before, _ := Apply(p, items, loc)
		after, _ := Apply(p, append(slices.Clone(items), next), loc)
		as := keptSet(after)
		if !p.Empty() && !as["new"] {
			t.Fatal("new newest item was pruned")
		}
		lost := 0
		for id := range keptSet(before) {
			if !as[id] {
				lost++
			}
		}
		if lost > 6 {
			t.Fatalf("one new item pruned %d previously kept items", lost)
		}
	})
}
