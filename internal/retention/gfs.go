// Package retention implements grandfather-father-son (GFS) retention.
//
// A Policy holds one count per rule (last, hourly, daily, weekly, monthly,
// yearly). Each periodic rule partitions time into periods (wall-clock hours,
// days, ISO weeks, months, years in a given location) and keeps the newest
// item of each of the N most recent periods that contain an item. The "last"
// rule keeps the N newest items. An item survives if at least one rule keeps
// it, or if it is pinned.
//
// The semantics match restic's "forget --keep-*" flags, so operators can
// reason about the policy with a familiar model. Apply runs in O(n log n) for
// the sort plus O(n·r) for r rules, and allocates O(n).
package retention

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Rule names a retention rule.
type Rule string

// Rules, in the order they are reported.
const (
	RuleLast    Rule = "last"
	RuleHourly  Rule = "hourly"
	RuleDaily   Rule = "daily"
	RuleWeekly  Rule = "weekly"
	RuleMonthly Rule = "monthly"
	RuleYearly  Rule = "yearly"
	RulePinned  Rule = "pinned"
)

// Policy holds the count for every rule. Negative counts are treated as zero.
type Policy struct {
	Last, Hourly, Daily, Weekly, Monthly, Yearly int
}

// Empty reports whether the policy keeps nothing (other than pinned items).
func (p Policy) Empty() bool {
	return max(p.Last, 0)+max(p.Hourly, 0)+max(p.Daily, 0)+max(p.Weekly, 0)+max(p.Monthly, 0)+max(p.Yearly, 0) == 0
}

// MaxKept is an upper bound on the number of unpinned items Apply keeps.
func (p Policy) MaxKept() int {
	return max(p.Last, 0) + max(p.Hourly, 0) + max(p.Daily, 0) + max(p.Weekly, 0) + max(p.Monthly, 0) + max(p.Yearly, 0)
}

// Item is one candidate for retention.
type Item struct {
	// ID uniquely identifies the item; it breaks ties between equal times.
	ID string
	// Time is the point in time the item represents.
	Time time.Time
	// Pinned items are always kept. They still occupy the periods they fall in.
	Pinned bool
}

// Decision is the verdict for one item.
type Decision struct {
	Item
	// Keep is true if any rule (or the pin) keeps the item.
	Keep bool
	// Reasons lists the rules that keep the item, in rule order.
	Reasons []Rule
}

// ReasonString renders Reasons as a comma-separated list.
func (d Decision) ReasonString() string {
	parts := make([]string, len(d.Reasons))
	for i, r := range d.Reasons {
		parts[i] = string(r)
	}
	return strings.Join(parts, ",")
}

// periodKey maps an instant to an opaque period identifier.
type periodKey func(t time.Time) int64

func hourKey(t time.Time) int64 {
	return int64(t.Year())*1_000_000 + int64(t.YearDay())*100 + int64(t.Hour())
}

func dayKey(t time.Time) int64 { return int64(t.Year())*1000 + int64(t.YearDay()) }

func weekKey(t time.Time) int64 {
	y, w := t.ISOWeek()
	return int64(y)*100 + int64(w)
}

func monthKey(t time.Time) int64 { return int64(t.Year())*100 + int64(t.Month()) }

func yearKey(t time.Time) int64 { return int64(t.Year()) }

type periodicRule struct {
	rule  Rule
	count int
	key   periodKey
}

// SortNewestFirst orders items by time descending, then ID ascending, which
// is the canonical order Apply reports decisions in.
func SortNewestFirst(items []Item) {
	slices.SortStableFunc(items, func(a, b Item) int {
		if c := b.Time.Compare(a.Time); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
}

// Apply evaluates the policy over items. Periods are computed in loc (UTC if
// nil). The result has one Decision per item, ordered newest first. Duplicate
// IDs are rejected because they would make the verdict ambiguous.
func Apply(p Policy, items []Item, loc *time.Location) ([]Decision, error) {
	if loc == nil {
		loc = time.UTC
	}
	seen := make(map[string]struct{}, len(items))
	for _, it := range items {
		if _, dup := seen[it.ID]; dup {
			return nil, fmt.Errorf("retention: duplicate item id %q", it.ID)
		}
		seen[it.ID] = struct{}{}
	}

	sorted := slices.Clone(items)
	SortNewestFirst(sorted)

	decisions := make([]Decision, len(sorted))
	for i, it := range sorted {
		decisions[i] = Decision{Item: it}
	}

	for i := 0; i < len(decisions) && i < max(p.Last, 0); i++ {
		decisions[i].Reasons = append(decisions[i].Reasons, RuleLast)
	}

	rules := []periodicRule{
		{RuleHourly, p.Hourly, hourKey},
		{RuleDaily, p.Daily, dayKey},
		{RuleWeekly, p.Weekly, weekKey},
		{RuleMonthly, p.Monthly, monthKey},
		{RuleYearly, p.Yearly, yearKey},
	}
	for _, r := range rules {
		if r.count <= 0 {
			continue
		}
		applyPeriodic(decisions, r, loc)
	}

	for i := range decisions {
		if decisions[i].Pinned {
			decisions[i].Reasons = append(decisions[i].Reasons, RulePinned)
		}
		decisions[i].Keep = len(decisions[i].Reasons) > 0
	}
	return decisions, nil
}

// applyPeriodic walks newest-first decisions. The first item seen in a period
// is the newest item of that period; it is kept until count distinct periods
// have been used. A set (rather than "key changed since the previous item")
// keeps the rule correct in zones whose clocks fall back across midnight,
// where a calendar day can appear twice in a newest-first walk.
func applyPeriodic(decisions []Decision, r periodicRule, loc *time.Location) {
	used := make(map[int64]struct{}, min(r.count, len(decisions)))
	for i := range decisions {
		k := r.key(decisions[i].Time.In(loc))
		if _, ok := used[k]; ok {
			continue
		}
		used[k] = struct{}{}
		decisions[i].Reasons = append(decisions[i].Reasons, r.rule)
		if len(used) == r.count {
			return
		}
	}
}

// Split partitions decisions into kept and pruned items, preserving order.
func Split(decisions []Decision) (kept, pruned []Decision) {
	for _, d := range decisions {
		if d.Keep {
			kept = append(kept, d)
		} else {
			pruned = append(pruned, d)
		}
	}
	return kept, pruned
}
