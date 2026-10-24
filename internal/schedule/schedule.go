// Package schedule evaluates cron schedules in a time zone and works out which
// schedule slot, if any, a controller should act on now.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// MaxMissed bounds how many slots Due walks before giving up. A policy that
// has missed more slots than this (an every-minute schedule after a week of
// operator downtime, say) is treated as "too far behind" instead of making a
// reconcile loop iterate without bound.
const MaxMissed = 10_000

// ErrTooManyMissed is returned by Due when more than MaxMissed slots passed.
var ErrTooManyMissed = errors.New("schedule: too many missed slots")

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Schedule is a parsed cron expression bound to a location.
type Schedule struct {
	expr string
	loc  *time.Location
	cron cron.Schedule
}

// Parse parses a 5-field cron expression or descriptor (@daily, "@every 1h")
// evaluated in the IANA zone tz ("" means UTC). TZ prefixes inside the
// expression are rejected so the zone has exactly one source of truth.
func Parse(expr, tz string) (*Schedule, error) {
	expr = strings.TrimSpace(expr)
	if strings.Contains(expr, "TZ=") {
		return nil, fmt.Errorf("schedule %q: set the time zone with spec.timeZone", expr)
	}
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("schedule: unknown time zone %q: %w", tz, err)
	}
	c, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: %w", expr, err)
	}
	// Backup names carry minute resolution, so slots must be a minute apart.
	if every, ok := c.(cron.ConstantDelaySchedule); ok && every.Delay < time.Minute {
		return nil, fmt.Errorf("schedule %q: @every intervals must be at least 1m", expr)
	}
	return &Schedule{expr: expr, loc: loc, cron: c}, nil
}

// String returns the expression.
func (s *Schedule) String() string { return s.expr }

// Location returns the evaluation zone.
func (s *Schedule) Location() *time.Location { return s.loc }

// Next returns the first slot strictly after t. "@every d" schedules are
// relative: the slot after t is t+d (truncated to the second), so they are
// anchored to the previous slot or the policy's creation time. A zero time means the
// schedule can never fire (e.g. "0 0 30 2 *").
func (s *Schedule) Next(t time.Time) time.Time {
	return s.cron.Next(t.In(s.loc))
}

// Decision is what Due concluded.
type Decision struct {
	// Slot is the most recent slot in (since, now]; zero if none.
	Slot time.Time
	// Missed counts slots in (since, now], including Slot.
	Missed int
	// Next is the first slot after now.
	Next time.Time
}

// Due reports the most recent slot after since and at or before now. A
// controller that records Slot as "handled" and calls Due again later gets
// each slot at most once; when several slots were missed only the most recent
// is returned, matching the CronJob controller. Cost is O(min(missed, MaxMissed)).
func (s *Schedule) Due(since, now time.Time) (Decision, error) {
	var d Decision
	for t := s.Next(since); !t.IsZero() && !t.After(now); t = s.Next(t) {
		d.Slot = t
		d.Missed++
		if d.Missed > MaxMissed {
			d.Next = s.Next(now)
			return d, ErrTooManyMissed
		}
	}
	d.Next = s.Next(now)
	return d, nil
}
