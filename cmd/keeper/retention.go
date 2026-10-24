package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/archit2501/keeper/internal/retention"
)

// previewRetention reads RFC 3339 timestamps (one per line, blank lines and
// #-comments ignored) and prints the verdict for each, newest first.
func previewRetention(p retention.Policy, tz string, in io.Reader, out io.Writer) error {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return fmt.Errorf("time zone %q: %w", tz, err)
	}
	var items []retention.Item
	sc := bufio.NewScanner(in)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		t, err := time.Parse(time.RFC3339, text)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		items = append(items, retention.Item{ID: fmt.Sprintf("#%d", line), Time: t})
	}
	if err := sc.Err(); err != nil {
		return err
	}
	decisions, err := retention.Apply(p, items, loc)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TIME\tVERDICT\tKEPT BY")
	kept := 0
	for _, d := range decisions {
		verdict := "prune"
		if d.Keep {
			verdict = "keep"
			kept++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", d.Time.In(loc).Format(time.RFC3339), verdict, d.ReasonString())
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%d kept, %d pruned\n", kept, len(decisions)-kept)
	return err
}
