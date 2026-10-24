package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunCommands(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"version"}, nil, &out, &errOut); err != nil || strings.TrimSpace(out.String()) != "dev" {
		t.Fatalf("version: %q %v", out.String(), err)
	}
	if err := run(nil, nil, &out, &errOut); err == nil {
		t.Fatal("no command accepted")
	}
	if err := run([]string{"frobnicate"}, nil, &out, &errOut); err == nil || !strings.Contains(errOut.String(), "usage") {
		t.Fatal("unknown command accepted")
	}
	out.Reset()
	if err := run([]string{"help"}, nil, &out, &errOut); err != nil || !strings.Contains(out.String(), "retention") {
		t.Fatal("help")
	}
	if err := run([]string{"mover", "--mode=backup"}, nil, &out, &errOut); err == nil {
		t.Fatal("mover without keys accepted")
	}
	t.Setenv("KEEPER_S3_ENDPOINT", "")
	if err := run([]string{"mover", "--mode=restore", "--archive-key=k"}, nil, &out, &errOut); err == nil {
		t.Fatal("mover without storage env accepted")
	}
	if err := run([]string{"manager", "--bogus"}, nil, &out, &errOut); err == nil {
		t.Fatal("bad manager flag accepted")
	}
}

func TestRetentionPreview(t *testing.T) {
	in := strings.NewReader(`# nightly backups
2026-03-01T02:00:00Z
2026-03-02T02:00:00Z
2026-03-03T02:00:00Z

2026-03-03T14:00:00Z
`)
	var out bytes.Buffer
	if err := run([]string{"retention", "--keep-last=1", "--keep-daily=2"}, in, &out, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"2026-03-03T14:00:00Z  keep     last,daily",
		"2026-03-03T02:00:00Z  prune",
		"2026-03-02T02:00:00Z  keep     daily",
		"2 kept, 2 pruned",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if err := run([]string{"retention"}, strings.NewReader("yesterday\n"), &out, &out); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("bad timestamp: %v", err)
	}
	if err := run([]string{"retention", "--time-zone=Nowhere/City"}, strings.NewReader(""), &out, &out); err == nil {
		t.Fatal("bad zone accepted")
	}
}
