package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tab58/heimdall-log-router/internal/app/agent"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// fakeClaude writes a fake claude binary to dir and prepends dir to PATH.
// The binary reads stdin (discarding it) and writes output to stdout.
func fakeClaude(t *testing.T, output string) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\ncat > /dev/null\necho " + "'" + output + "'"
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestAnalyzerProcess(t *testing.T) {
	want := "Root Cause: disk full\nFix: clear /tmp"
	fakeClaude(t, want)

	a := agent.NewAnalyzer()
	e := stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "db",
		Message:   "no space left on device",
	}
	got, err := a.Process(context.Background(), []stream.Event{e})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAnalyzerProcessWithContext(t *testing.T) {
	want := "Root Cause: cascading timeout"
	fakeClaude(t, want)

	a := agent.NewAnalyzer()
	trigger := stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "request timeout",
	}
	ctxEvents := []stream.Event{
		{Timestamp: time.Now().Add(-2 * time.Second), Severity: "warn", Service: "db", Message: "slow query"},
		{Timestamp: time.Now().Add(-1 * time.Second), Severity: "warn", Service: "cache", Message: "cache miss"},
	}

	got, err := a.Process(context.Background(), append(ctxEvents, trigger))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAnalyzerProcessCancelled(t *testing.T) {
	// Fake claude that sleeps — context cancellation should kill it.
	dir := t.TempDir()
	script := "#!/bin/sh\nsleep 10\necho done"
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	a := agent.NewAnalyzer()
	_, err := a.Process(ctx, []stream.Event{{Severity: "error"}})
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func TestBuildPromptContainsErrorDetails(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	e := stream.Event{
		Timestamp: ts,
		Severity:  "error",
		Service:   "payments",
		Message:   "charge failed: card declined",
	}

	got := agent.BuildPrompt(e, nil)

	checks := []string{
		"payments",
		"charge failed: card declined",
		ts.Format(time.RFC3339),
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Errorf("prompt missing %q\nprompt:\n%s", want, got)
		}
	}
}

func TestBuildPromptIncludesContextLogs(t *testing.T) {
	e := stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"}
	ctx := []stream.Event{
		{Timestamp: time.Now(), Severity: "warn", Service: "db", Message: "slow query"},
		{Timestamp: time.Now(), Severity: "info", Service: "cache", Message: "eviction"},
	}

	got := agent.BuildPrompt(e, ctx)

	for _, want := range []string{"slow query", "eviction", "db", "cache"} {
		if !strings.Contains(got, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}
