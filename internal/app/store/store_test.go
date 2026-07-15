package store

import (
	"testing"
	"time"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

func makeEvent(sev, msg string) stream.Event {
	return stream.Event{Timestamp: time.Now(), Severity: sev, Service: "svc", Message: msg}
}

func TestAppendAndRecentContext(t *testing.T) {
	s := New()

	// Empty store returns empty slice.
	if got := s.RecentContext(10); len(got) != 0 {
		t.Errorf("empty store: got %d events, want 0", len(got))
	}

	s.Append(makeEvent("info", "first"))
	s.Append(makeEvent("error", "second"))
	s.Append(makeEvent("info", "third"))

	got := s.RecentContext(10)
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
	if got[0].Message != "first" || got[2].Message != "third" {
		t.Errorf("unexpected order: %v", got)
	}
}

func TestRecentContextWindowCapped(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		s.Append(makeEvent("info", "msg"))
	}

	got := s.RecentContext(3)
	if len(got) != 3 {
		t.Errorf("got %d events, want 3", len(got))
	}
}

func TestRingBufferEviction(t *testing.T) {
	s := &LogStore{ring: make([]entry, 0, 3), ringSize: 3}

	s.Append(makeEvent("info", "a"))
	s.Append(makeEvent("info", "b"))
	s.Append(makeEvent("info", "c"))
	s.Append(makeEvent("info", "d")) // evicts "a"

	got := s.RecentContext(10)
	if len(got) != 3 {
		t.Fatalf("ring size = %d, want 3", len(got))
	}
	if got[0].Message != "b" {
		t.Errorf("first message = %q, want %q", got[0].Message, "b")
	}
	if got[2].Message != "d" {
		t.Errorf("last message = %q, want %q", got[2].Message, "d")
	}
}

func TestSearch(t *testing.T) {
	s := New()
	s.Append(makeEvent("error", "database connection failed"))
	s.Append(makeEvent("info", "all systems operational"))
	s.Append(makeEvent("warn", "database slow query detected"))

	results := s.Search("database", 10)
	if len(results) != 2 {
		t.Fatalf("search results = %d, want 2", len(results))
	}
}

func TestSearchCaseInsensitive(t *testing.T) {
	s := New()
	s.Append(makeEvent("error", "TIMEOUT connecting to Redis"))

	results := s.Search("timeout", 10)
	if len(results) != 1 {
		t.Fatalf("case-insensitive search: got %d results, want 1", len(results))
	}
}

func TestSnapshotReturnsSeq(t *testing.T) {
	s := New()
	s.Append(makeEvent("info", "a"))
	s.Append(makeEvent("info", "b"))
	seqC := s.Append(makeEvent("error", "c"))

	events, id, lastSeq := s.Snapshot(2)
	if len(events) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(events))
	}
	if id == "" {
		t.Error("id empty")
	}
	if lastSeq != seqC {
		t.Errorf("lastSeq = %d, want %d", lastSeq, seqC)
	}
}

func TestSearchLimit(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		s.Append(makeEvent("error", "repeated error"))
	}

	results := s.Search("repeated", 3)
	if len(results) != 3 {
		t.Errorf("search limit: got %d results, want 3", len(results))
	}
}
