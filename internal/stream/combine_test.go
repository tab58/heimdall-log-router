package stream

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeStream is a test helper used by combine_test.go and merge_test.go.
type fakeStream struct {
	ch     chan Event
	mu     sync.Mutex
	closed bool
}

func newFakeStream(bufSize int) *fakeStream {
	return &fakeStream{ch: make(chan Event, bufSize)}
}

func (f *fakeStream) Events() <-chan Event { return f.ch }

func (f *fakeStream) Write(e Event) { f.ch <- e }

func (f *fakeStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		close(f.ch)
		f.closed = true
	}
	return nil
}

func (f *fakeStream) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func TestCombine_MergesAllEvents(t *testing.T) {
	a := newFakeStream(4)
	b := newFakeStream(4)
	c := newFakeStream(4)
	combined := Combine(a, b, c)

	go func() {
		a.Write(Event{Message: "a1"})
		a.Write(Event{Message: "a2"})
		a.Close()
	}()
	go func() {
		b.Write(Event{Message: "b1"})
		b.Close()
	}()
	go func() {
		c.Write(Event{Message: "c1"})
		c.Write(Event{Message: "c2"})
		c.Write(Event{Message: "c3"})
		c.Close()
	}()

	var got []string
	for e := range combined.Events() {
		got = append(got, e.Message)
	}
	sort.Strings(got)
	want := []string{"a1", "a2", "b1", "c1", "c2", "c3"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCombine_ClosesOnlyAfterAllInputs(t *testing.T) {
	a := newFakeStream(1)
	b := newFakeStream(1)
	combined := Combine(a, b)

	a.Close()

	done := make(chan struct{})
	go func() {
		for range combined.Events() {
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("combined closed before all inputs closed")
	case <-time.After(20 * time.Millisecond):
	}

	b.Close()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("combined did not close after all inputs closed")
	}
}

func TestCombine_CloseStopsAllSources(t *testing.T) {
	a := newFakeStream(1)
	b := newFakeStream(1)
	combined := Combine(a, b)

	if err := combined.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !a.isClosed() || !b.isClosed() {
		t.Error("expected all sources to be closed")
	}

	for range combined.Events() {
	}
}

func TestCombine_CloseUnblocksStuckWorkers(t *testing.T) {
	// Flood sources past the output buffer without reading — workers should
	// be blocked on `out <- e`. Close must unblock them so Wait-style drains
	// can complete instead of leaking goroutines.
	a := newFakeStream(defaultBufferSize * 4)
	b := newFakeStream(defaultBufferSize * 4)
	for range defaultBufferSize * 4 {
		a.Write(Event{Message: "a"})
		b.Write(Event{Message: "b"})
	}

	combined := Combine(a, b)

	// Give workers time to fill `out` and park on the send.
	time.Sleep(20 * time.Millisecond)

	if err := combined.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for range combined.Events() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("combined did not drain after Close — workers likely leaked")
	}
}

func TestCombine_Empty(t *testing.T) {
	combined := Combine()
	select {
	case _, ok := <-combined.Events():
		if ok {
			t.Error("expected closed channel from empty Combine")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("empty Combine did not close")
	}
}
