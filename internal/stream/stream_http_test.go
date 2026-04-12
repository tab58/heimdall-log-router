package stream

import (
	"testing"
	"time"
)

// TestHTTPIngestStreamImplementsStream verifies compile-time interface satisfaction.
var _ ReadStream = (*HTTPIngestStream)(nil)

// TestHTTPIngestStreamWriteAndRead verifies that written events are received on the channel.
func TestHTTPIngestStreamWriteAndRead(t *testing.T) {
	s := NewHTTPIngestStream(8)

	e := Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "test error",
	}
	s.Write(e)
	s.Close()

	var received []Event
	for evt := range s.Events() {
		received = append(received, evt)
	}

	if len(received) != 1 {
		t.Fatalf("received %d events, want 1", len(received))
	}
	if received[0].Message != "test error" {
		t.Errorf("Message = %q, want %q", received[0].Message, "test error")
	}
	if received[0].Severity != "error" {
		t.Errorf("Severity = %q, want %q", received[0].Severity, "error")
	}
}

// TestHTTPIngestStreamMultipleEvents verifies ordering is preserved.
func TestHTTPIngestStreamMultipleEvents(t *testing.T) {
	s := NewHTTPIngestStream(16)

	messages := []string{"first", "second", "third"}
	for _, msg := range messages {
		s.Write(Event{Message: msg})
	}
	s.Close()

	var received []Event
	for evt := range s.Events() {
		received = append(received, evt)
	}

	if len(received) != len(messages) {
		t.Fatalf("received %d events, want %d", len(received), len(messages))
	}
	for i, want := range messages {
		if received[i].Message != want {
			t.Errorf("event[%d].Message = %q, want %q", i, received[i].Message, want)
		}
	}
}

// TestHTTPIngestStreamClosedChannelSignal verifies Events() channel closes after Close().
func TestHTTPIngestStreamClosedChannelSignal(t *testing.T) {
	s := NewHTTPIngestStream(4)
	s.Close()

	// Channel should be immediately closed; range should exit without blocking.
	done := make(chan struct{})
	go func() {
		for range s.Events() {
		}
		close(done)
	}()

	select {
	case <-done:
		// expected
	case <-time.After(time.Second):
		t.Fatal("Events() channel was not closed after Close()")
	}
}
