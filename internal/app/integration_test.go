package app

import (
	"bytes"
	"context"
	"log"
	"testing"
	"time"

	"github.com/tab58/heimdall-log-router/internal/app/store"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// TestIntegrationStreamToAgentToOutput verifies the full flow:
// stream produces an error event → agent processes it → output sink receives the result.
func TestIntegrationStreamToAgentToOutput(t *testing.T) {
	mock := &mockAgent{result: "integration test: root cause identified"}
	out := &mockOutput{}

	logger := log.New(&bytes.Buffer{}, "", 0)

	app := &application{
		agent:    mock,
		store:    store.New(),
		debounce: debouncer{cooldown: 0},
		events:   make(chan stream.Event, 64),
		logger:   logger,
		loopDone: make(chan struct{}),
		output:   out,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	// Create a stream and send some context events followed by an error.
	s := stream.NewHTTPIngestStream(16)
	app.AddStream(s)

	for i := 0; i < 5; i++ {
		s.Write(stream.Event{
			Timestamp: time.Now(),
			Severity:  "info",
			Service:   "api",
			Message:   "normal log entry",
		})
	}

	s.Write(stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "database connection failed",
	})
	s.Close()

	app.Wait()

	if got := mock.getProcessCalls(); got != 1 {
		t.Errorf("processCalls = %d, want 1", got)
	}

	writes := out.getWrites()
	if len(writes) != 1 {
		t.Fatalf("output writes = %d, want 1", len(writes))
	}
	if writes[0].Result != "integration test: root cause identified" {
		t.Errorf("result = %q, want %q", writes[0].Result, "integration test: root cause identified")
	}
	if writes[0].ID == "" {
		t.Error("result ID should not be empty")
	}
}

// TestIntegrationMultipleStreams verifies that multiple concurrent streams all feed
// into the same event loop.
func TestIntegrationMultipleStreams(t *testing.T) {
	mock := &mockAgent{result: "multi-stream analysis"}

	logger := log.New(&bytes.Buffer{}, "", 0)

	app := &application{
		agent:    mock,
		store:    store.New(),
		debounce: debouncer{cooldown: 0},
		events:   make(chan stream.Event, 64),
		logger:   logger,
		loopDone: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	// Attach two streams.
	s1 := stream.NewHTTPIngestStream(8)
	s2 := stream.NewHTTPIngestStream(8)
	app.AddStream(s1)
	app.AddStream(s2)

	// Send an info event from stream 1, an error from stream 2.
	s1.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "svc1", Message: "ping"})
	s2.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "svc2", Message: "crash"})
	s1.Close()
	s2.Close()

	app.Wait()

	if got := mock.getProcessCalls(); got != 1 {
		t.Errorf("processCalls = %d, want 1 (one error event)", got)
	}
}
