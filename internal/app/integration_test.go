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

// TestIntegrationStreamToBatchToAnalyzer verifies the full flow: stream
// produces an error event, the batch is emitted to the monitor, a process
// decision runs the agent, and the done frame carries the analyzer output.
func TestIntegrationStreamToBatchToAnalyzer(t *testing.T) {
	mock := &mockAgent{result: "integration: root cause identified"}
	mon := &mockMonitor{}

	logger := log.New(&bytes.Buffer{}, "", 0)

	app := &application{
		agent:         mock,
		store:         store.New(),
		monitor:       mon,
		events:        make(chan stream.Event, 64),
		logger:        logger,
		loopDone:      make(chan struct{}),
		batchDebounce: 20 * time.Millisecond,
		batchSize:     50,
		queueMax:      3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(16)
	app.AddStream(s)

	for range 5 {
		s.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "api", Message: "normal log entry"})
	}
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "database connection failed"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id
	q := mon.getQueued()[0]
	if q.summary.Count != 6 {
		t.Errorf("batch summary count = %d, want 6", q.summary.Count)
	}
	if q.summary.FirstError != "database connection failed" {
		t.Errorf("summary first_error = %q", q.summary.FirstError)
	}

	if err := app.Decide(batchID, stream.DecideProcess); err != nil {
		t.Fatalf("Decide: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		dones := mon.getDones()
		return len(dones) == 1 && dones[0].summary == "integration: root cause identified"
	})
	if got := mock.getProcessCalls(); got != 1 {
		t.Errorf("processCalls = %d, want 1", got)
	}

	s.Close()
	cancel()
	app.Wait()
}

// TestIntegrationMultipleStreams verifies that multiple concurrent streams
// funnel into the same event loop.
func TestIntegrationMultipleStreams(t *testing.T) {
	mock := &mockAgent{result: "multi-stream analysis"}
	mon := &mockMonitor{}

	logger := log.New(&bytes.Buffer{}, "", 0)

	app := &application{
		agent:         mock,
		store:         store.New(),
		monitor:       mon,
		events:        make(chan stream.Event, 64),
		logger:        logger,
		loopDone:      make(chan struct{}),
		batchDebounce: 20 * time.Millisecond,
		batchSize:     50,
		queueMax:      3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s1 := stream.NewHTTPIngestStream(8)
	s2 := stream.NewHTTPIngestStream(8)
	app.AddStream(s1)
	app.AddStream(s2)

	s1.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "svc1", Message: "ping"})
	s2.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "svc2", Message: "crash"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id
	if err := app.Decide(batchID, stream.DecideProcess); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	waitFor(t, time.Second, func() bool { return mock.getProcessCalls() == 1 })

	s1.Close()
	s2.Close()
	cancel()
	app.Wait()
}
