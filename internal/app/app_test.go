package app

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/tab58/heimdall-log-router/internal/app/agent"
	"github.com/tab58/heimdall-log-router/internal/app/store"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// mockAgent is a test double for agent.Agent.
type mockAgent struct {
	mu           sync.Mutex
	result       string
	err          error
	processCalls int
	// onProcess, if set, is invoked inside Process before returning.
	onProcess func()
}

func (m *mockAgent) Process(_ context.Context, _ string, _ []stream.Event) (string, error) {
	m.mu.Lock()
	m.processCalls++
	fn := m.onProcess
	r := m.result
	e := m.err
	m.mu.Unlock()
	if fn != nil {
		fn()
	}
	return r, e
}

func (m *mockAgent) getProcessCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processCalls
}

type queuedFrame struct {
	id      string
	summary stream.BatchSummary
}

type removedFrame struct {
	id     string
	reason string
}

// mockMonitor captures frames sent through the monitor output.
type mockMonitor struct {
	mu       sync.Mutex
	events   []stream.Event
	queued   []queuedFrame
	removed  []removedFrame
	notices  []string
	statuses []string
	deltas   []string
	dones    []doneFrame
	errors   []string
	resets   int
}

func (m *mockMonitor) SendEvent(e stream.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

type doneFrame struct {
	id      string
	summary string
}

func (m *mockMonitor) SendBatchQueued(id string, s stream.BatchSummary) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queued = append(m.queued, queuedFrame{id: id, summary: s})
	return nil
}

func (m *mockMonitor) SendBatchRemoved(id, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, removedFrame{id: id, reason: reason})
	return nil
}

func (m *mockMonitor) SendNotice(msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notices = append(m.notices, msg)
	return nil
}

func (m *mockMonitor) SendStatus(_, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses = append(m.statuses, text)
	return nil
}

func (m *mockMonitor) SendDelta(_, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deltas = append(m.deltas, text)
	return nil
}

func (m *mockMonitor) SendDone(id, summary string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dones = append(m.dones, doneFrame{id: id, summary: summary})
	return nil
}

func (m *mockMonitor) SendError(msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, msg)
	return nil
}

func (m *mockMonitor) SendReset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resets++
	return nil
}

func (m *mockMonitor) getQueued() []queuedFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]queuedFrame, len(m.queued))
	copy(out, m.queued)
	return out
}

func (m *mockMonitor) getRemoved() []removedFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]removedFrame, len(m.removed))
	copy(out, m.removed)
	return out
}

func (m *mockMonitor) getNotices() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.notices))
	copy(out, m.notices)
	return out
}

func (m *mockMonitor) getEvents() []stream.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]stream.Event, len(m.events))
	copy(out, m.events)
	return out
}

func (m *mockMonitor) getDones() []doneFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]doneFrame, len(m.dones))
	copy(out, m.dones)
	return out
}

// newTestApp builds an application with a mock agent and monitor.
func newTestApp(t *testing.T, mock agent.Agent, mon MonitorOutput) *application {
	t.Helper()
	logger := log.New(&bytes.Buffer{}, "", 0)
	return &application{
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
}

// waitFor polls fn until it returns true or deadline elapses.
func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// An error event triggers a queued batch after BatchDebounce; the summary
// carries the error line, and no analyzer runs until Decide.
func TestErrorTriggersQueuedBatch(t *testing.T) {
	mock := &mockAgent{result: "diagnosis"}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	s.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "api", Message: "warming up"})
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	if got := mock.getProcessCalls(); got != 0 {
		t.Errorf("processCalls before decide = %d, want 0", got)
	}
	q := mon.getQueued()[0]
	if q.summary.FirstError != "boom" || q.summary.Service != "api" || q.summary.Count != 2 {
		t.Errorf("summary = %+v", q.summary)
	}

	s.Close()
	cancel()
	app.Wait()
}

// Info-only events never produce a batch.
func TestInfoLevelNoBatch(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(4)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "api", Message: "ok"})

	time.Sleep(80 * time.Millisecond)
	if got := len(mon.getQueued()); got != 0 {
		t.Errorf("queued = %d, want 0 for info event", got)
	}

	s.Close()
	cancel()
	app.Wait()
}

// Events keep flowing to the monitor while a batch sits in the queue —
// the old gate would have frozen the stream here.
func TestStreamContinuesWhilePending(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })

	s.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "api", Message: "still alive"})
	waitFor(t, time.Second, func() bool { return len(mon.getEvents()) == 2 })

	s.Close()
	cancel()
	app.Wait()
}

// process runs the analyzer; the batch is auto-removed with reason
// "processed" and SendDone carries the result.
func TestDecideProcessRunsAnalyzer(t *testing.T) {
	mock := &mockAgent{result: "root cause found"}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id

	if err := app.Decide(batchID, stream.DecideProcess); err != nil {
		t.Fatalf("Decide: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		dones := mon.getDones()
		return len(dones) == 1 && dones[0].summary == "root cause found"
	})
	waitFor(t, time.Second, func() bool {
		rm := mon.getRemoved()
		return len(rm) == 1 && rm[0].id == batchID && rm[0].reason == "processed"
	})

	s.Close()
	cancel()
	app.Wait()
}

// clear removes the batch with reason "deleted" and never invokes the agent.
func TestDecideClearDeletesBatch(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id

	if err := app.Decide(batchID, stream.DecideClear); err != nil {
		t.Fatalf("Decide: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		rm := mon.getRemoved()
		return len(rm) == 1 && rm[0].id == batchID && rm[0].reason == "deleted"
	})
	if got := mock.getProcessCalls(); got != 0 {
		t.Errorf("processCalls = %d, want 0 on clear", got)
	}

	s.Close()
	cancel()
	app.Wait()
}

// Decide with an unknown id returns ErrUnknownBatch.
func TestDecideUnknownID(t *testing.T) {
	app := newTestApp(t, &mockAgent{}, &mockMonitor{})
	if err := app.Decide("nonexistent", stream.DecideProcess); err == nil {
		t.Error("Decide on unknown id: want error, got nil")
	}
}

// Two separate error bursts produce two queued batches.
func TestMultipleBatchesQueue(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "first"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "second"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 2 })

	ids := mon.getQueued()
	if ids[0].id == ids[1].id {
		t.Errorf("batch ids should differ, both %q", ids[0].id)
	}

	s.Close()
	cancel()
	app.Wait()
}

// When the queue is full, a new batch is rejected with a notice.
func TestQueueFullRejectsBatch(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon) // queueMax: 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	for i := 0; i < 3; i++ {
		s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: fmt.Sprintf("err-%d", i)})
		want := i + 1
		waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == want })
	}

	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "overflow"})
	waitFor(t, time.Second, func() bool { return len(mon.getNotices()) == 1 })
	if got := len(mon.getQueued()); got != 3 {
		t.Errorf("queued = %d, want 3 (overflow rejected)", got)
	}

	s.Close()
	cancel()
	app.Wait()
}

// A second process decision while an analysis is running is rejected.
func TestProcessWhileRunningRejected(t *testing.T) {
	release := make(chan struct{})
	mock := &mockAgent{result: "done", onProcess: func() { <-release }}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "first"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "second"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 2 })

	first := mon.getQueued()[0].id
	second := mon.getQueued()[1].id

	if err := app.Decide(first, stream.DecideProcess); err != nil {
		t.Fatalf("Decide first: %v", err)
	}
	waitFor(t, time.Second, func() bool { return mock.getProcessCalls() == 1 })

	if err := app.Decide(second, stream.DecideProcess); err == nil {
		t.Error("second process while running: want error, got nil")
	}
	if err := app.Decide(first, stream.DecideClear); err == nil {
		t.Error("clear of running batch: want error, got nil")
	}

	close(release)
	waitFor(t, time.Second, func() bool { return len(mon.getRemoved()) == 1 })

	s.Close()
	cancel()
	app.Wait()
}

// ReplayQueue re-sends every queued batch on monitor reconnect.
func TestReplayQueue(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "first"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "second"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 2 })

	app.ReplayQueue()
	if got := len(mon.getQueued()); got != 4 {
		t.Errorf("queued frames after replay = %d, want 4 (2 original + 2 replayed)", got)
	}

	s.Close()
	cancel()
	app.Wait()
}

// Reset drops queued batches and clears the ring.
func TestResetDropsQueue(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id

	app.Reset()

	if err := app.Decide(batchID, stream.DecideProcess); err == nil {
		t.Error("Decide after Reset: want ErrUnknownBatch, got nil")
	}

	s.Close()
	cancel()
	app.Wait()
}

// NewApplication returns a non-nil Application with defaults.
func TestNewApplicationDefaults(t *testing.T) {
	a := NewApplication(ApplicationConfig{})
	if a == nil {
		t.Fatal("NewApplication returned nil")
	}
}

// Regression: Wait must return after ctx cancel even when an attached
// stream is never closed and no further events arrive. Previously the
// fan-in goroutine stayed parked in its receive forever, so Wait hung and
// main.go never reached srv.Shutdown (stuck server holding the port after
// Ctrl+C).
func TestWaitReturnsAfterCancelWithOpenStream(t *testing.T) {
	mock := &mockAgent{}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "info", Service: "api", Message: "one event"})
	waitFor(t, time.Second, func() bool { return len(mon.getEvents()) == 1 })

	cancel() // shutdown signal; stream stays open, no more traffic

	done := make(chan struct{})
	go func() {
		app.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after ctx cancel with an open, idle stream")
	}
	s.Close()
}

// cancelAgent blocks inside Process until its context is cancelled. Unlike
// ctxAgent it is reusable across calls (channel send, not close), so a test
// can process → cancel → process the same batch again.
type cancelAgent struct {
	started chan struct{}
}

func (m *cancelAgent) Process(ctx context.Context, _ string, _ []stream.Event) (string, error) {
	m.started <- struct{}{}
	<-ctx.Done()
	return "", ctx.Err()
}

// Cancel aborts the running analysis, keeps the batch in the queue for a
// retry, and announces batch_removed(reason:"canceled") so the UI unsticks.
func TestDecideCancelKeepsBatchQueued(t *testing.T) {
	mock := &cancelAgent{started: make(chan struct{})}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})

	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })
	batchID := mon.getQueued()[0].id

	if err := app.Decide(batchID, stream.DecideProcess); err != nil {
		t.Fatalf("Decide(process): %v", err)
	}
	<-mock.started // analysis is now blocked "in the LLM call"

	if err := app.Decide(batchID, stream.DecideCancel); err != nil {
		t.Fatalf("Decide(cancel): %v", err)
	}

	waitFor(t, time.Second, func() bool {
		rm := mon.getRemoved()
		return len(rm) == 1 && rm[0].id == batchID && rm[0].reason == "canceled"
	})

	app.mu.Lock()
	stillQueued := app.findLocked(batchID) != nil
	idle := app.runningID == ""
	app.mu.Unlock()
	if !stillQueued {
		t.Error("batch removed from queue after cancel, want it kept")
	}
	if !idle {
		t.Error("runningID not cleared after cancel")
	}

	// The kept batch can be processed again.
	if err := app.Decide(batchID, stream.DecideProcess); err != nil {
		t.Fatalf("Decide(process) after cancel: %v", err)
	}
	<-mock.started

	s.Close()
	cancel()
	app.Wait()
}

// Cancel is rejected when the target batch is not being analyzed.
func TestDecideCancelNotRunning(t *testing.T) {
	app := newTestApp(t, &mockAgent{}, &mockMonitor{})
	if err := app.Decide("nonexistent", stream.DecideCancel); err == nil {
		t.Error("Decide(cancel) with nothing running: want error, got nil")
	}
}

// ctxAgent blocks inside Process until its context is cancelled — a stand-in
// for a long LLM analysis. Used to verify shutdown aborts in-flight work.
type ctxAgent struct {
	mu      sync.Mutex
	started chan struct{}
	err     error
}

func (m *ctxAgent) Process(ctx context.Context, _ string, _ []stream.Event) (string, error) {
	close(m.started)
	<-ctx.Done()
	m.mu.Lock()
	m.err = ctx.Err()
	m.mu.Unlock()
	return "", ctx.Err()
}

// Regression: Ctrl+C (ctx cancel) during a running analysis must abort the
// analysis and let Wait return promptly. Previously runAnalysis derived its
// context from context.Background(), so Wait blocked on wg.Wait until the
// analysis finished or hit the 15-minute analysisTimeout — a stuck server
// holding the port after Ctrl+C.
func TestWaitAbortsInFlightAnalysisOnCancel(t *testing.T) {
	mock := &ctxAgent{started: make(chan struct{})}
	mon := &mockMonitor{}
	app := newTestApp(t, mock, mon)

	ctx, cancel := context.WithCancel(context.Background())
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)
	s.Write(stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"})
	waitFor(t, time.Second, func() bool { return len(mon.getQueued()) == 1 })

	if err := app.Decide(mon.getQueued()[0].id, stream.DecideProcess); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	<-mock.started // analysis is now blocked "in the LLM call"

	cancel() // shutdown signal mid-analysis

	done := make(chan struct{})
	go func() {
		app.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return: in-flight analysis was not aborted by ctx cancel")
	}
	s.Close()
}
