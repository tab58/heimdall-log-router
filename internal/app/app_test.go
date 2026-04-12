package app

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/tab58/heimdall-log-router/internal/app/store"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// mockAgent is a test double for the agent.Agent interface.
type mockAgent struct {
	mu           sync.Mutex
	result       string
	err          error
	processCalls int
}

func (m *mockAgent) Process(_ context.Context, _ []stream.Event) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processCalls++
	return m.result, m.err
}

func (m *mockAgent) getProcessCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processCalls
}

// mockOutput captures AnalysisResults written to the application output sink.
type mockOutput struct {
	mu      sync.Mutex
	writes  []stream.AnalysisResult
	writeFn func(stream.AnalysisResult) error
}

func (m *mockOutput) Write(r stream.AnalysisResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes = append(m.writes, r)
	if m.writeFn != nil {
		return m.writeFn(r)
	}
	return nil
}

func (m *mockOutput) Close() error { return nil }

func (m *mockOutput) getWrites() []stream.AnalysisResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]stream.AnalysisResult, len(m.writes))
	copy(copied, m.writes)
	return copied
}

// newTestApp builds an application with a mock agent and optional output sink.
func newTestApp(t *testing.T, mock *mockAgent, out stream.WriteStream) *application {
	t.Helper()
	logger := log.New(&bytes.Buffer{}, "", 0)
	return &application{
		agent:    mock,
		store:    store.New(),
		debounce: debouncer{cooldown: 0}, // no cooldown for tests
		events:   make(chan stream.Event, 64),
		logger:   logger,
		loopDone: make(chan struct{}),
		output:   out,
	}
}

// AddStream + Start: error events should trigger agent processing and output write.
func TestAddStreamStartAutoAnalysis(t *testing.T) {
	mock := &mockAgent{result: "root cause: timeout"}
	out := &mockOutput{}
	app := newTestApp(t, mock, out)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	s.Write(stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "connection timeout",
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
	if writes[0].Result != "root cause: timeout" {
		t.Errorf("result = %q, want %q", writes[0].Result, "root cause: timeout")
	}
	if writes[0].ID == "" {
		t.Error("result ID should not be empty")
	}
}

// Info-level events should NOT trigger agent processing.
func TestInfoLevelNoAnalysis(t *testing.T) {
	mock := &mockAgent{result: "should not happen"}
	out := &mockOutput{}
	app := newTestApp(t, mock, out)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.Start(ctx)

	s := stream.NewHTTPIngestStream(8)
	app.AddStream(s)

	s.Write(stream.Event{
		Timestamp: time.Now(),
		Severity:  "info",
		Service:   "api",
		Message:   "all good",
	})
	s.Close()

	app.Wait()

	if got := mock.getProcessCalls(); got != 0 {
		t.Errorf("processCalls = %d, want 0 for info level", got)
	}
	if got := len(out.getWrites()); got != 0 {
		t.Errorf("output writes = %d, want 0 for info level", got)
	}
}

// NewApplication should use defaults when optional fields are zero.
func TestNewApplicationDefaults(t *testing.T) {
	a := NewApplication(ApplicationConfig{})
	if a == nil {
		t.Fatal("NewApplication returned nil")
	}
}
