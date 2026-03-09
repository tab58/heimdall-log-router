package app

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/store"
)

// mockAnalyzer is a test double for the analyzer interface.
type mockAnalyzer struct {
	mu            sync.Mutex
	analyzeResult string
	analyzeErr    error
	askResult     string
	askErr        error
	analyzeCalls  int
	askCalls      int
}

func (m *mockAnalyzer) AnalyzeError(_ context.Context, _ store.LogEntry, _ []store.LogEntry) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.analyzeCalls++
	return m.analyzeResult, m.analyzeErr
}

func (m *mockAnalyzer) Ask(_ context.Context, _ string, _ []store.LogEntry) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.askCalls++
	return m.askResult, m.askErr
}

func (m *mockAnalyzer) getAnalyzeCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.analyzeCalls
}

func (m *mockAnalyzer) getAskCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.askCalls
}

// mockPlugin records Send calls for dispatcher assertions.
type mockPlugin struct {
	mu        sync.Mutex
	name      string
	sendCalls []plugin.PluginPayload
}

func (m *mockPlugin) Name() string { return m.name }
func (m *mockPlugin) Start() error { return nil }
func (m *mockPlugin) Send(_ context.Context, payload plugin.PluginPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendCalls = append(m.sendCalls, payload)
	return nil
}
func (m *mockPlugin) Shutdown(_ context.Context) error { return nil }

func (m *mockPlugin) getSendCalls() []plugin.PluginPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]plugin.PluginPayload, len(m.sendCalls))
	copy(copied, m.sendCalls)
	return copied
}

// newTestApp creates an application with mock analyzer and a dispatcher with the given mock plugins.
func newTestApp(t *testing.T, mock *mockAnalyzer, plugins ...plugin.Plugin) (*application, []*mockPlugin) {
	t.Helper()
	logger := log.New(&bytes.Buffer{}, "", 0)
	d := plugin.NewDispatcher(plugins, logger)
	return &application{
		store:      store.New(""),
		analyzer:   mock,
		debounce:   debouncer{cooldown: 0}, // no cooldown for tests
		dispatcher: &d,
		logger:     logger,
	}, nil
}

func TestNewApplicationWithDispatcher(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ApplicationConfig
		wantErr bool
	}{
		{
			// Nil dispatcher should be accepted (backwards compatible).
			name: "nil dispatcher is valid",
			cfg: ApplicationConfig{
				Store:        store.New("/tmp/test.jsonl"),
				LlmApiKey:    "sk-test",
				DebounceTime: 5 * time.Second,
			},
			wantErr: false,
		},
		{
			// Dispatcher provided should be accepted.
			name: "with dispatcher",
			cfg: ApplicationConfig{
				Store:        store.New("/tmp/test.jsonl"),
				LlmApiKey:    "sk-test",
				DebounceTime: 5 * time.Second,
				Dispatcher:   &plugin.Dispatcher{},
			},
			wantErr: false,
		},
		{
			// Missing store should still return error.
			name: "missing store with dispatcher",
			cfg: ApplicationConfig{
				LlmApiKey:  "sk-test",
				Dispatcher: &plugin.Dispatcher{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewApplication(tt.cfg)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// HandleVectorIngest should trigger auto-analysis for error logs and dispatch the result.
func TestHandleVectorIngestAutoAnalysisDispatch(t *testing.T) {
	mock := &mockAnalyzer{analyzeResult: "root cause: timeout"}
	mp := &mockPlugin{name: "test"}
	app, _ := newTestApp(t, mock, mp)

	entry := &VectorIngestLogEntry{
		Timestamp: time.Now(),
		Source:    "api",
		Level:     "error",
		Message:   "connection timeout",
		Service:   "svc",
	}

	err := app.HandleVectorIngest(context.Background(), entry)
	if err != nil {
		t.Fatalf("HandleVectorIngest error: %v", err)
	}

	// Wait for async analysis goroutine.
	app.Wait()

	if got := mock.getAnalyzeCalls(); got != 1 {
		t.Errorf("analyzeCalls = %d, want 1", got)
	}
	sends := mp.getSendCalls()
	if len(sends) != 1 {
		t.Fatalf("plugin sendCalls = %d, want 1", len(sends))
	}
	if sends[0].Type != plugin.AutoAnalysis {
		t.Errorf("payload type = %q, want %q", sends[0].Type, plugin.AutoAnalysis)
	}
	if sends[0].Analysis != "root cause: timeout" {
		t.Errorf("payload analysis = %q, want %q", sends[0].Analysis, "root cause: timeout")
	}
}

// HandleVectorIngest with info level should NOT trigger auto-analysis.
func TestHandleVectorIngestInfoLevelNoAnalysis(t *testing.T) {
	mock := &mockAnalyzer{analyzeResult: "should not happen"}
	mp := &mockPlugin{name: "test"}
	app, _ := newTestApp(t, mock, mp)

	entry := &VectorIngestLogEntry{
		Timestamp: time.Now(),
		Source:    "api",
		Level:     "info",
		Message:   "all good",
		Service:   "svc",
	}

	err := app.HandleVectorIngest(context.Background(), entry)
	if err != nil {
		t.Fatalf("HandleVectorIngest error: %v", err)
	}

	// No async goroutine expected for info level, but Wait() to be safe.
	app.Wait()

	if got := mock.getAnalyzeCalls(); got != 0 {
		t.Errorf("analyzeCalls = %d, want 0 for info level", got)
	}
	if got := len(mp.getSendCalls()); got != 0 {
		t.Errorf("plugin sendCalls = %d, want 0 for info level", got)
	}
}

// HandleAsk should dispatch ask_response payload after AI responds.
func TestHandleAskDispatch(t *testing.T) {
	mock := &mockAnalyzer{askResult: "the answer is 42"}
	mp := &mockPlugin{name: "test"}
	app, _ := newTestApp(t, mock, mp)

	result, err := app.HandleAsk(context.Background(), "what happened?", 10)
	if err != nil {
		t.Fatalf("HandleAsk error: %v", err)
	}
	if result != "the answer is 42" {
		t.Errorf("result = %q, want %q", result, "the answer is 42")
	}

	sends := mp.getSendCalls()
	if len(sends) != 1 {
		t.Fatalf("plugin sendCalls = %d, want 1", len(sends))
	}
	if sends[0].Type != plugin.AskResponse {
		t.Errorf("payload type = %q, want %q", sends[0].Type, plugin.AskResponse)
	}
	if sends[0].Analysis != "the answer is 42" {
		t.Errorf("payload analysis = %q, want %q", sends[0].Analysis, "the answer is 42")
	}
}

// HandleVectorIngest with nil dispatcher should still work (no-op dispatch).
func TestHandleVectorIngestNilDispatcher(t *testing.T) {
	mock := &mockAnalyzer{analyzeResult: "analysis"}
	app := &application{
		store:      store.New(""),
		analyzer:   mock,
		debounce:   debouncer{cooldown: 0},
		dispatcher: nil,
		logger:     log.New(&bytes.Buffer{}, "", 0),
	}

	entry := &VectorIngestLogEntry{
		Timestamp: time.Now(),
		Source:    "api",
		Level:     "error",
		Message:   "boom",
		Service:   "svc",
	}

	err := app.HandleVectorIngest(context.Background(), entry)
	if err != nil {
		t.Fatalf("HandleVectorIngest with nil dispatcher error: %v", err)
	}

	app.Wait()
	// Should not panic with nil dispatcher.
}
