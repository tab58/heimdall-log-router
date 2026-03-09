package app

import (
	"bytes"
	"context"
	"log"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/store"
)

// TestIntegrationPluginFlow verifies the full flow: ingest error log -> auto-analysis -> plugin receives payload.
func TestIntegrationPluginFlow(t *testing.T) {
	// Mock analyzer that returns a predictable analysis.
	mock := &mockAnalyzer{analyzeResult: "integration test: root cause identified"}

	// Mock plugin to capture dispatched payloads.
	mp := &mockPlugin{name: "integration-hook"}

	// Build a real dispatcher with the mock plugin.
	logger := log.New(&bytes.Buffer{}, "", 0)
	d := plugin.NewDispatcher([]plugin.Plugin{mp}, logger)

	// Build app with mock analyzer and real dispatcher.
	s := store.New("")
	app := &application{
		store:      s,
		analyzer:   mock,
		debounce:   debouncer{cooldown: 0},
		dispatcher: &d,
		logger:     logger,
	}

	// Ingest some context logs first.
	for i := 0; i < 5; i++ {
		_ = app.HandleVectorIngest(context.Background(), &VectorIngestLogEntry{
			Timestamp: time.Now(),
			Source:    "api",
			Level:     "info",
			Message:   "normal log entry",
			Service:   "svc",
		})
	}

	// Ingest an error log — should trigger auto-analysis + dispatch.
	err := app.HandleVectorIngest(context.Background(), &VectorIngestLogEntry{
		Timestamp: time.Now(),
		Source:    "api",
		Level:     "error",
		Message:   "database connection failed",
		Service:   "svc",
	})
	if err != nil {
		t.Fatalf("HandleVectorIngest error: %v", err)
	}

	// Wait for async analysis.
	app.Wait()

	// Verify the mock analyzer was called.
	if got := mock.getAnalyzeCalls(); got != 1 {
		t.Errorf("analyzeCalls = %d, want 1", got)
	}

	// Verify the mock plugin received the payload.
	sends := mp.getSendCalls()
	if len(sends) != 1 {
		t.Fatalf("plugin sendCalls = %d, want 1", len(sends))
	}

	payload := sends[0]
	if payload.Type != plugin.AutoAnalysis {
		t.Errorf("payload.Type = %q, want %q", payload.Type, plugin.AutoAnalysis)
	}
	if payload.Analysis != "integration test: root cause identified" {
		t.Errorf("payload.Analysis = %q, want %q", payload.Analysis, "integration test: root cause identified")
	}
	if len(payload.LogEntries) == 0 {
		t.Error("payload.LogEntries should not be empty")
	}
}

// TestIntegrationAskFlow verifies: ask question -> AI response -> plugin receives ask_response payload.
func TestIntegrationAskFlow(t *testing.T) {
	mock := &mockAnalyzer{askResult: "the service crashed due to OOM"}
	mp := &mockPlugin{name: "ask-hook"}

	logger := log.New(&bytes.Buffer{}, "", 0)
	d := plugin.NewDispatcher([]plugin.Plugin{mp}, logger)

	s := store.New("")
	// Add some logs for context.
	s.Append(store.LogEntry{
		Timestamp: time.Now(),
		Source:    "api",
		Level:     "error",
		Message:   "out of memory",
		Service:   "svc",
	})

	app := &application{
		store:      s,
		analyzer:   mock,
		debounce:   debouncer{cooldown: 0},
		dispatcher: &d,
		logger:     logger,
	}

	result, err := app.HandleAsk(context.Background(), "why did it crash?", 10)
	if err != nil {
		t.Fatalf("HandleAsk error: %v", err)
	}
	if result != "the service crashed due to OOM" {
		t.Errorf("result = %q, want %q", result, "the service crashed due to OOM")
	}

	// Verify plugin received ask_response payload.
	sends := mp.getSendCalls()
	if len(sends) != 1 {
		t.Fatalf("plugin sendCalls = %d, want 1", len(sends))
	}
	if sends[0].Type != plugin.AskResponse {
		t.Errorf("payload.Type = %q, want %q", sends[0].Type, plugin.AskResponse)
	}
	if sends[0].Analysis != "the service crashed due to OOM" {
		t.Errorf("payload.Analysis = %q, want %q", sends[0].Analysis, "the service crashed due to OOM")
	}
}
