package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/store"
)

// mockPlugin is a test double that records calls and can return configured errors.
type mockPlugin struct {
	name        string
	startErr    error
	sendErr     error
	shutdownErr error
	sendCalls   []PluginPayload
}

func (m *mockPlugin) Name() string { return m.name }
func (m *mockPlugin) Start() error { return m.startErr }
func (m *mockPlugin) Send(_ context.Context, payload PluginPayload) error {
	m.sendCalls = append(m.sendCalls, payload)
	return m.sendErr
}
func (m *mockPlugin) Shutdown(_ context.Context) error { return m.shutdownErr }

// mockPlugin must satisfy the Plugin interface at compile time.
var _ Plugin = (*mockPlugin)(nil)

// AnalysisType constants should have the correct string values.
func TestAnalysisTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		got      AnalysisType
		expected string
	}{
		{"AutoAnalysis value", AutoAnalysis, "auto_analysis"},
		{"AskResponse value", AskResponse, "ask_response"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.got) != tt.expected {
				t.Errorf("got %q, want %q", tt.got, tt.expected)
			}
		})
	}
}

// PluginPayload should hold all required fields.
func TestPluginPayload(t *testing.T) {
	now := time.Now()
	entries := []store.LogEntry{
		{Timestamp: now, Source: "api", Level: "error", Message: "fail", Service: "svc"},
	}
	payload := PluginPayload{
		Type:       AutoAnalysis,
		Analysis:   "root cause: timeout",
		LogEntries: entries,
		Timestamp:  now,
	}

	if payload.Type != AutoAnalysis {
		t.Errorf("Type = %q, want %q", payload.Type, AutoAnalysis)
	}
	if payload.Analysis != "root cause: timeout" {
		t.Errorf("Analysis = %q, want %q", payload.Analysis, "root cause: timeout")
	}
	if len(payload.LogEntries) != 1 {
		t.Fatalf("LogEntries len = %d, want 1", len(payload.LogEntries))
	}
	if payload.LogEntries[0].Message != "fail" {
		t.Errorf("LogEntries[0].Message = %q, want %q", payload.LogEntries[0].Message, "fail")
	}
	if !payload.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, want %v", payload.Timestamp, now)
	}
}

// A mock should satisfy the Plugin interface and record Send calls.
func TestMockPluginSatisfiesInterface(t *testing.T) {
	m := &mockPlugin{name: "test-plugin"}
	var p Plugin = m

	if p.Name() != "test-plugin" {
		t.Errorf("Name() = %q, want %q", p.Name(), "test-plugin")
	}
	if err := p.Start(); err != nil {
		t.Errorf("Start() = %v, want nil", err)
	}

	payload := PluginPayload{Type: AskResponse, Analysis: "answer", Timestamp: time.Now()}
	if err := p.Send(context.Background(), payload); err != nil {
		t.Errorf("Send() = %v, want nil", err)
	}
	if len(m.sendCalls) != 1 {
		t.Fatalf("sendCalls len = %d, want 1", len(m.sendCalls))
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() = %v, want nil", err)
	}
}
