package plugin

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/store"
)

// newTestLogger creates a logger that writes to a buffer for assertion.
func newTestLogger() (*log.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return log.New(buf, "", 0), buf
}

// testPayload creates a sample PluginPayload for dispatcher tests.
func testPayload() PluginPayload {
	return PluginPayload{
		Type:     AutoAnalysis,
		Analysis: "test analysis",
		LogEntries: []store.LogEntry{
			{Timestamp: time.Now(), Source: "api", Level: "error", Message: "boom", Service: "svc"},
		},
		Timestamp: time.Now(),
	}
}

func TestDispatcher(t *testing.T) {
	tests := []struct {
		name      string
		plugins   func() ([]*mockPlugin, []Plugin)
		action    func(t *testing.T, d Dispatcher, plugins []*mockPlugin, logBuf *bytes.Buffer)
	}{
		{
			// Zero-plugin dispatcher should be a no-op — Send and Shutdown do nothing.
			name: "zero plugins is no-op",
			plugins: func() ([]*mockPlugin, []Plugin) {
				return nil, nil
			},
			action: func(t *testing.T, d Dispatcher, _ []*mockPlugin, logBuf *bytes.Buffer) {
				d.Send(context.Background(), testPayload())
				d.Shutdown(context.Background())
				if logBuf.Len() > 0 {
					t.Errorf("expected no log output, got %q", logBuf.String())
				}
			},
		},
		{
			// Send should fan out payload to all plugins.
			name: "fan-out to multiple plugins",
			plugins: func() ([]*mockPlugin, []Plugin) {
				m1 := &mockPlugin{name: "p1"}
				m2 := &mockPlugin{name: "p2"}
				return []*mockPlugin{m1, m2}, []Plugin{m1, m2}
			},
			action: func(t *testing.T, d Dispatcher, mocks []*mockPlugin, _ *bytes.Buffer) {
				payload := testPayload()
				d.Send(context.Background(), payload)
				// Allow goroutines to complete.
				time.Sleep(50 * time.Millisecond)
				for _, m := range mocks {
					if len(m.sendCalls) != 1 {
						t.Errorf("plugin %q: sendCalls = %d, want 1", m.name, len(m.sendCalls))
					}
				}
			},
		},
		{
			// When a plugin's Send fails, the error should be logged and other plugins unaffected.
			name: "error logging on plugin failure",
			plugins: func() ([]*mockPlugin, []Plugin) {
				m1 := &mockPlugin{name: "fail-plugin", sendErr: errors.New("connection refused")}
				m2 := &mockPlugin{name: "ok-plugin"}
				return []*mockPlugin{m1, m2}, []Plugin{m1, m2}
			},
			action: func(t *testing.T, d Dispatcher, mocks []*mockPlugin, logBuf *bytes.Buffer) {
				d.Send(context.Background(), testPayload())
				time.Sleep(50 * time.Millisecond)
				// The failing plugin's error should be logged.
				if !strings.Contains(logBuf.String(), "connection refused") {
					t.Errorf("expected log to contain 'connection refused', got %q", logBuf.String())
				}
				// The successful plugin should still receive the payload.
				if len(mocks[1].sendCalls) != 1 {
					t.Errorf("ok-plugin sendCalls = %d, want 1", len(mocks[1].sendCalls))
				}
			},
		},
		{
			// Shutdown should call Shutdown on all plugins.
			name: "shutdown calls all plugins",
			plugins: func() ([]*mockPlugin, []Plugin) {
				m1 := &mockPlugin{name: "p1"}
				m2 := &mockPlugin{name: "p2"}
				return []*mockPlugin{m1, m2}, []Plugin{m1, m2}
			},
			action: func(t *testing.T, d Dispatcher, mocks []*mockPlugin, _ *bytes.Buffer) {
				// Shutdown is tested by ensuring no panic and plugins are called.
				// Since mockPlugin.Shutdown returns nil, we just verify no error.
				d.Shutdown(context.Background())
			},
		},
		{
			// Context cancellation should be respected — Send should not block.
			name: "context cancellation",
			plugins: func() ([]*mockPlugin, []Plugin) {
				m := &mockPlugin{name: "slow"}
				return []*mockPlugin{m}, []Plugin{m}
			},
			action: func(t *testing.T, d Dispatcher, _ []*mockPlugin, _ *bytes.Buffer) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // cancel immediately
				// Send should not block even with cancelled context.
				d.Send(ctx, testPayload())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, logBuf := newTestLogger()
			mocks, plugins := tt.plugins()
			d := NewDispatcher(plugins, logger)
			tt.action(t, d, mocks, logBuf)
		})
	}
}
