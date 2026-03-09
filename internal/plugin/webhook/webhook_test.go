package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/store"
)

// testPayload creates a sample PluginPayload for webhook tests.
func testPayload() plugin.PluginPayload {
	return plugin.PluginPayload{
		Type:     plugin.AutoAnalysis,
		Analysis: "root cause: timeout",
		LogEntries: []store.LogEntry{
			{Timestamp: time.Now(), Source: "api", Level: "error", Message: "boom", Service: "svc"},
		},
		Timestamp: time.Now(),
	}
}

func TestNewWebhookPlugin(t *testing.T) {
	tests := []struct {
		name    string
		pName   string
		config  map[string]string
		wantErr bool
	}{
		{
			// Valid config with url should succeed.
			name:    "valid config",
			pName:   "my-hook",
			config:  map[string]string{"url": "https://example.com/hook"},
			wantErr: false,
		},
		{
			// Missing url key should return an error.
			name:    "missing url",
			pName:   "bad-hook",
			config:  map[string]string{},
			wantErr: true,
		},
		{
			// Empty url value should return an error.
			name:    "empty url",
			pName:   "bad-hook",
			config:  map[string]string{"url": ""},
			wantErr: true,
		},
		{
			// Nil config map should return an error.
			name:    "nil config",
			pName:   "bad-hook",
			config:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewWebhookPlugin(tt.pName, tt.config)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Name() != tt.pName {
				t.Errorf("Name() = %q, want %q", p.Name(), tt.pName)
			}
		})
	}
}

// WebhookPlugin must satisfy the plugin.Plugin interface at compile time.
var _ plugin.Plugin = (*WebhookPlugin)(nil)

func TestWebhookPluginSend(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErr    bool
		checkBody  bool
	}{
		{
			// Successful POST to a 200 server should return nil error.
			name: "success 200",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method = %q, want POST", r.Method)
				}
				if ct := r.Header.Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
				body, _ := io.ReadAll(r.Body)
				var payload plugin.PluginPayload
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Errorf("failed to unmarshal body: %v", err)
				}
				if payload.Analysis != "root cause: timeout" {
					t.Errorf("payload.Analysis = %q, want %q", payload.Analysis, "root cause: timeout")
				}
				w.WriteHeader(http.StatusOK)
			}),
			wantErr:   false,
			checkBody: true,
		},
		{
			// Non-2xx response should return an error.
			name: "non-2xx returns error",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			p, err := NewWebhookPlugin("test", map[string]string{"url": server.URL})
			if err != nil {
				t.Fatalf("NewWebhookPlugin failed: %v", err)
			}

			err = p.Send(context.Background(), testPayload())
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// Send to an unreachable URL should return an error.
func TestWebhookPluginSendUnreachable(t *testing.T) {
	p, err := NewWebhookPlugin("test", map[string]string{"url": "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewWebhookPlugin failed: %v", err)
	}

	err = p.Send(context.Background(), testPayload())
	if err == nil {
		t.Fatal("expected error for unreachable URL, got nil")
	}
}

// Start should be a no-op and return nil for v1.
func TestWebhookPluginStart(t *testing.T) {
	p, err := NewWebhookPlugin("test", map[string]string{"url": "https://example.com"})
	if err != nil {
		t.Fatalf("NewWebhookPlugin failed: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Errorf("Start() = %v, want nil", err)
	}
}

// Shutdown should be a no-op and return nil for v1.
func TestWebhookPluginShutdown(t *testing.T) {
	p, err := NewWebhookPlugin("test", map[string]string{"url": "https://example.com"})
	if err != nil {
		t.Fatalf("NewWebhookPlugin failed: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() = %v, want nil", err)
	}
}
