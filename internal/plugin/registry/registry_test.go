package registry

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/tbright/heimdall/internal/config"
)

// newTestLogger creates a logger that writes to a buffer for assertion.
func newTestLogger() (*log.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return log.New(buf, "", 0), buf
}

func TestBuildPlugins(t *testing.T) {
	tests := []struct {
		name            string
		defs            []config.PluginDef
		wantCount       int
		wantLogContains string
	}{
		{
			// Empty definitions should return zero plugins.
			name:      "empty defs",
			defs:      nil,
			wantCount: 0,
		},
		{
			// Valid webhook definition should produce one plugin.
			name: "valid webhook",
			defs: []config.PluginDef{
				{Name: "hook1", Type: "webhook", Config: map[string]string{"url": "https://example.com"}},
			},
			wantCount: 1,
		},
		{
			// Unknown plugin type should be skipped and logged.
			name: "unknown type skipped",
			defs: []config.PluginDef{
				{Name: "bad", Type: "slack", Config: map[string]string{}},
			},
			wantCount:       0,
			wantLogContains: "unknown plugin type",
		},
		{
			// Webhook with missing url should fail validation and be skipped.
			name: "webhook missing url skipped",
			defs: []config.PluginDef{
				{Name: "bad-hook", Type: "webhook", Config: map[string]string{}},
			},
			wantCount:       0,
			wantLogContains: "bad-hook",
		},
		{
			// Mix of valid and invalid should only return valid plugins.
			name: "mixed valid and invalid",
			defs: []config.PluginDef{
				{Name: "good", Type: "webhook", Config: map[string]string{"url": "https://example.com"}},
				{Name: "bad", Type: "unknown", Config: nil},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, logBuf := newTestLogger()
			plugins := BuildPlugins(tt.defs, logger)
			if len(plugins) != tt.wantCount {
				t.Errorf("plugin count = %d, want %d", len(plugins), tt.wantCount)
			}
			if tt.wantLogContains != "" {
				if !strings.Contains(logBuf.String(), tt.wantLogContains) {
					t.Errorf("log output %q should contain %q", logBuf.String(), tt.wantLogContains)
				}
			}
		})
	}
}
