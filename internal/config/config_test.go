package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeYAML is a test helper that writes content to a temp YAML file and returns its path.
func writeYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "heimdall.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test YAML: %v", err)
	}
	return path
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) string // returns path to YAML file
		envKey     string                     // value to set for ANTHROPIC_API_KEY env var (empty = unset)
		wantErr    bool
		check      func(t *testing.T, cfg Config)
	}{
		{
			// Valid YAML with all fields set should parse correctly.
			name: "valid YAML with all fields",
			setup: func(t *testing.T) string {
				return writeYAML(t, `
api_key: "sk-test-key"
server_port: ":8080"
debounce_time: 10s
plugin_log_dir: "/var/log/heimdall/"
plugins:
  - name: "my-webhook"
    type: "webhook"
    config:
      url: "https://example.com/hook"
`)
			},
			check: func(t *testing.T, cfg Config) {
				if cfg.APIKey != "sk-test-key" {
					t.Errorf("APIKey = %q, want %q", cfg.APIKey, "sk-test-key")
				}
				if cfg.ServerPort != ":8080" {
					t.Errorf("ServerPort = %q, want %q", cfg.ServerPort, ":8080")
				}
				if cfg.DebounceTime != 10*time.Second {
					t.Errorf("DebounceTime = %v, want %v", cfg.DebounceTime, 10*time.Second)
				}
				if cfg.PluginLogDir != "/var/log/heimdall/" {
					t.Errorf("PluginLogDir = %q, want %q", cfg.PluginLogDir, "/var/log/heimdall/")
				}
				if len(cfg.Plugins) != 1 {
					t.Fatalf("len(Plugins) = %d, want 1", len(cfg.Plugins))
				}
				p := cfg.Plugins[0]
				if p.Name != "my-webhook" {
					t.Errorf("Plugin.Name = %q, want %q", p.Name, "my-webhook")
				}
				if p.Type != "webhook" {
					t.Errorf("Plugin.Type = %q, want %q", p.Type, "webhook")
				}
				if p.Config["url"] != "https://example.com/hook" {
					t.Errorf("Plugin.Config[url] = %q, want %q", p.Config["url"], "https://example.com/hook")
				}
			},
		},
		{
			// Missing file should return default config with no error.
			name: "missing file returns defaults",
			setup: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "nonexistent.yaml")
			},
			check: func(t *testing.T, cfg Config) {
				if cfg.ServerPort != ":7077" {
					t.Errorf("default ServerPort = %q, want %q", cfg.ServerPort, ":7077")
				}
				if cfg.DebounceTime != 5*time.Second {
					t.Errorf("default DebounceTime = %v, want %v", cfg.DebounceTime, 5*time.Second)
				}
				if cfg.PluginLogDir != "/tmp/heimdall/logs/" {
					t.Errorf("default PluginLogDir = %q, want %q", cfg.PluginLogDir, "/tmp/heimdall/logs/")
				}
				if len(cfg.Plugins) != 0 {
					t.Errorf("default Plugins should be empty, got %d", len(cfg.Plugins))
				}
			},
		},
		{
			// YAML with missing optional fields should fill in defaults.
			name: "partial YAML applies defaults",
			setup: func(t *testing.T) string {
				return writeYAML(t, `api_key: "sk-partial"`)
			},
			check: func(t *testing.T, cfg Config) {
				if cfg.APIKey != "sk-partial" {
					t.Errorf("APIKey = %q, want %q", cfg.APIKey, "sk-partial")
				}
				if cfg.ServerPort != ":7077" {
					t.Errorf("default ServerPort = %q, want %q", cfg.ServerPort, ":7077")
				}
				if cfg.DebounceTime != 5*time.Second {
					t.Errorf("default DebounceTime = %v, want %v", cfg.DebounceTime, 5*time.Second)
				}
				if cfg.PluginLogDir != "/tmp/heimdall/logs/" {
					t.Errorf("default PluginLogDir = %q, want %q", cfg.PluginLogDir, "/tmp/heimdall/logs/")
				}
			},
		},
		{
			// Empty api_key in YAML should fall back to ANTHROPIC_API_KEY env var.
			name: "api_key falls back to env var",
			setup: func(t *testing.T) string {
				return writeYAML(t, `server_port: ":9090"`)
			},
			envKey: "sk-from-env",
			check: func(t *testing.T, cfg Config) {
				if cfg.APIKey != "sk-from-env" {
					t.Errorf("APIKey = %q, want %q (from env)", cfg.APIKey, "sk-from-env")
				}
			},
		},
		{
			// Malformed YAML should return a parse error.
			name: "malformed YAML returns error",
			setup: func(t *testing.T) string {
				return writeYAML(t, `{{{not valid yaml`)
			},
			wantErr: true,
		},
		{
			// YAML with empty plugins list should return config with no plugins.
			name: "empty plugins list",
			setup: func(t *testing.T) string {
				return writeYAML(t, `
api_key: "sk-test"
plugins: []
`)
			},
			check: func(t *testing.T, cfg Config) {
				if len(cfg.Plugins) != 0 {
					t.Errorf("Plugins should be empty, got %d", len(cfg.Plugins))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set or unset ANTHROPIC_API_KEY env var for this test case.
			if tt.envKey != "" {
				t.Setenv("ANTHROPIC_API_KEY", tt.envKey)
			} else {
				t.Setenv("ANTHROPIC_API_KEY", "")
			}

			path := tt.setup(t)
			cfg, err := Load(path)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}
