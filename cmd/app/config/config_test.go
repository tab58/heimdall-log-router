package config

import (
	"strings"
	"testing"
	"time"
)

func TestApplyWebSocketDefaultsFillsZeros(t *testing.T) {
	in := []WebSocketSource{{Name: "a", URL: "wss://x/y"}}
	out := applyWebSocketDefaults(in)

	if len(out) != 1 {
		t.Fatalf("len=%d, want 1", len(out))
	}
	got := out[0]
	if got.BufferSize != DefaultWSBufferSize {
		t.Errorf("BufferSize = %d, want %d", got.BufferSize, DefaultWSBufferSize)
	}
	if got.HandshakeTimeout != DefaultWSHandshakeTimeout {
		t.Errorf("HandshakeTimeout = %s, want %s", got.HandshakeTimeout, DefaultWSHandshakeTimeout)
	}
	if got.ReconnectMin != DefaultWSReconnectMin {
		t.Errorf("ReconnectMin = %s", got.ReconnectMin)
	}
	if got.ReconnectMax != DefaultWSReconnectMax {
		t.Errorf("ReconnectMax = %s", got.ReconnectMax)
	}
	if got.PingInterval != DefaultWSPingInterval {
		t.Errorf("PingInterval = %s", got.PingInterval)
	}
	// Original slice must not be mutated (immutability rule).
	if in[0].BufferSize != 0 {
		t.Errorf("input mutated: BufferSize=%d", in[0].BufferSize)
	}
}

func TestApplyWebSocketDefaultsRespectsExplicitValues(t *testing.T) {
	in := []WebSocketSource{{
		Name:             "explicit",
		URL:              "wss://x/y",
		BufferSize:       17,
		HandshakeTimeout: 2 * time.Second,
		ReconnectMin:     500 * time.Millisecond,
		ReconnectMax:     2 * time.Second,
		PingInterval:     5 * time.Second,
	}}
	out := applyWebSocketDefaults(in)
	got := out[0]
	if got.BufferSize != 17 {
		t.Errorf("BufferSize = %d, want 17", got.BufferSize)
	}
	if got.PingInterval != 5*time.Second {
		t.Errorf("PingInterval = %s, want 5s", got.PingInterval)
	}
}

func TestApplyWebSocketDefaultsExpandsEnvInHeaders(t *testing.T) {
	t.Setenv("UPSTREAM_TOKEN", "secret-abc")

	in := []WebSocketSource{{
		Name: "a",
		URL:  "wss://x/y",
		Headers: map[string]string{
			"Authorization": "Bearer ${UPSTREAM_TOKEN}",
			"X-Static":      "plain",
		},
	}}
	out := applyWebSocketDefaults(in)
	got := out[0].Headers["Authorization"]
	if got != "Bearer secret-abc" {
		t.Errorf("Authorization = %q, want %q", got, "Bearer secret-abc")
	}
	if out[0].Headers["X-Static"] != "plain" {
		t.Errorf("X-Static = %q, want plain", out[0].Headers["X-Static"])
	}
}

func TestValidateWebSocketSource(t *testing.T) {
	tests := []struct {
		name    string
		src     WebSocketSource
		wantErr string
	}{
		{
			name: "valid wss",
			src:  WebSocketSource{Name: "a", URL: "wss://x/y"},
		},
		{
			name: "valid ws",
			src:  WebSocketSource{Name: "a", URL: "ws://x/y"},
		},
		{
			name:    "missing name",
			src:     WebSocketSource{URL: "wss://x/y"},
			wantErr: "name is required",
		},
		{
			name:    "missing url",
			src:     WebSocketSource{Name: "a"},
			wantErr: "url is required",
		},
		{
			name:    "bad scheme",
			src:     WebSocketSource{Name: "a", URL: "http://x/y"},
			wantErr: "scheme must be ws or wss",
		},
		{
			name:    "no host",
			src:     WebSocketSource{Name: "a", URL: "wss://"},
			wantErr: "must have a host",
		},
		{
			name: "reconnect inversion",
			src: WebSocketSource{
				Name: "a", URL: "wss://x/y",
				ReconnectMin: 10 * time.Second,
				ReconnectMax: 1 * time.Second,
			},
			wantErr: "reconnect_max must be >= reconnect_min",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWebSocketSource(tt.src)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want contains %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateIndexesSourceErrors(t *testing.T) {
	cfg := &Config{
		Sources: SourcesConfig{
			WebSockets: []WebSocketSource{
				{Name: "ok", URL: "wss://x/y"},
				{Name: "", URL: "wss://x/y"},
			},
		},
	}
	err := validate(cfg)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "sources.websockets[1]") {
		t.Errorf("error = %q, want index [1]", err.Error())
	}
}

func TestModelDefaultAndEnvOverride(t *testing.T) {
	cfg := &Config{}
	applyDefaults(cfg)
	if cfg.Model != DefaultModel {
		t.Errorf("Model default = %q, want %q", cfg.Model, DefaultModel)
	}

	t.Setenv("HEIMDALL_MODEL", "claude-sonnet-5")
	applyEnvFallbacks(cfg)
	if cfg.Model != "claude-sonnet-5" {
		t.Errorf("Model after env = %q, want %q", cfg.Model, "claude-sonnet-5")
	}
}

func TestBatchSizeAndQueueMaxDefaults(t *testing.T) {
	tests := []struct {
		name          string
		in            Config
		wantBatchSize int
		wantQueueMax  int
	}{
		{"zero values get defaults", Config{}, DefaultBatchSize, DefaultQueueMax},
		{"explicit values kept", Config{BatchSize: 10, QueueMax: 5}, 10, 5},
		{"negative values get defaults", Config{BatchSize: -1, QueueMax: -1}, DefaultBatchSize, DefaultQueueMax},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.in
			applyDefaults(&cfg)
			if cfg.BatchSize != tt.wantBatchSize {
				t.Errorf("BatchSize = %d, want %d", cfg.BatchSize, tt.wantBatchSize)
			}
			if cfg.QueueMax != tt.wantQueueMax {
				t.Errorf("QueueMax = %d, want %d", cfg.QueueMax, tt.wantQueueMax)
			}
		})
	}
}

func TestProviderDefaultsAndModelSelection(t *testing.T) {
	tests := []struct {
		name         string
		in           Config
		wantProvider string
		wantModel    string
	}{
		{"empty defaults to anthropic + haiku", Config{}, ProviderAnthropic, DefaultModel},
		{"ollama gets glm cloud default model", Config{Provider: "ollama"}, ProviderOllama, DefaultOllamaModel},
		{"ollama keeps explicit model", Config{Provider: "ollama", Model: "qwen3.5:9b"}, ProviderOllama, "qwen3.5:9b"},
		{"anthropic keeps explicit model", Config{Provider: "anthropic", Model: "claude-sonnet-5"}, ProviderAnthropic, "claude-sonnet-5"},
		{"provider is case-insensitive", Config{Provider: "Ollama"}, ProviderOllama, DefaultOllamaModel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.in
			applyDefaults(&cfg)
			if cfg.Provider != tt.wantProvider {
				t.Errorf("Provider = %q, want %q", cfg.Provider, tt.wantProvider)
			}
			if cfg.Model != tt.wantModel {
				t.Errorf("Model = %q, want %q", cfg.Model, tt.wantModel)
			}
		})
	}
}

func TestProviderValidation(t *testing.T) {
	cfg := &Config{Provider: "openai"}
	applyDefaults(cfg)
	if err := validate(cfg); err == nil {
		t.Error("validate: want error for unknown provider, got nil")
	}
}

func TestAPIKeyEnvFallbackByProvider(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "anth-key")
	t.Setenv("OLLAMA_API_KEY", "olla-key")

	anth := &Config{Provider: ProviderAnthropic}
	applyDefaults(anth)
	applyEnvFallbacks(anth)
	if anth.APIKey != "anth-key" {
		t.Errorf("anthropic APIKey = %q, want anth-key", anth.APIKey)
	}

	olla := &Config{Provider: ProviderOllama}
	applyDefaults(olla)
	applyEnvFallbacks(olla)
	if olla.APIKey != "olla-key" {
		t.Errorf("ollama APIKey = %q, want olla-key", olla.APIKey)
	}

	explicit := &Config{Provider: ProviderOllama, APIKey: "from-yaml"}
	applyDefaults(explicit)
	applyEnvFallbacks(explicit)
	if explicit.APIKey != "from-yaml" {
		t.Errorf("explicit APIKey = %q, want from-yaml (yaml wins)", explicit.APIKey)
	}
}
