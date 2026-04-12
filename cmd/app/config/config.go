package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultServerPort   = ":7077"
	DefaultDebounceTime = 5 * time.Second
)

// Config holds all application configuration loaded from heimdall.yaml.
type Config struct {
	APIKey       string        `yaml:"api_key"`
	ServerPort   string        `yaml:"server_port"`
	DebounceTime time.Duration `yaml:"debounce_time"`
	Sources      SourcesConfig `yaml:"sources"`
	// Vector holds an embedded Vector config tree. Written to disk at startup
	// so the Vector process can be launched against it.
	Vector yaml.Node `yaml:"vector"`
}

// SourcesConfig groups external input sources that Heimdall pulls from.
type SourcesConfig struct {
	WebSockets []WebSocketSource `yaml:"websockets"`
}

// WebSocketSource configures a single outbound WebSocket log source.
//
// Example:
//
//	sources:
//	  websockets:
//	    - name: upstream-router
//	      url: wss://logs.example.internal/stream
//	      headers:
//	        Authorization: "Bearer ${UPSTREAM_TOKEN}"
//	      ping_interval: 30s
type WebSocketSource struct {
	Name             string            `yaml:"name"`
	URL              string            `yaml:"url"`
	Headers          map[string]string `yaml:"headers"`
	HandshakeTimeout time.Duration     `yaml:"handshake_timeout"`
	ReconnectMin     time.Duration     `yaml:"reconnect_min"`
	ReconnectMax     time.Duration     `yaml:"reconnect_max"`
	PingInterval     time.Duration     `yaml:"ping_interval"`
	BufferSize       int               `yaml:"buffer_size"`
}

// Defaults applied to WebSocketSource entries when fields are zero-valued.
const (
	DefaultWSBufferSize       = 256
	DefaultWSHandshakeTimeout = 10 * time.Second
	DefaultWSReconnectMin     = 1 * time.Second
	DefaultWSReconnectMax     = 30 * time.Second
	DefaultWSPingInterval     = 30 * time.Second
)

// WriteVectorConfig marshals the embedded Vector config sub-tree to path.
// Returns an error if no vector config is present.
func (c Config) WriteVectorConfig(path string) error {
	if c.Vector.Kind == 0 {
		return errors.New("no vector config found under 'vector:' key in heimdall.yaml")
	}
	data, err := yaml.Marshal(&c.Vector)
	if err != nil {
		return fmt.Errorf("marshal vector config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir for vector config: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write vector config: %w", err)
	}
	return nil
}

// Load reads a YAML config file from path and returns a Config.
// Returns defaults if the file does not exist. Returns an error for malformed YAML.
func Load() (*Config, error) {
	configPath := os.Getenv("HEIMDALL_CONFIG_PATH")
	if configPath == "" {
		configPath = "heimdall.yaml"
	}

	var cfg *Config

	data, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			applyDefaults(cfg)
			applyEnvFallbacks(cfg)
			return cfg, nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	applyDefaults(cfg)
	applyEnvFallbacks(cfg)
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return cfg, nil
}

// applyDefaults fills zero-value fields with their default values.
func applyDefaults(cfg *Config) {
	if cfg == nil {
		return
	}
	if cfg.ServerPort == "" {
		cfg.ServerPort = DefaultServerPort
	}
	if cfg.DebounceTime == 0 {
		cfg.DebounceTime = DefaultDebounceTime
	}
	cfg.Sources.WebSockets = applyWebSocketDefaults(cfg.Sources.WebSockets)
}

// applyWebSocketDefaults returns a new slice with zero-valued fields filled in
// and ${VAR} placeholders in Headers values expanded from the environment.
func applyWebSocketDefaults(in []WebSocketSource) []WebSocketSource {
	if len(in) == 0 {
		return in
	}
	out := make([]WebSocketSource, len(in))
	for i, s := range in {
		if s.BufferSize <= 0 {
			s.BufferSize = DefaultWSBufferSize
		}
		if s.HandshakeTimeout <= 0 {
			s.HandshakeTimeout = DefaultWSHandshakeTimeout
		}
		if s.ReconnectMin <= 0 {
			s.ReconnectMin = DefaultWSReconnectMin
		}
		if s.ReconnectMax <= 0 {
			s.ReconnectMax = DefaultWSReconnectMax
		}
		if s.PingInterval == 0 {
			s.PingInterval = DefaultWSPingInterval
		}
		if len(s.Headers) > 0 {
			expanded := make(map[string]string, len(s.Headers))
			for k, v := range s.Headers {
				expanded[k] = os.ExpandEnv(v)
			}
			s.Headers = expanded
		}
		out[i] = s
	}
	return out
}

// validate checks required fields and semantic correctness of a Config.
func validate(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	for i, s := range cfg.Sources.WebSockets {
		if err := validateWebSocketSource(s); err != nil {
			return fmt.Errorf("sources.websockets[%d]: %w", i, err)
		}
	}
	return nil
}

func validateWebSocketSource(s WebSocketSource) error {
	if strings.TrimSpace(s.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(s.URL) == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(s.URL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return fmt.Errorf("url scheme must be ws or wss, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("url must have a host")
	}
	if s.ReconnectMax < s.ReconnectMin {
		return errors.New("reconnect_max must be >= reconnect_min")
	}
	return nil
}

// applyEnvFallbacks fills in config values from environment variables.
// ANTHROPIC_API_KEY is a fallback (yaml wins). HEIMDALL_SERVER_PORT is an
// override (env wins) so the container can set the port without editing yaml.
func applyEnvFallbacks(cfg *Config) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if port := os.Getenv("HEIMDALL_SERVER_PORT"); port != "" {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		cfg.ServerPort = port
	}
}
