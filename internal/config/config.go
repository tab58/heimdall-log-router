package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultServerPort   = ":7077"
	DefaultDebounceTime = 5 * time.Second
	DefaultPluginLogDir = "/tmp/heimdall/logs/"
)

// PluginDef defines a single plugin's configuration.
type PluginDef struct {
	Name   string            `yaml:"name"`
	Type   string            `yaml:"type"`
	Config map[string]string `yaml:"config"`
}

// Config holds all application configuration loaded from heimdall.yaml.
type Config struct {
	APIKey       string        `yaml:"api_key"`
	ServerPort   string        `yaml:"server_port"`
	DebounceTime time.Duration `yaml:"debounce_time"`
	PluginLogDir string        `yaml:"plugin_log_dir"`
	Plugins      []PluginDef   `yaml:"plugins"`
}

// Load reads a YAML config file from path and returns a Config.
// Returns defaults if the file does not exist. Returns an error for malformed YAML.
func Load(path string) (Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			applyDefaults(&cfg)
			applyEnvFallbacks(&cfg)
			return cfg, nil
		}
		return Config{}, fmt.Errorf("reading config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config: %w", err)
	}

	applyDefaults(&cfg)
	applyEnvFallbacks(&cfg)
	return cfg, nil
}

// applyDefaults fills zero-value fields with their default values.
func applyDefaults(cfg *Config) {
	if cfg.ServerPort == "" {
		cfg.ServerPort = DefaultServerPort
	}
	if cfg.DebounceTime == 0 {
		cfg.DebounceTime = DefaultDebounceTime
	}
	if cfg.PluginLogDir == "" {
		cfg.PluginLogDir = DefaultPluginLogDir
	}
}

// applyEnvFallbacks fills in config values from environment variables when not set.
func applyEnvFallbacks(cfg *Config) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("ANTHROPIC_API_KEY")
	}
}
