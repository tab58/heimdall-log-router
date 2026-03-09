package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// projectRoot returns the project root by walking up from this test file.
func projectRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller")
	}
	// internal/config/example_test.go -> project root is 3 levels up
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// The example heimdall.yaml at project root should exist and be parseable by Load().
func TestExampleHeimdallYAML(t *testing.T) {
	root := projectRoot(t)
	path := filepath.Join(root, "heimdall.yaml")

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("heimdall.yaml does not exist at project root")
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("failed to parse example heimdall.yaml: %v", err)
	}

	// Should have defaults applied for any missing fields.
	if cfg.ServerPort == "" {
		t.Error("ServerPort should not be empty after loading example")
	}
}
