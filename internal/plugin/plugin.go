package plugin

import (
	"context"
	"time"

	"github.com/tbright/heimdall/internal/store"
)

// AnalysisType identifies the source of AI output.
type AnalysisType string

const (
	AutoAnalysis AnalysisType = "auto_analysis"
	AskResponse  AnalysisType = "ask_response"
)

// PluginPayload is the data sent to every plugin after AI analysis.
type PluginPayload struct {
	Type       AnalysisType    `json:"type"`
	Analysis   string          `json:"analysis"`
	LogEntries []store.LogEntry `json:"log_entries"`
	Timestamp  time.Time       `json:"timestamp"`
}

// Plugin is the interface all plugins must implement.
type Plugin interface {
	Name() string
	Start() error
	Send(ctx context.Context, payload PluginPayload) error
	Shutdown(ctx context.Context) error
}
