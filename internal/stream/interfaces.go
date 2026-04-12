package stream

import "time"

// Event is the canonical log event type flowing through the system.
type Event struct {
	Timestamp time.Time
	Severity  string // "error", "fatal", "warn", "info", etc.
	Service   string
	Message   string
}

// AnalysisResult is the output of agent analysis, keyed by a content-hash ID
// over the log batch that produced it.
type AnalysisResult struct {
	ID     string
	Result string
}

// ReadStream is a source of Events that the application consumes.
type ReadStream interface {
	Events() <-chan Event
	Close() error
}

// WriteStream is a sink for AnalysisResults produced by the application.
type WriteStream interface {
	Write(AnalysisResult) error
	Close() error
}
