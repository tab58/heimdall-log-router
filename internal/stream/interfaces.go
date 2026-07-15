package stream

import "time"

// Event is the canonical log event type flowing through the system.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source,omitempty"` // upstream ingress label, e.g. "docker", "http", "ws:upstream-router"
	Severity  string    `json:"severity,omitempty"`
	Service   string    `json:"service,omitempty"`
	Message   string    `json:"message"`
}

// Message kinds for AnalysisResult. "result" is the terminal analyzer output;
// "status" is an intermediate progress update (e.g., claude still running).
const (
	KindResult = "result"
	KindStatus = "status"
	KindPrompt = "prompt"
)

// AnalysisResult is the output of agent analysis, keyed by a content-hash ID
// over the log batch that produced it. Kind distinguishes terminal results
// from intermediate status updates; empty Kind is treated as a result.
// Trigger is populated on KindPrompt messages so subscribers can display the
// exact error that would start a Claude session before approving.
type AnalysisResult struct {
	ID      string `json:"id"`
	Kind    string `json:"kind,omitempty"`
	Result  string `json:"result"`
	Trigger *Event `json:"trigger,omitempty"`
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
