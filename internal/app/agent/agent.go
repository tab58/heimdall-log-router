package agent

import (
	"context"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

// Agent analyzes a slice of events and returns a diagnosis.
// The implementation is responsible for identifying the triggering event
// and building whatever prompt it needs from the full event window.
type Agent interface {
	Process(ctx context.Context, events []stream.Event) (string, error)
}
