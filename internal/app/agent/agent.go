package agent

import (
	"context"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

// Agent analyzes a slice of events and returns a diagnosis. id is the
// request identifier (content hash of the event batch) and is included in
// progress output so clients can correlate heartbeats with results.
type Agent interface {
	Process(ctx context.Context, id string, events []stream.Event) (string, error)
}
