package lifecycle

import (
	"context"

	"github.com/tbright/heimdall/internal/plugin"
)

// ServerShutdowner is satisfied by any server with a Shutdown method (e.g., *http.Server).
type ServerShutdowner interface {
	Shutdown(ctx context.Context) error
}

// Waiter blocks until background work is done (e.g., in-flight analysis goroutines).
type Waiter interface {
	Wait()
}

// GracefulShutdown waits for in-flight goroutines, shuts down the dispatcher, then the server.
func GracefulShutdown(ctx context.Context, app Waiter, dispatcher *plugin.Dispatcher, server ServerShutdowner) error {
	if app != nil {
		app.Wait()
	}
	if dispatcher != nil {
		dispatcher.Shutdown(ctx)
	}
	return server.Shutdown(ctx)
}
