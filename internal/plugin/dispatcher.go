package plugin

import (
	"context"
	"log"
	"sync"
)

// Dispatcher fans out PluginPayload to all registered plugins concurrently.
type Dispatcher struct {
	plugins []Plugin
	logger  *log.Logger
}

// NewDispatcher creates a Dispatcher with the given plugins and error logger.
func NewDispatcher(plugins []Plugin, logger *log.Logger) Dispatcher {
	return Dispatcher{
		plugins: plugins,
		logger:  logger,
	}
}

// Send fans out the payload to all plugins concurrently. Errors are logged, never returned.
func (d *Dispatcher) Send(ctx context.Context, payload PluginPayload) {
	if len(d.plugins) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, p := range d.plugins {
		wg.Add(1)
		go func(p Plugin) {
			defer wg.Done()
			if err := p.Send(ctx, payload); err != nil {
				d.logger.Printf("plugin %q send error: %v", p.Name(), err)
			}
		}(p)
	}
	wg.Wait()
}

// Shutdown calls Shutdown on all plugins.
func (d *Dispatcher) Shutdown(ctx context.Context) {
	for _, p := range d.plugins {
		if err := p.Shutdown(ctx); err != nil {
			d.logger.Printf("plugin %q shutdown error: %v", p.Name(), err)
		}
	}
}
