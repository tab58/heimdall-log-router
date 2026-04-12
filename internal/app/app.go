package app

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/tab58/heimdall-log-router/internal/app/agent"
	"github.com/tab58/heimdall-log-router/internal/app/store"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

const (
	analysisTimeout    = 30 * time.Second
	defaultDebounce    = 5 * time.Second
	recentContextCount = 100
	eventsChanSize     = 512
)

// Application is the central abstraction: it consumes streams, routes events
// through the agent, and writes results to the configured output stream.
type Application interface {
	AddStream(s stream.ReadStream)
	Start(ctx context.Context)
	Wait()
}

// ApplicationConfig wires the components that make up the application.
type ApplicationConfig struct {
	DebounceTime time.Duration
	Logger       *log.Logger
	Output       stream.WriteStream
}

type application struct {
	agent     agent.Agent
	store     *store.LogStore
	debounce  debouncer
	inflight  inflight
	events    chan stream.Event
	logger    *log.Logger
	closeOnce sync.Once
	loopDone  chan struct{}
	streamWg  sync.WaitGroup // tracks stream fan-in goroutines
	wg        sync.WaitGroup // tracks analysis goroutines

	output stream.WriteStream
}

// NewApplication constructs an Application from cfg.
func NewApplication(cfg ApplicationConfig) Application {
	debounceTime := cfg.DebounceTime
	if debounceTime == 0 {
		debounceTime = defaultDebounce
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	analyzer := agent.NewAnalyzer()

	return &application{
		agent:    analyzer,
		store:    store.New(),
		debounce: debouncer{cooldown: debounceTime},
		events:   make(chan stream.Event, eventsChanSize),
		logger:   logger,
		loopDone: make(chan struct{}),
		output:   cfg.Output,
	}
}

// AddStream registers a stream with the application. A goroutine drains the
// stream's channel into the central events channel until the stream is closed.
func (a *application) AddStream(s stream.ReadStream) {
	a.streamWg.Go(func() {
		for e := range s.Events() {
			select {
			case a.events <- e:
			case <-a.loopDone:
				return
			}
		}
	})
}

// Start launches the main event loop. It returns immediately; use Wait to block
// until all in-flight work has completed after streams are closed.
func (a *application) Start(ctx context.Context) {
	go func() {
		defer close(a.loopDone)
		for {
			select {
			case e, ok := <-a.events:
				if !ok {
					return
				}
				a.store.Append(e)
				if isActionable(e) && a.debounce.ShouldFire() {
					a.wg.Go(func() { a.processAsync(e) })
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Wait blocks until all in-flight analysis goroutines complete.
//
// Sequence:
//  1. Wait for all stream goroutines to drain their channels into a.events.
//  2. Close a.events so the event loop drains any remaining buffered events.
//  3. Wait for the event loop to exit (guarantees all wg.Add calls have fired).
//  4. Wait for all analysis goroutines to complete.
func (a *application) Wait() {
	a.streamWg.Wait()
	a.closeOnce.Do(func() { close(a.events) })
	<-a.loopDone
	a.wg.Wait()
}

// isActionable returns true for events that should trigger agent analysis.
func isActionable(e stream.Event) bool {
	return e.Severity == "error" || e.Severity == "fatal"
}

func (a *application) processAsync(_ stream.Event) {
	snapshot, id := a.store.Snapshot(recentContextCount)
	if !a.inflight.tryAcquire(id) {
		return
	}
	defer a.inflight.release(id)

	ctx, cancel := context.WithTimeout(context.Background(), analysisTimeout)
	defer cancel()

	result, err := a.agent.Process(ctx, snapshot)
	if err != nil {
		a.logger.Printf("AI analysis failed: %v", err)
		return
	}

	if a.output != nil {
		if err := a.output.Write(stream.AnalysisResult{ID: id, Result: result}); err != nil {
			a.logger.Printf("output write failed: %v", err)
		}
	}
}
