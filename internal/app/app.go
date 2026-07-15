package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tab58/tenzing-agent-harness/pkg/tenzing"

	"github.com/tab58/heimdall-log-router/internal/app/agent"
	"github.com/tab58/heimdall-log-router/internal/app/store"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

const (
	analysisTimeout      = 15 * time.Minute
	defaultBatchDebounce = 3 * time.Second
	defaultBatchSize     = 50
	defaultQueueMax      = 20
	eventsChanSize       = 512
	maxSummaryRunes      = 200
)

// ErrUnknownBatch is returned by Decide when no queued batch matches the id.
var ErrUnknownBatch = errors.New("no queued batch for id")

// ErrAnalysisRunning is returned by Decide when a process decision arrives
// while another batch is being analyzed, or a clear targets the running batch.
var ErrAnalysisRunning = errors.New("analysis already running")

// ErrNotRunning is returned by Decide when a cancel targets a batch that is
// not being analyzed.
var ErrNotRunning = errors.New("batch is not being analyzed")

// MonitorOutput is the subset of stream.MonitorWS the application needs.
type MonitorOutput interface {
	SendEvent(e stream.Event) error
	SendBatchQueued(batchID string, s stream.BatchSummary) error
	SendBatchRemoved(batchID, reason string) error
	SendNotice(msg string) error
	SendStatus(batchID, text string) error
	SendDelta(batchID, text string) error
	SendDone(batchID, summary string) error
	SendError(msg string) error
	SendReset() error
}

// Application is the central abstraction: it consumes streams, queues error
// batches, and streams analyzer output through a MonitorOutput.
type Application interface {
	AddStream(s stream.ReadStream)
	Start(ctx context.Context)
	Decide(id string, action stream.DecideAction) error
	Reset()
	ReplayQueue()
	Wait()
}

// ApplicationConfig wires the components that make up the application.
type ApplicationConfig struct {
	// BatchDebounce is the quiet window after the first error before the
	// last BatchSize ring events are snapshotted into a queued batch.
	BatchDebounce time.Duration
	// BatchSize is the number of most-recent events copied into each batch.
	BatchSize int
	// QueueMax caps the number of undecided batches; new batches are
	// rejected while the queue is full.
	QueueMax int
	Logger   *log.Logger
	Monitor  MonitorOutput
	// Model is the model definition the analyzer reasons with.
	Model tenzing.ModelDefinition
	// LLMFactory (may be nil) overrides how the harness builds LLM clients
	// from model definitions; nil resolves API keys from provider env vars.
	LLMFactory func(tenzing.ModelDefinition) (tenzing.LLM, error)
	// CodeSearchDirs lists folders rendered into the analyzer's system
	// prompt as the places to search for source code.
	CodeSearchDirs []string
}

// queuedBatch is one entry in the processing queue.
type queuedBatch struct {
	id       string
	snapshot []stream.Event
	summary  stream.BatchSummary
}

type application struct {
	agent    agent.Agent
	store    *store.LogStore
	monitor  MonitorOutput
	events   chan stream.Event
	logger   *log.Logger
	wg       sync.WaitGroup // analysis goroutines
	streamWg sync.WaitGroup // stream fan-in goroutines

	batchDebounce time.Duration
	batchTimer    *time.Timer
	batchSize     int
	queueMax      int

	// Queue state. Accessed from the event loop goroutine and from Decide
	// callers — protected by mu.
	mu             sync.Mutex
	queue          []*queuedBatch
	runningID      string
	cancelAnalysis context.CancelFunc // aborts the running analysis; nil when idle
	currentBatch   atomic.Pointer[string]

	closeOnce sync.Once
	loopDone  chan struct{}

	// runCtx is the lifecycle context handed to Start. Analyses derive
	// from it so shutdown cancels them instead of waiting them out.
	runCtx context.Context
}

// NewApplication constructs an Application from cfg.
func NewApplication(cfg ApplicationConfig) Application {
	batchDebounce := cfg.BatchDebounce
	if batchDebounce == 0 {
		batchDebounce = defaultBatchDebounce
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	queueMax := cfg.QueueMax
	if queueMax <= 0 {
		queueMax = defaultQueueMax
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	a := &application{
		store:         store.New(),
		monitor:       cfg.Monitor,
		events:        make(chan stream.Event, eventsChanSize),
		logger:        logger,
		batchDebounce: batchDebounce,
		batchSize:     batchSize,
		queueMax:      queueMax,
		loopDone:      make(chan struct{}),
	}

	// Analyzer progress lines become claude_status frames; assistant text
	// chunks become claude_delta frames. Both look up the current batch id
	// from an atomic pointer so the analyzer instance can be reused across
	// batches.
	progress := func(msg string) {
		id := a.loadBatchID()
		if a.monitor != nil {
			if err := a.monitor.SendStatus(id, msg); err != nil {
				logger.Printf("monitor status send failed: %v", err)
			}
		}
	}
	delta := func(text string) {
		id := a.loadBatchID()
		if a.monitor != nil {
			if err := a.monitor.SendDelta(id, text); err != nil {
				logger.Printf("monitor delta send failed: %v", err)
			}
		}
	}
	a.agent = agent.NewAnalyzer(cfg.Model, cfg.LLMFactory, logger, progress, delta, cfg.CodeSearchDirs)
	return a
}

// loadBatchID returns the current batch id (empty if idle).
func (a *application) loadBatchID() string {
	p := a.currentBatch.Load()
	if p == nil {
		return ""
	}
	return *p
}

func (a *application) setBatchID(id string) {
	a.currentBatch.Store(&id)
}

func (a *application) clearBatchID() {
	var empty string
	a.currentBatch.Store(&empty)
}

// AddStream registers a stream with the application. A goroutine drains the
// stream's channel into the central events channel until the stream is
// closed or the event loop exits. The loopDone case in the outer select
// matters for shutdown: without it, a stream that is never closed (e.g. the
// HTTP ingest stream on ctx cancel) parks this goroutine in the receive
// forever and Wait never returns. Stream goroutines are tracked in streamWg
// for orderly shutdown.
func (a *application) AddStream(s stream.ReadStream) {
	a.streamWg.Go(func() {
		for {
			select {
			case e, ok := <-s.Events():
				if !ok {
					return
				}
				select {
				case a.events <- e:
				case <-a.loopDone:
					return
				}
			case <-a.loopDone:
				return
			}
		}
	})
}

// Start launches the main event loop. ctx is retained so in-flight
// analyses are aborted when it is cancelled (shutdown must not wait out a
// long LLM call — that is what held the port after Ctrl+C).
func (a *application) Start(ctx context.Context) {
	a.runCtx = ctx
	go a.runLoop(ctx)
}

// runLoop is the single-goroutine event loop. Ingest never pauses: events
// always flow to the store and monitor regardless of queue state.
func (a *application) runLoop(ctx context.Context) {
	defer close(a.loopDone)

	var batchC <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-batchC:
			batchC = nil
			a.finalizeBatch()
		case e, ok := <-a.events:
			if !ok {
				return
			}
			a.store.Append(e)
			if a.monitor != nil {
				if err := a.monitor.SendEvent(e); err != nil {
					a.logger.Printf("monitor event send failed: %v", err)
				}
			}
			if isActionable(e) {
				batchC = a.resetBatchTimer()
			}
		}
	}
}

// resetBatchTimer (re)starts the batch debounce timer and returns its
// channel. The event loop captures the channel into a local batchC so the
// select can observe the fire.
func (a *application) resetBatchTimer() <-chan time.Time {
	if a.batchTimer == nil {
		a.batchTimer = time.NewTimer(a.batchDebounce)
		return a.batchTimer.C
	}
	if !a.batchTimer.Stop() {
		// Drain the channel if the timer already fired and the value
		// hasn't been received yet.
		select {
		case <-a.batchTimer.C:
		default:
		}
	}
	a.batchTimer.Reset(a.batchDebounce)
	return a.batchTimer.C
}

// finalizeBatch is called from the event loop when the batch timer fires.
// It copies the last batchSize events (no truncation — the ring keeps
// rolling) and appends the batch to the queue, unless the queue is full.
func (a *application) finalizeBatch() {
	snapshot, id, _ := a.store.Snapshot(a.batchSize)
	if len(snapshot) == 0 {
		return
	}

	a.mu.Lock()
	if len(a.queue) >= a.queueMax {
		a.mu.Unlock()
		a.logger.Printf("queue full (%d): batch %s rejected", a.queueMax, shortID(id))
		if a.monitor != nil {
			_ = a.monitor.SendNotice(fmt.Sprintf("queue full (%d batches) — new batch rejected", a.queueMax))
		}
		return
	}
	for _, b := range a.queue {
		if b.id == id {
			// Identical snapshot already queued (content-hash collision on
			// an unchanged window) — nothing new to analyze.
			a.mu.Unlock()
			return
		}
	}
	b := &queuedBatch{id: id, snapshot: snapshot, summary: summarize(snapshot)}
	a.queue = append(a.queue, b)
	a.mu.Unlock()

	a.logger.Printf("batch %s queued: %d events", shortID(id), len(snapshot))
	if a.monitor != nil {
		if err := a.monitor.SendBatchQueued(id, b.summary); err != nil {
			a.logger.Printf("monitor batch_queued send failed: %v", err)
		}
	}
}

// ReplayQueue re-sends every queued batch to the monitor. Called when a
// new monitor connects so batches queued while no client was attached
// (or before a reconnect) are surfaced.
func (a *application) ReplayQueue() {
	a.mu.Lock()
	batches := make([]*queuedBatch, len(a.queue))
	copy(batches, a.queue)
	a.mu.Unlock()

	if a.monitor == nil {
		return
	}
	for _, b := range batches {
		if err := a.monitor.SendBatchQueued(b.id, b.summary); err != nil {
			a.logger.Printf("replay queue failed: %v", err)
			return
		}
	}
}

// findLocked returns the queued batch with the given id. Caller holds mu.
func (a *application) findLocked(id string) *queuedBatch {
	for _, b := range a.queue {
		if b.id == id {
			return b
		}
	}
	return nil
}

// removeLocked deletes the batch with the given id from the queue and
// reports whether it was present. Caller holds mu.
func (a *application) removeLocked(id string) bool {
	for i, b := range a.queue {
		if b.id == id {
			a.queue = append(a.queue[:i], a.queue[i+1:]...)
			return true
		}
	}
	return false
}

// Decide handles a client decision for a queued batch. process runs the
// analyzer (one at a time); clear deletes the batch from the queue.
func (a *application) Decide(id string, action stream.DecideAction) error {
	switch action {
	case stream.DecideClear:
		a.mu.Lock()
		if a.runningID == id {
			a.mu.Unlock()
			return ErrAnalysisRunning
		}
		found := a.removeLocked(id)
		a.mu.Unlock()
		if !found {
			return ErrUnknownBatch
		}
		a.logger.Printf("batch %s deleted by client", shortID(id))
		if a.monitor != nil {
			if err := a.monitor.SendBatchRemoved(id, "deleted"); err != nil {
				a.logger.Printf("monitor batch_removed send failed: %v", err)
			}
		}
		return nil

	case stream.DecideProcess:
		a.mu.Lock()
		if a.runningID != "" {
			a.mu.Unlock()
			return ErrAnalysisRunning
		}
		b := a.findLocked(id)
		if b == nil {
			a.mu.Unlock()
			return ErrUnknownBatch
		}
		a.runningID = id
		base := a.runCtx
		if base == nil { // Start not called (unit tests driving Decide directly)
			base = context.Background()
		}
		// ctx/cancel are created here, not in runAnalysis, so a cancel
		// decide arriving before the goroutine is scheduled still aborts.
		ctx, cancelRun := context.WithTimeout(base, analysisTimeout)
		a.cancelAnalysis = cancelRun
		a.mu.Unlock()

		a.setBatchID(id)
		a.wg.Go(func() { a.runAnalysis(ctx, cancelRun, b) })
		return nil

	case stream.DecideCancel:
		a.mu.Lock()
		if a.runningID != id {
			a.mu.Unlock()
			return ErrNotRunning
		}
		cancel := a.cancelAnalysis
		a.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		a.logger.Printf("batch %s cancel requested by client", shortID(id))
		return nil

	default:
		return fmt.Errorf("unknown decide action %q", action)
	}
}

// Reset drops all queued batches (except one being analyzed), wipes the
// ring buffer, and sends a reset ack. Analysis in flight is NOT canceled.
func (a *application) Reset() {
	a.mu.Lock()
	var kept []*queuedBatch
	for _, b := range a.queue {
		if b.id == a.runningID {
			kept = append(kept, b)
		}
	}
	dropped := len(a.queue) - len(kept)
	a.queue = kept
	a.mu.Unlock()

	a.store.Clear()
	a.logger.Printf("ring buffer cleared (%d queued batches dropped)", dropped)

	if a.monitor != nil {
		if err := a.monitor.SendReset(); err != nil {
			a.logger.Printf("monitor reset send failed: %v", err)
		}
	}
}

// runAnalysis runs the agent on a batch, streams deltas through the
// monitor, and auto-removes the batch from the queue when finished. A
// canceled analysis keeps its batch queued so the operator can retry or
// delete it.
func (a *application) runAnalysis(ctx context.Context, cancel context.CancelFunc, b *queuedBatch) {
	defer cancel()

	result, err := a.agent.Process(ctx, b.id, b.snapshot)

	canceled := errors.Is(err, context.Canceled)
	a.mu.Lock()
	a.cancelAnalysis = nil
	a.runningID = ""
	if !canceled {
		a.removeLocked(b.id)
	}
	a.mu.Unlock()
	a.clearBatchID()

	if canceled {
		a.logger.Printf("analysis %s canceled", shortID(b.id))
		if a.monitor != nil {
			_ = a.monitor.SendNotice("analysis canceled")
			_ = a.monitor.SendDone(b.id, "")
			// reason "canceled" tells the UI to go idle without dropping
			// the queue row — the batch is still queued server-side.
			_ = a.monitor.SendBatchRemoved(b.id, "canceled")
		}
		return
	}

	if err != nil {
		a.logger.Printf("analysis %s failed: %v", shortID(b.id), err)
		if a.monitor != nil {
			_ = a.monitor.SendError(fmt.Sprintf("analysis failed: %v", err))
			_ = a.monitor.SendDone(b.id, "")
		}
	} else if a.monitor != nil {
		if err := a.monitor.SendDone(b.id, result); err != nil {
			a.logger.Printf("monitor done send failed: %v", err)
		}
	}
	if a.monitor != nil {
		if err := a.monitor.SendBatchRemoved(b.id, "processed"); err != nil {
			a.logger.Printf("monitor batch_removed send failed: %v", err)
		}
	}
}

// summarize builds the queue-row summary for a snapshot: the first
// actionable event's message/service/timestamp, falling back to the last
// event if the window somehow contains no error.
func summarize(events []stream.Event) stream.BatchSummary {
	s := stream.BatchSummary{Count: len(events)}
	for _, e := range events {
		if isActionable(e) {
			s.FirstError = truncateRunes(e.Message, maxSummaryRunes)
			s.Service = e.Service
			s.Timestamp = e.Timestamp
			return s
		}
	}
	if n := len(events); n > 0 {
		last := events[n-1]
		s.FirstError = truncateRunes(last.Message, maxSummaryRunes)
		s.Service = last.Service
		s.Timestamp = last.Timestamp
	}
	return s
}

// truncateRunes shortens s to at most n runes, appending an ellipsis when
// truncated. Rune-based so multi-byte characters are never split.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// Wait blocks until all in-flight work has completed after streams are
// closed. Sequence: drain stream goroutines, close events, wait for loop,
// wait for analysis.
func (a *application) Wait() {
	a.streamWg.Wait()
	a.closeOnce.Do(func() { close(a.events) })
	<-a.loopDone
	a.wg.Wait()
}

// isActionable returns true for events that should trigger a batch. Only
// error and fatal events qualify — debug/info/warn never cause a batch to
// fire. The comparison is case-insensitive so upstream sources that emit
// "ERROR" or "Fatal" are handled the same as lowercase.
func isActionable(e stream.Event) bool {
	sev := strings.ToLower(strings.TrimSpace(e.Severity))
	return sev == "error" || sev == "fatal"
}

// shortID returns the first 8 characters of id for log output.
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
