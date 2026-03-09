package app

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/tbright/heimdall/internal/ai"
	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/store"
)

const (
	analysisTimeout    = 30 * time.Second
	defaultDebounce    = 5 * time.Second
	recentContextCount = 100
)

type Application interface {
	HandleVectorIngest(ctx context.Context, log *VectorIngestLogEntry) error
	HandleAsk(ctx context.Context, question string, numLogs int) (string, error)
	// Wait blocks until all in-flight background goroutines (e.g., auto-analysis) complete.
	Wait()
}

// analyzer is an internal interface for AI analysis, satisfied by ai.Analyzer.
type analyzer interface {
	AnalyzeError(ctx context.Context, errorEntry store.LogEntry, contextEntries []store.LogEntry) (string, error)
	Ask(ctx context.Context, question string, contextEntries []store.LogEntry) (string, error)
}

type application struct {
	store      *store.LogStore
	analyzer   analyzer
	debounce   debouncer
	dispatcher *plugin.Dispatcher
	logger     *log.Logger
	wg         sync.WaitGroup
}

type ApplicationConfig struct {
	Store        *store.LogStore
	LlmApiKey    string
	DebounceTime time.Duration
	Dispatcher   *plugin.Dispatcher
	Logger       *log.Logger
}

func NewApplication(cfg ApplicationConfig) (Application, error) {
	store := cfg.Store
	if store == nil {
		return nil, errors.New("store is required")
	}

	debounceTime := cfg.DebounceTime
	if debounceTime == 0 {
		debounceTime = defaultDebounce
	}

	llmApiKey := cfg.LlmApiKey
	if llmApiKey == "" {
		return nil, errors.New("llm api key is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	return &application{
		store:      cfg.Store,
		analyzer:   ai.NewAnalyzer(llmApiKey),
		debounce:   debouncer{cooldown: debounceTime},
		dispatcher: cfg.Dispatcher,
		logger:     logger,
	}, nil
}

func (a *application) HandleAsk(ctx context.Context, question string, numLogs int) (string, error) {
	contextLogs := a.store.RecentContext(numLogs)
	result, err := a.analyzer.Ask(ctx, question, contextLogs)
	if err != nil {
		return "", err
	}

	a.dispatchPayload(ctx, plugin.AskResponse, result, contextLogs)

	return result, nil
}

type VectorIngestLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"log_source"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
}

func (a *application) HandleVectorIngest(ctx context.Context, log *VectorIngestLogEntry) error {
	storeEntry := store.LogEntry{
		Timestamp: log.Timestamp,
		Source:    log.Source,
		Level:     log.Level,
		Message:   log.Message,
		Service:   log.Service,
	}
	a.store.Append(storeEntry)

	// Auto-analyze errors with debouncing
	if (log.Level == "error" || log.Level == "fatal") && a.debounce.ShouldFire() {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.analyzeAsync(storeEntry)
		}()
	}

	return nil
}

func (a *application) analyzeAsync(errorEntry store.LogEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), analysisTimeout)
	defer cancel()

	contextLogs := a.store.RecentContext(recentContextCount)
	analysis, err := a.analyzer.AnalyzeError(ctx, errorEntry, contextLogs)
	if err != nil {
		a.logger.Printf("AI analysis failed: %v", err)
		return
	}

	a.logger.Printf("=== AI Analysis for [%s] error ===", errorEntry.Source)
	a.logger.Printf("Error: %s", errorEntry.Message)
	a.logger.Printf("%s", analysis)
	a.logger.Printf("=================================")

	a.dispatchPayload(ctx, plugin.AutoAnalysis, analysis, contextLogs)
}

// Wait blocks until all in-flight background goroutines complete.
func (a *application) Wait() {
	a.wg.Wait()
}

// dispatchPayload sends a payload to all plugins via the dispatcher, if configured.
func (a *application) dispatchPayload(ctx context.Context, analysisType plugin.AnalysisType, analysis string, logEntries []store.LogEntry) {
	if a.dispatcher == nil {
		return
	}
	a.dispatcher.Send(ctx, plugin.PluginPayload{
		Type:       analysisType,
		Analysis:   analysis,
		LogEntries: logEntries,
		Timestamp:  time.Now(),
	})
}
