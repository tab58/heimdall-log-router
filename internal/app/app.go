package app

import (
	"context"
	"fmt"
	"time"

	"github.com/tbright/heimdall/internal/ai"
	"github.com/tbright/heimdall/internal/store"
)

type Application interface {
	HandleVectorIngest(ctx context.Context, log *VectorIngestLogEntry) error
	HandleAsk(ctx context.Context, question string, numLogs int) (string, error)
}

type application struct {
	store    *store.LogStore
	analyzer ai.Analyzer
	debounce debouncer
}

type ApplicationConfig struct {
	Store        *store.LogStore
	LlmApiKey    string
	DebounceTime time.Duration
}

func NewApplication(cfg ApplicationConfig) (Application, error) {
	store := cfg.Store
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	debounceTime := cfg.DebounceTime
	if debounceTime == 0 {
		debounceTime = 5 * time.Second
	}

	llmApiKey := cfg.LlmApiKey
	if llmApiKey == "" {
		return nil, fmt.Errorf("llm api key is required")
	}

	return &application{
		store:    cfg.Store,
		analyzer: ai.NewAnalyzer(llmApiKey),
		debounce: debouncer{cooldown: debounceTime},
	}, nil
}

func (a *application) HandleAsk(ctx context.Context, question string, numLogs int) (string, error) {
	contextLogs := a.store.RecentContext(numLogs)
	return a.analyzer.Ask(ctx, question, contextLogs)
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

	// // Auto-analyze errors with debouncing
	// if (log.Level == "error" || log.Level == "fatal") && a.debounce.shouldFire() {
	// 	go a.analyzeAsync(storeEntry)
	// }

	return nil
}

func (a *application) analyzeAsync(errorEntry store.LogEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contextLogs := a.store.RecentContext(100)
	analysis, err := a.analyzer.AnalyzeError(ctx, errorEntry, contextLogs)
	if err != nil {
		fmt.Printf("[heimdall] AI analysis failed: %v\n", err)
		return
	}

	fmt.Printf("\n=== AI Analysis for [%s] error ===\n", errorEntry.Source)
	fmt.Printf("Error: %s\n", errorEntry.Message)
	fmt.Printf("\n%s\n", analysis)
	fmt.Println("=================================")
}
