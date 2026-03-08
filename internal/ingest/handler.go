package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/tbright/log-router/internal/ai"
	"github.com/tbright/log-router/internal/store"
)

type Handler struct {
	store    *store.LogStore
	analyzer ai.Analyzer
	debounce debouncer
}

type debouncer struct {
	mu       sync.Mutex
	lastFire time.Time
	cooldown time.Duration
}

func (d *debouncer) shouldFire() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if time.Since(d.lastFire) < d.cooldown {
		return false
	}
	d.lastFire = time.Now()
	return true
}

func NewHandler(logStore *store.LogStore, analyzer ai.Analyzer) Handler {
	return Handler{
		store:    logStore,
		analyzer: analyzer,
		debounce: debouncer{cooldown: 5 * time.Second},
	}
}

type ingestRequest struct {
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"log_source"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
}

type askRequest struct {
	Question string `json:"question"`
	Context  int    `json:"context"` // number of recent log lines to include
}

func (h *Handler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	entry := store.LogEntry{
		Timestamp: req.Timestamp,
		Source:    req.Source,
		Level:     req.Level,
		Message:   req.Message,
		Service:   req.Service,
	}
	h.store.Append(entry)

	// Auto-analyze errors with debouncing
	if (entry.Level == "error" || entry.Level == "fatal") && h.debounce.shouldFire() {
		go h.analyzeAsync(entry)
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) HandleAsk(w http.ResponseWriter, r *http.Request) {
	var req askRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	contextSize := req.Context
	if contextSize <= 0 {
		contextSize = 100
	}

	ctx := r.Context()
	contextLogs := h.store.RecentContext(contextSize)

	analysis, err := h.analyzer.Ask(ctx, req.Question, contextLogs)
	if err != nil {
		http.Error(w, fmt.Sprintf("analysis failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"analysis": analysis,
	})
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) analyzeAsync(errorEntry store.LogEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contextLogs := h.store.RecentContext(100)
	analysis, err := h.analyzer.AnalyzeError(ctx, errorEntry, contextLogs)
	if err != nil {
		fmt.Printf("[log-router] AI analysis failed: %v\n", err)
		return
	}

	fmt.Printf("\n=== AI Analysis for [%s] error ===\n", errorEntry.Source)
	fmt.Printf("Error: %s\n", errorEntry.Message)
	fmt.Printf("\n%s\n", analysis)
	fmt.Println("=================================")
}
