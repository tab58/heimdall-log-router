package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/tbright/log-router/internal/ai"
	"github.com/tbright/log-router/internal/ingest"
	"github.com/tbright/log-router/internal/store"
)

func main() {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY not configured")
	}

	logStore := store.New("/tmp/logrouter/data/all.jsonl")
	analyzer := ai.NewAnalyzer(apiKey)
	handler := ingest.NewHandler(logStore, analyzer)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", handler.HandleIngest)
	mux.HandleFunc("POST /ask", handler.HandleAsk)
	mux.HandleFunc("GET /health", handler.HandleHealth)

	addr := ":7077"
	fmt.Printf("AI agent listening on %s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
