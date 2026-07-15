# Heimdall

Heimdall watches a live log feed, queues error batches in a bounded queue, and
runs them through the tenzing harness in-process agentic loop for
root-cause analysis. Ingest never pauses — the live log stream keeps flowing
regardless of queue state. An embedded web UI is the single client that
decides whether each queued batch gets processed or cleared, and streams the
analysis back as it runs.

See **[AGENTS.md](AGENTS.md)** for the config reference, hard rules, and
recipes, and **[ARCHITECTURE.md](ARCHITECTURE.md)** for the full code
structure, data flow, and monitor WebSocket protocol.

## Running

```bash
task run         # heimdall server + Vector sidecar
task run-agent   # server only (go run cmd/app/main.go)
task run-vector  # vector only
```

The selected provider's API key is required at startup: `OLLAMA_API_KEY` for `provider: ollama`, `ANTHROPIC_API_KEY` for `provider: anthropic` (or `api_key` in `heimdall.yaml`).

Open http://localhost:7077/ for the monitor UI.

## Config

See the `heimdall.yaml` quick-reference table in [AGENTS.md](AGENTS.md).

## Tests

```bash
go test ./...
go test -race ./...
go test ./... -cover
```
