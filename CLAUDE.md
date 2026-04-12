# Heimdall

Heimdall is a Go service that routes application error logs to an LLM for automated diagnosis and fix suggestions.

## How it works

1. A web application emits logs.
2. [Vector](https://vector.dev) (configured in `vector.yaml`) aggregates those logs and watches for errors.
3. When Vector detects an error, it forwards the most recent batch of logs to Heimdall over HTTP. Heimdall can also **pull** from upstream WebSocket log sources configured under `sources.websockets` in `heimdall.yaml`, so events are not limited to the Vector push path.
4. Heimdall sends that batch to an LLM (Anthropic Claude via `anthropic-sdk-go`) to analyze the failure and propose a fix.
5. The result is broadcast to WebSocket subscribers on `GET /ws/results`.

## Layout

- `cmd/app` — main entrypoint for the Heimdall service.
- `cmd/app/config` — config loading (`heimdall.yaml`).
- `api/http` — Huma/Gin HTTP router and route registration (receives batches from Vector).
- `internal/app` — application event loop, debouncer, inflight tracker.
- `internal/app/agent` — LLM analyzer and system prompt template.
- `internal/app/store` — in-memory ring-buffer log store.
- `internal/stream` — `ReadStream` / `WriteStream` interfaces and implementations.
- `vector.yaml` — Vector pipeline config (sources, transforms, sink → Heimdall).
- `heimdall.yaml` — Heimdall service config (LLM, sources, server).

## Running

Tasks are defined in `Taskfile.yml`:

- `task run` — start both the Heimdall agent and Vector.
- `task run-agent` — run the Heimdall service only (`go run cmd/app/main.go`).
- `task run-vector` — run Vector only.

## Tests

```
go test ./...
go test -race ./...
go test ./... -cover
```

## Key dependencies

- `github.com/anthropics/anthropic-sdk-go` — LLM client.
- `github.com/danielgtaylor/huma/v2` — HTTP API framework.
- `github.com/gin-gonic/gin` — HTTP server.
- `gopkg.in/yaml.v3` — config parsing.
