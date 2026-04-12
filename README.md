# Heimdall

Heimdall is a Go service that routes application error logs to an LLM for
automated diagnosis and fix suggestions, then broadcasts the results to
subscribed WebSocket clients.

## How it operates

### 1. Ingress — logs come in three ways

- **HTTP push** (`POST /ingest`): Vector pushes one log per request.
  `HandleVectorIngest` (`api/http/routes.go`) maps it to a `stream.Event` and
  pushes into an `HTTPIngestStream`.
- **WebSocket server** (`GET /stream`): A client upgrades and streams JSON
  events; `WebSocketStream` reads them into its channel.
- **WebSocket dial** (`sources.websockets` in config): Heimdall dials
  upstream log feeds via `WebSocketDialStream`, with auto-reconnect
  (`internal/stream/stream_ws_dial.go`). Wired in `cmd/app/main.go`.

All satisfy `stream.ReadStream`. Each is registered via
`application.AddStream`, which spawns a goroutine fan-in into the central
`a.events` channel (`internal/app/app.go`).

### 2. Event loop

`application.Start` runs a single goroutine that:

- Appends every event to the `LogStore` ring buffer.
- If the event is actionable (severity `error`/`fatal`) and
  `debounce.ShouldFire()`, launches `processAsync` as a tracked goroutine.

### 3. Analysis with dedup (`processAsync`)

- `store.Snapshot(100)` copies the last 100 events and computes a 12-char
  SHA-256 ID over their serialized contents.
- `inflight.tryAcquire(id)` returns immediately if another goroutine is
  already analyzing an identical batch. Otherwise it claims the slot and
  defers `release`.
- `agent.Process(ctx, snapshot)` runs with a 30s timeout. The system prompt
  asks for a one-liner in the form `cause: ... | fix: ...`.
- On success, `stream.AnalysisResult{ID, Result}` is written to `a.output`.

### 4. Egress — WebSocket fan-out

`a.output` is a `*WebSocketWriteStream`, wired in `cmd/app/main.go`.

- Clients connect to `GET /ws/results`. The route is `gin.WrapH(wsOut)`, so
  Gin hands the request to `WebSocketWriteStream.ServeHTTP`, which upgrades
  the connection and adds it to the client set.
- On each `Write`, the sink JSON-encodes once and broadcasts to every
  attached client under a mutex. Clients whose writes error are dropped and
  closed.
- On shutdown, `main.go` calls `application.Wait()` to drain in-flight
  analyses, then `wsOut.Close()` to close all subscribers, then
  `srv.Shutdown`.

### End-to-end

```
Vector → POST /ingest → event channel → store + debounce
       → snapshot + SHA-256 ID → inflight gate → LLM
       → AnalysisResult → broadcast to all /ws/results subscribers
```

Duplicate batches collapse to a single LLM call while in flight. Post-
completion dedup is intentionally not implemented — if the same batch
hashes identically immediately after completion, it will re-run.

## Layout

- `cmd/app` — main entrypoint for the Heimdall service.
- `cmd/app/config` — config loading (`heimdall.yaml`).
- `api/http` — Huma/Gin HTTP router and route registration.
- `internal/app` — application event loop, debouncer, inflight tracker.
- `internal/app/agent` — LLM analyzer and system prompt template.
- `internal/app/store` — in-memory ring-buffer log store with `Snapshot`.
- `internal/stream` — `ReadStream` / `WriteStream` interfaces and
  implementations (`HTTPIngestStream`, `WebSocketStream`,
  `WebSocketDialStream`, `WebSocketWriteStream`, `Combine`, `Merge`).
- `vector.yaml` — Vector pipeline config (sources, transforms, sink).
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
- `github.com/gorilla/websocket` — WebSocket upgrade.
- `gopkg.in/yaml.v3` — config parsing.
