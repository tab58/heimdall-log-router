# AGENTS.md — heimdall

Heimdall is a Go service that watches a live log feed, queues error batches in a bounded processing queue, and runs them through the tenzing harness in-process agentic loop (from `github.com/tab58/tenzing-agent-harness`) for root-cause analysis. Ingest never pauses — the live log stream keeps flowing regardless of queue state. An embedded web UI served at GET / (single page, api/http/index.go) is the single client that decides whether each queued batch gets processed or cleared.

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for full code structure, data flow, and the monitor WebSocket protocol.

## Config quick-reference (`heimdall.yaml`)

| Key | Default | Purpose |
|---|---|---|
| `provider` | `anthropic` | LLM backend: `anthropic` or `ollama` (Ollama Cloud). |
| `model` | per provider | Analysis model (env `HEIMDALL_MODEL` overrides). Defaults: anthropic → `claude-haiku-4-5`, ollama → `glm-5.2:cloud`. |
| `server_port` | `:7077` | HTTP listen address (env `PORT` overrides). |
| `batch_debounce` | `3s` | Quiet window after the first error before snapshotting. |
| `batch_size` | `50` | Number of most-recent ring events copied into each batch. |
| `queue_max` | `20` | Max undecided batches held in the queue; new batches are rejected while full. |
| `code_search_dirs` | — | Folders listed in the analyzer system prompt as the places to search for source code (paths as the analyzer sees them, e.g. relative to `HEIMDALL_WORKSPACE_DIR`). |
| `monitor_path` | `/ws/monitor` | Single-client monitor WebSocket path. |
| `sources.websockets` | — | Upstream WS sources Heimdall pulls from. |
| `vector` | — | Embedded Vector pipeline written to `HEIMDALL_VECTOR_CONFIG_PATH`. |

The selected provider's API key (`api_key` in yaml, falling back to `ANTHROPIC_API_KEY` or `OLLAMA_API_KEY` to match `provider`) is required at startup. `HEIMDALL_WORKSPACE_DIR` scopes the harness's Read/Glob/Grep tools to a mounted source tree. Bash and Edit tools are disabled — log content is untrusted and the analyzer is read-only.

## Running

```bash
task run         # heimdall server + Vector sidecar
task run-agent   # server only
task run-vector  # vector only
```

Open http://localhost:7077/ for the monitor UI.

## Tests

```bash
go test ./...
go test -race ./...
go test ./... -cover
```

## Hard rules

- **Only `error` / `fatal` events trigger a batch.** `isActionable` in `internal/app/app.go` is case-insensitive; keep it that way.
- **Ingest never pauses.** There is no gate; the queue is bounded by `queue_max` and new batches are rejected while full. Do not reintroduce backpressure.
- **At most one running analysis; up to `queue_max` queued batches.** `Decide(process)` is rejected with `ErrAnalysisRunning` while another batch is running; queued (undecided) batches can pile up to `queue_max` before `finalizeBatch` starts rejecting new ones.
- **`/ws/monitor` is single-client.** A second dial returns 409. The web UI is the only client; all decide/stream traffic flows over that one socket; there is no `POST /decide`.
- **Server → monitor frames are typed.** Use `stream.MonitorWS.Send*` (`SendEvent`, `SendBatchQueued`, `SendBatchRemoved`, `SendNotice`, `SendDelta`, `SendStatus`, `SendDone`, `SendError`, `SendReset`) — do not hand-roll JSON into the connection.
- **`POST /ingest` accepts every severity.** Filtering happens downstream in `isActionable`, not at the HTTP boundary, so the monitor sees a complete live feed.

## Recipes

- **Add a new server→client frame type:** add a constant in `internal/stream/monitor_ws.go`, extend `MonitorFrame`, add a `Send*` method, and handle it in handleFrame in api/http/index.go. Bump `monitorProtocolVersion` if existing clients cannot ignore it (protocol is currently v2).
- **Change the batching window:** `batch_debounce` in `heimdall.yaml` (or `DefaultBatchDebounce` in `cmd/app/config/config.go`). `batch_size` controls how many events each batch copies.
- **Change what counts as actionable:** `isActionable` in `internal/app/app.go`. Keep the check case-insensitive and test with upstream sources that emit different casing.
- **New config field:** add to `Config` in `cmd/app/config/config.go`, extend `applyDefaults`, thread through `ApplicationConfig` or `ServerConfig` in `cmd/app/main.go`.

## Key dependencies

- `github.com/tab58/tenzing-agent-harness` — `pkg/tenzing` public harness API (in-process agentic loop), pinned to a tagged release.
- `github.com/tab58/llm-providers` — LLM clients (Anthropic, Ollama, and more); indirect via tenzing (`pkg/tenzing` aliases its types), pinned to a tagged release.
- `github.com/danielgtaylor/huma/v2` — HTTP API framework.
- `github.com/gin-gonic/gin` — HTTP server.
- `github.com/gorilla/websocket` — WS for `/stream`, `/ws/monitor`, and dial sources.
- `gopkg.in/yaml.v3` — config parsing.
