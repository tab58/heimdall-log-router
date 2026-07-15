# Heimdall Architecture

Deep reference for how Heimdall is put together. Read [CLAUDE.md](CLAUDE.md) first for the hard rules and recipes; this doc explains the *why*.

## 1. Big picture

Heimdall is a client/server pair:

- **Server** (`cmd/app`) — a Go service that ingests log events from Vector / upstream WS sources, runs an error → queue → decide → analyze state machine, and streams Claude's response to the monitor.
- **Monitor** (GET /) — an embedded single-page web UI that connects to the server over a single WebSocket (/ws/monitor), shows a live log feed on the left, a queue of undecided batches and the streamed Claude response on the right, and lets the operator decide whether each queued batch is processed or cleared.

```
                          ┌──────────────────────────────────────────────┐
                          │                   Heimdall server            │
   POST /ingest           │                                              │
 Vector ─────────────────▶│  HTTPIngestStream ─┐                         │
                          │                    ▼                         │
 upstream WS   GET /stream│                 events chan                  │
 (dial sources) ─────────▶│                    │                         │
                          │                    ▼                         │
                          │          ┌───────────────────┐               │
                          │          │ runLoop           │  never pauses │
                          │          │ (single goroutine)│               │
                          │          └──────┬────┬───────┘               │
                          │                 │    │                       │
                          │       SendEvent │    │ isActionable?         │
                          │                 ▼    ▼                       │
                          │           LogStore  batchTimer(3s)           │
                          │           (ring, no truncate)  │             │
                          │              on fire           ▼             │
                          │               Snapshot(batchSize)            │
                          │                    │  → queue (≤ queue_max)  │
                          │                    ▼                         │
                          │           SendBatchQueued                   │
                          │                    │                         │
                          │                    ▼                         │
                          │              queued batches                 │
                          │                    │                         │
                          │           Decide(process) │  Decide(clear)   │
                          │                 ┌─────────┴───────┐          │
                          │                 ▼                 ▼          │
                          │           runAnalysis        removeLocked +  │
                          │           (one at a time,    SendBatchRemoved│
                          │            runningID)         (reason:       │
                          │                 │              "deleted")    │
                          │                 ▼                            │
                          │           tenzing harness loop               │
                          │           progress/delta                     │
                          │                 │                            │
                          │                 ▼                            │
                          │            SendDelta/SendStatus              │
                          │                 │                            │
                          │                 ▼                            │
                          │           SendDone + auto-remove +           │
                          │           SendBatchRemoved(reason:"processed")│
                          └─────────────────┼─────────────────┼──────────┘
                                            │                 │
                                            ▼                 ▼
                                       ┌──────────────────────────┐
                                       │   Web monitor UI         │
                                       │  (browser, single        │
                                       │   GET /ws/monitor client)│
                                       └──────────────────────────┘
```

## 2. Directory layout

```
tools/heimdall/
├── cmd/
│   └── app/                  # Server entrypoint
│       ├── main.go           # Wire config + monitor + streams + HTTP
│       └── config/config.go  # heimdall.yaml loader
├── api/http/
│   ├── router.go             # Route registration + ServerConfig
│   ├── routes.go             # Handlers (/ingest, /healthz, /stream)
│   └── index.go              # Embedded web monitor page (HTML/CSS/JS)
├── internal/
│   ├── app/
│   │   ├── app.go            # Application state machine (queue, Decide, runAnalysis)
│   │   ├── store/            # Ring-buffer log store (Snapshot, Clear)
│   │   └── agent/
│   │       ├── agent.go      # Agent interface
│   │       ├── analyzer.go   # tenzing harness in-process agentic loop
│   │       └── prompts/      # Embedded system prompt template
│   └── stream/
│       ├── interfaces.go     # Event, ReadStream, WriteStream
│       ├── stream_http.go    # HTTP ingest channel
│       ├── stream_ws.go      # Server-side WS ingest (+ severity normalization)
│       ├── stream_ws_dial.go # Client-side WS dial source (reconnect, backoff)
│       └── monitor_ws.go     # Single-client monitor WS (typed frames, 409 on 2nd dial)
├── heimdall.yaml             # Server config (server_port, batch_debounce, monitor_path, sources, vector)
├── vector.yaml               # Vector sidecar pipeline
└── Taskfile.yml              # run, run-agent, run-vector
```

## 3. Event lifecycle

The server is a single-goroutine event loop (`application.runLoop` in `internal/app/app.go`) wrapped by per-stream fan-in goroutines and an analyzer goroutine kicked off on `process` decisions. Ingest never pauses — there is no gate or backpressure mechanism; the loop always drains `events` into the store and monitor regardless of queue state.

### Ingest

- **`POST /ingest`** (`api/http/routes.go:HandleVectorIngest`) — Vector's push path. Accepts **all** severities, lowercases `level`, wraps in `stream.Event`, and writes into `HTTPIngestStream`. The old filter for error-only has been intentionally removed so the monitor shows a full live feed.
- **`GET /stream`** — server-side WS upgrade; each opened connection becomes a `WebSocketStream` added to the app via `AddStream`. Same origin policy as `/ws/monitor`: no `Origin` header (non-browser dialers) or same-host `Origin` only.
- **`sources.websockets`** — outbound `WebSocketDialStream`s that dial upstream feeds, reconnect with exponential backoff, and forward decoded events. Both WS paths share `readEventsInto` which normalizes `Severity` to lowercase on the way in.

Every attached stream spawns a fan-in goroutine in `application.AddStream` that drains the stream's `Events()` channel into the central `application.events` channel. The goroutine exits when its stream closes **or** when the event loop exits — the latter is what lets `Wait` return after ctx cancel even for streams (like the HTTP ingest stream) that nothing ever closes.

### runLoop

```go
for {
  select {
  case <-ctx.Done(): return
  case <-batchC:
    batchC = nil
    a.finalizeBatch()
  case e, ok := <-a.events:
    if !ok { return }
    a.store.Append(e)
    a.monitor.SendEvent(e)
    if isActionable(e) { batchC = a.resetBatchTimer() }
  }
}
```

- **`store.Append`** — mutex-protected ring buffer with monotonic sequence numbers that survive eviction. The ring is never truncated; batches are copies taken via `Snapshot`, so overlapping windows are possible if a batch is finalized before older events age out.
- **`SendEvent`** — per-event push to the monitor, no batching. Drops silently if no monitor is attached.
- **`isActionable`** — case-insensitive `error|fatal` check. Non-actionable events are stored but do not start a batch.
- **`resetBatchTimer`** — (re)arms a `time.Timer` sized to `batchDebounce` (default 3s). Additional errors in the window extend it; the timer channel is selected as a sibling of the event channel so the fire runs in the loop goroutine.

### Finalize

When `batchC` fires, `finalizeBatch`:

1. `store.Snapshot(batchSize)` — copies the most recent `batch_size` (default 50) events currently in the ring with a content-hash id. The ring itself is **not** truncated, so a later batch can overlap with an earlier one.
2. If the queue already holds `queue_max` (default 20) undecided batches, the new batch is rejected: it is dropped, a log line is emitted, and `monitor.SendNotice` tells the operator the queue is full. No batch is queued.
3. If a batch with the same content-hash id is already queued (identical window), the new one is a no-op duplicate and is dropped silently.
4. Otherwise the batch is appended to `a.queue` (guarded by `a.mu`).
5. `monitor.SendBatchQueued(id, summary)` — announces the new queue row to the client. `summary` (`stream.BatchSummary`) is built by `summarize`: the first actionable event's message/service/timestamp (or the last event if none is actionable).

### Decide

`Decide(id, action)` runs on the HTTP goroutine (called by the monitor WS read loop through `SetDecideHandler`), operating on the in-memory queue (`a.queue`, guarded by `a.mu`):

- **`clear`** — finds the batch in the queue by id (`ErrUnknownBatch` if absent); if it is the batch currently running (`a.runningID`), returns `ErrAnalysisRunning` instead of clearing. Otherwise removes it from the queue and sends `SendBatchRemoved(id, "deleted")`.
- **`process`** — rejected with `ErrAnalysisRunning` if another batch is already running (`a.runningID != ""`); rejected with `ErrUnknownBatch` if `id` isn't in the queue. Otherwise sets `a.runningID = id`, creates the analysis context (bounded by `analysisTimeout`) and stores its `CancelFunc` in `a.cancelAnalysis`, then spawns `runAnalysis` in a new goroutine — the context is created before the spawn so a `cancel` decide can never race past an unset cancel func. Analysis is one-at-a-time; the ingest/event loop is never blocked by a running analysis.
- **`cancel`** — rejected with `ErrNotRunning` unless `id` is the batch currently being analyzed. Otherwise calls the running analysis's stored `context.CancelFunc` (`a.cancelAnalysis`), aborting the LLM call. The canceled batch stays in the queue for a retry or delete.

`runAnalysis` calls `agent.Process`, which builds an in-process tenzing harness loop (`tenzing.New` from `github.com/tab58/tenzing-agent-harness/pkg/tenzing`) with the embedded system prompt, the configured model definition and LLM factory (`tenzing.LLMFromModel`), and Bash/Edit disabled (logs are untrusted input; the analyzer is read-only). The batch is serialized as a markdown prompt (`agent.BuildPrompt`) and passed to `RunTurn`:

- `OnTextDelta` chunks → `DeltaFunc` → `SendDelta(id, text)`.
- Event hooks (tool use, reasoning iterations, failures) and a 5s heartbeat → `ProgressFunc` → `SendStatus(id, text)`.
- `RunTurn`'s return value → the final text → `SendDone(id, result)`.

Once `Process` returns (or errors), `runAnalysis` clears `a.runningID` and the current batch id, then branches on the outcome. Canceled (`context.Canceled`): the batch is kept in the queue and the monitor gets `notice("analysis canceled")`, `claude_done`, and `batch_removed(reason:"canceled")` — the latter tells the UI to go idle without dropping the queue row. Any other outcome: the batch is removed from the queue and `SendBatchRemoved(id, "processed")` is sent — automatic queue cleanup, no operator action required beyond the original `process` decide.

### Disconnected monitor

`MonitorWS.SetConnectHandler(app.ReplayQueue)` fires after the hello frame on every fresh attach. `ReplayQueue` re-sends a `batch_queued` frame for every batch currently in the queue. This lets the server keep queuing batches while no monitor is connected — ingest never pauses, batches accumulate in `a.queue` up to `queue_max`, and the web UI sees the full queue the moment it dials in.

## 4. Monitor WebSocket protocol

Single-client duplex JSON WebSocket at `GET /ws/monitor`. A second upgrade returns HTTP 409. Protocol version in the hello frame is `2` (v2 replaced the `log` frame with `batch_queued`/`batch_removed` and added `notice`). Upgrades enforce a same-origin check (gorilla's default): requests carrying an `Origin` header must match the request host (403 otherwise); clients without an `Origin` header (curl, Go dialers) are admitted.

### Server → client frames

| type | fields | meaning |
|---|---|---|
| `hello` | `version` | Sent immediately after upgrade. |
| `event` | `event` | Single log event for the live feed. Fired for every ingested event. |
| `batch_queued` | `batch_id`, `batch{first_error,service,count,timestamp}` | A batch entered the queue (or is being replayed to a freshly attached monitor). |
| `batch_removed` | `batch_id`, `reason` (`"deleted"`\|`"processed"`\|`"canceled"`) | A batch left the queue (cleared or processed) — or, for `"canceled"`, its analysis was aborted and the batch remains queued (the UI keeps the row and returns to idle). |
| `notice` | `msg` | Non-fatal informational message, e.g. queue full. |
| `claude_status` | `batch_id`, `text` | Heartbeat / tool use / cost line from the analyzer. |
| `claude_delta` | `batch_id`, `text` | One assistant text chunk (streamed). |
| `claude_done` | `batch_id`, `summary` | Terminal frame for a processed batch (analysis finished or failed). |
| `error` | `msg` | Protocol-level or analyzer error. |

### Client → server frames

| type | fields | meaning |
|---|---|---|
| `decide` | `batch_id`, `action` (`process`\|`clear`\|`cancel`) | Operator decision for a queued batch; `cancel` aborts the running analysis. |
| `reset` | — | Wipe the ring buffer and drop all queued batches (running analysis unaffected); acked with a server `reset` frame. |
| `ping` | — | Liveness only. |

### Wire struct

All frames share a single envelope — see `MonitorFrame` in `internal/stream/monitor_ws.go`. Unused fields are `omitempty`. Typed `SendEvent` / `SendBatchQueued` / `SendBatchRemoved` / `SendNotice` / `SendDelta` / `SendStatus` / `SendDone` / `SendError` / `SendReset` methods on `*MonitorWS` are the only supported way to write frames from the server.

## 5. Web monitor UI

The monitor is a single embedded page served at GET / (api/http/index.go). Layout is two panes: the left "logs" pane renders the live event feed; the right pane stacks a "queue" box (one row per queued batch, each with a `first_error`/service/count summary and per-row `process` / `✕` buttons) above the "analysis" box (streamed Claude output) and a controls bar (`reset` button, `cancel` button — grayed out unless an analysis is running — status line, connection state). Vanilla JS dials /ws/monitor, renders frames in handleFrame, and sends `decide` frames (`process`/`clear` from the per-row buttons, `cancel` from the Cancel button) and a `reset` frame from the Reset button. Reconnect uses 1s→30s exponential backoff; `ReplayQueue` repopulates the queue pane on reattach.

## 6. Invariants and gotchas

- **The queue is bounded.** `a.queue` (guarded by `a.mu`) holds at most `queue_max` (default 20) undecided batches; `finalizeBatch` rejects (and `SendNotice`s) new batches once full.
- **At most one running analysis.** `a.runningID` is a single string, guarded by `a.mu`. `Decide(process)` returns `ErrAnalysisRunning` while `runningID != ""`; a `clear` targeting the running batch also returns `ErrAnalysisRunning`.
- **Batch ids are stable and content-hashed.** `store.Snapshot` hashes the serialized events; the same window always produces the same id. `finalizeBatch` drops a newly-finalized batch as a no-op if an identical id is already queued (dedup on identical content).
- **Severity normalization must stay at the ingest boundary.** `POST /ingest` and `readEventsInto` both lowercase severity so downstream code (including `isActionable`) sees a canonical string.
- **Do not block the event loop.** `SendEvent` must return quickly. `monitor_ws.sendFrame` acquires a short mutex around `conn.WriteMessage`; if the client stalls the write will eventually error and the frame is dropped with the connection closed.
- **`cmd.Wait` races stdoutPipe.** `analyzer.go` must let the scanner goroutine drain to EOF *before* calling `cmd.Wait`. Heartbeat is decoupled via a `scannerDone` channel. Reintroducing the old "wait in parallel" pattern reintroduces a `-race` failure.
- **ctx cancellation is the shutdown signal.** `application.Wait` drains stream goroutines, closes `events`, waits for the loop to exit (`runLoop` returns on `<-ctx.Done()`), then waits for in-flight analysis. Analyses derive their context from the ctx handed to `Start` (bounded by `analysisTimeout`, 15m), so cancellation aborts a running LLM call instead of waiting it out — Ctrl+C must never sit behind an analysis. A second SIGINT force-quits (`main.go`).

## 7. Testing

Run from `tools/heimdall`:

```
go test ./...             # all packages
go test -race ./...       # required: catches analyzer callback races and queue races
go test ./... -cover      # coverage
```

Key test surfaces:

- `internal/app/app_test.go` — `TestErrorTriggersQueuedBatch`, `TestInfoLevelNoBatch`, `TestStreamContinuesWhilePending`, `TestDecideProcessRunsAnalyzer`, `TestDecideClearDeletesBatch`, `TestDecideUnknownID`, `TestMultipleBatchesQueue`, `TestQueueFullRejectsBatch`, `TestProcessWhileRunningRejected`, `TestReplayQueue`, `TestResetDropsQueue`, `TestNewApplicationDefaults`, `TestDecideCancelKeepsBatchQueued`, `TestDecideCancelNotRunning`.
- `internal/app/analyzer_test.go` — mock `provider.LLM` that streams canned chunks so `DeltaFunc`/`ProgressFunc` can be asserted without API access.
- `internal/app/integration_test.go` — full stream → batch → analyze → done flow (`TestIntegrationStreamToBatchToAnalyzer`, `TestIntegrationMultipleStreams`).
- `internal/app/store/store_test.go` — ring eviction, snapshot hashing (no truncate — the ring is never truncated).
- `cmd/app/config/config_test.go` — YAML load + defaults + env fallback, including `TestBatchSizeAndQueueMaxDefaults`.

## 8. Deployment notes

- The server is a single binary with an embedded system prompt; no `claude` CLI is required. `HEIMDALL_WORKSPACE_DIR` scopes the harness's Read/Glob/Grep tools to a mounted source tree; unset for local dev.
- `heimdall.yaml` can embed a `vector:` sub-tree. On startup `cmd/app/main.go` writes it to `HEIMDALL_VECTOR_CONFIG_PATH` (default `/tmp/heimdall/vector.yaml`) so a sidecar Vector process can launch against it.
- The `PORT` env var overrides `server_port`. The `provider` key selects the LLM backend (`anthropic` or `ollama`/Ollama Cloud); the matching API key (`api_key`, falling back to `ANTHROPIC_API_KEY` / `OLLAMA_API_KEY`) is validated at startup. `HEIMDALL_MODEL` / `model` selects the model (defaults: anthropic → `claude-haiku-4-5`, ollama → `glm-5.2:cloud`).
