# Plan: Outbound WebSocket Log Sources

Let Heimdall dial configured WebSocket endpoints and ingest their messages as
`stream.Event`s, alongside the existing `/ingest` HTTP path and in-process
`/stream` subscribers.

## Current state

- `internal/stream/stream_ws.go` — `WebSocketStream` wraps an **already-upgraded
  server-side** `*websocket.Conn` (used by `api/http/routes.go:55`
  `HandleWebSocketStream`). It has no dial logic, no reconnect, and its
  `readLoop` closes the event channel on any read error.
- `internal/app/app.go:78` — `application.AddStream` fans any `stream.ReadStream`
  into the central event channel. A new dial-based source only needs to
  implement `ReadStream` (`Events() <-chan Event`, `Close() error`) to plug in.
- `cmd/app/config/config.go` — `Config` currently has no notion of external
  input sources. Vector sub-tree is embedded but unrelated.
- `cmd/app/main.go:52` — only one input stream is wired today
  (`NewHTTPIngestStream`).

## Design

### 1. New type: `stream.WebSocketDialStream`

New file `internal/stream/stream_ws_dial.go`. Client-side counterpart to
`WebSocketStream`:

- Config struct:
  ```go
  type WebSocketDialConfig struct {
      URL             string            // ws:// or wss://
      Headers         map[string]string // e.g. Authorization
      BufferSize      int               // channel buffer, default 256
      HandshakeTimeout time.Duration    // default 10s
      ReconnectMin    time.Duration     // backoff floor, default 1s
      ReconnectMax    time.Duration     // backoff ceiling, default 30s
      PingInterval    time.Duration     // 0 disables, default 30s
  }
  ```
- Constructor `NewWebSocketDialStream(ctx context.Context, cfg WebSocketDialConfig) *WebSocketDialStream`
  starts a supervisor goroutine. The supervisor loop:
  1. Dials with `websocket.DefaultDialer.DialContext`, applying headers.
  2. On success, runs a read loop that JSON-decodes each message into
     `stream.Event` and forwards to the buffered channel. Malformed messages
     are logged and skipped (matching existing server-side behavior).
  3. On any read/dial error, closes the current conn and reconnects with
     exponential backoff (`ReconnectMin` → `ReconnectMax`, full jitter).
  4. Exits — and closes the event channel exactly once — when the supplied
     context is canceled or `Close()` is called.
- Pings: if `PingInterval > 0`, a separate goroutine sends WebSocket ping
  control frames; the conn's `SetReadDeadline` is refreshed on each pong so a
  half-open TCP connection surfaces as a read error and triggers reconnect.
- Thread safety: single writer to the channel (the read loop), `sync.Once`
  guards channel close, atomic flag prevents double-close of the conn.

### 2. Refactor — share the decode path

The existing `stream_ws.go` read loop and the new dialer both decode
`websocket.Conn → Event`. Extract a small helper:

```go
func readEventsInto(conn *websocket.Conn, ch chan<- Event) error
```

Both stream types call it; the server-side variant exits on return, the client
variant catches the error and reconnects.

### 3. Config schema

Extend `cmd/app/config/config.go`:

```go
type Config struct {
    // ...existing fields...
    Sources SourcesConfig `yaml:"sources"`
}

type SourcesConfig struct {
    WebSockets []WebSocketSource `yaml:"websockets"`
}

type WebSocketSource struct {
    Name             string            `yaml:"name"`
    URL              string            `yaml:"url"`
    Headers          map[string]string `yaml:"headers"`
    HandshakeTimeout time.Duration     `yaml:"handshake_timeout"`
    ReconnectMin     time.Duration     `yaml:"reconnect_min"`
    ReconnectMax     time.Duration     `yaml:"reconnect_max"`
    PingInterval     time.Duration     `yaml:"ping_interval"`
    BufferSize       int               `yaml:"buffer_size"`
}
```

Validation in `applyDefaults` / a new `validate()`:
- `Name` and `URL` required; URL must parse and use `ws`/`wss`.
- Env expansion on `Headers` values (e.g. `${SLACK_TOKEN}`) so secrets stay out
  of YAML.
- Fill defaults (buffer 256, handshake 10s, reconnect 1s–30s, ping 30s).

Example `heimdall.yaml` snippet in the file's comment block:
```yaml
sources:
  websockets:
    - name: upstream-router
      url: wss://logs.example.internal/stream
      headers:
        Authorization: "Bearer ${UPSTREAM_TOKEN}"
      ping_interval: 30s
```

### 4. Wiring in `cmd/app/main.go`

After `application := app.NewApplication(...)`, before `application.Start(ctx)`:

```go
for _, src := range cfg.Sources.WebSockets {
    ws := stream.NewWebSocketDialStream(ctx, stream.WebSocketDialConfig{
        URL: src.URL, Headers: src.Headers, /* ... */
    })
    application.AddStream(ws)
    appLogger.Printf("websocket source %q dialing %s", src.Name, src.URL)
}
```

Lifecycle: the dialer's supervisor is bound to the same `ctx` that drives
`application.Start`, so `cancel()` in the shutdown path stops redials and
closes event channels, which lets `application.Wait()` unblock naturally.

### 5. Tests (`internal/stream/stream_ws_dial_test.go`)

Use `httptest.NewServer` with a gorilla `websocket.Upgrader` as a fake upstream.
Table-driven where practical:

- **happy path** — server sends 3 JSON events; dial stream emits all 3 in
  order.
- **malformed message skipped** — server sends `not-json` then a valid event;
  only the valid one is emitted, no panic.
- **reconnect on server close** — server accepts, closes, accepts again;
  stream emits events from both connections without closing its output
  channel.
- **context cancel stops supervisor** — canceling ctx closes the events
  channel and terminates goroutines (verify with a short deadline + goroutine
  count or a `sync.WaitGroup` exposed for tests).
- **headers forwarded** — test server asserts `Authorization` header on the
  upgrade request.
- **env expansion in config** — `config_test.go` covers `${VAR}` in
  `Headers`.

Run with `-race`. Target 80%+ coverage on the new file.

### 6. Docs

- Update `CLAUDE.md` "How it works" to note that Heimdall can also **pull**
  from WebSocket sources, not just receive from Vector.
- Add a `sources:` section to the comment header of `heimdall.yaml`.

## Implementation order

1. Extract `readEventsInto` helper; keep existing `stream_ws.go` behavior
   identical (regression-safe refactor).
2. Add `WebSocketDialStream` + unit tests.
3. Extend `Config` + `applyDefaults` + env expansion + config tests.
4. Wire sources in `main.go`.
5. Manual smoke test: run a trivial `wscat`-style echo server that emits a
   fatal-level event; confirm Heimdall analyzes it.
6. Update docs.

## Out of scope

- Authentication schemes beyond static headers (mTLS, OAuth refresh).
- Message formats other than a single JSON `Event` per frame (no NDJSON
  framing, no protobuf). Can be added later behind a `codec:` field.
- Backpressure to the upstream — if the app's events channel is full the
  dialer blocks on channel send until shutdown, same as every other stream
  today.
- Dynamic reconfiguration — sources are read once at startup; changing
  `heimdall.yaml` requires a restart.

## Risks

- **Duplicate events** if the upstream replays on reconnect. Acceptable —
  `inflight` dedupe already keys analysis on a content hash of the recent
  batch.
- **Goroutine leak** if `Close()` races with ctx cancel. Mitigation:
  `sync.Once` on channel close; supervisor exits on whichever signal arrives
  first.
- **Slow consumer** stalling the dialer. Mitigation: buffered channel
  (configurable), and the central `application.events` channel is also
  buffered (512).
