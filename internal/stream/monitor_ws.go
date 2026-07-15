package stream

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Monitor WS protocol version. Bumped on wire-incompatible changes.
// v2: log frame replaced by batch_queued/batch_removed; notice added.
const monitorProtocolVersion = "2"

// Monitor frame types.
const (
	FrameHello        = "hello"
	FrameEvent        = "event"
	FrameClaudeStatus = "claude_status"
	FrameClaudeDelta  = "claude_delta"
	FrameClaudeDone   = "claude_done"
	FrameError        = "error"
	FrameDecide       = "decide"
	FramePing         = "ping"
	FrameReset        = "reset"
	FrameBatchQueued  = "batch_queued"
	FrameBatchRemoved = "batch_removed"
	FrameNotice       = "notice"
)

// DecideAction is the client-side choice for a pending batch.
type DecideAction string

const (
	DecideProcess DecideAction = "process"
	DecideClear   DecideAction = "clear"
	DecideCancel  DecideAction = "cancel"
)

// BatchSummary is the queue-row payload for a batch_queued frame: enough
// for the operator to triage without the full event list.
type BatchSummary struct {
	FirstError string    `json:"first_error"`
	Service    string    `json:"service"`
	Count      int       `json:"count"`
	Timestamp  time.Time `json:"timestamp"`
}

// MonitorFrame is the JSON envelope exchanged over the monitor WebSocket.
// Fields are optional; only those relevant to a given Type are set.
type MonitorFrame struct {
	Type    string        `json:"type"`
	Version string        `json:"version,omitempty"`
	BatchID string        `json:"batch_id,omitempty"`
	Text    string        `json:"text,omitempty"`
	Summary string        `json:"summary,omitempty"`
	Event   *Event        `json:"event,omitempty"`
	Msg     string        `json:"msg,omitempty"`
	Action  string        `json:"action,omitempty"`
	Batch   *BatchSummary `json:"batch,omitempty"`
	Reason  string        `json:"reason,omitempty"`
}

// DecideFunc is invoked when the client sends a decide frame. It is called
// synchronously from the read goroutine and should return quickly; expensive
// work (e.g. spawning claude) must be dispatched to another goroutine.
type DecideFunc func(batchID string, action DecideAction)

// ResetFunc is invoked when the client sends a reset frame. The handler
// should wipe the ring buffer and any pending batch. Called synchronously
// from the read goroutine.
type ResetFunc func()

var errMonitorClosed = errors.New("monitor ws closed")

// ErrMonitorBusy is returned by ServeHTTP's rejected upgrade when a monitor
// is already attached. Exposed so callers can distinguish legitimate 409.
var ErrMonitorBusy = errors.New("monitor ws already attached")

// MonitorWS is a single-client WebSocket that the embedded web monitor dials.
// At most one connection is accepted at a time; a second upgrade attempt
// returns 409. The server uses typed Send* methods to push frames; the read
// side decodes client frames and routes decide actions to DecideFunc.
type MonitorWS struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	closed   bool
	decide   DecideFunc
	reset    ResetFunc
	connect  func()
	upgrader websocket.Upgrader
	logf     func(format string, args ...any)
}

// NewMonitorWS creates an unattached monitor stream. logf (may be nil) is
// called for protocol-level diagnostics.
func NewMonitorWS(logf func(format string, args ...any)) *MonitorWS {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &MonitorWS{
		// Zero-value upgrader: gorilla's default CheckOrigin admits requests
		// without an Origin header (non-browser clients) and otherwise
		// requires the Origin host to match the request host — blocking
		// cross-site WebSocket hijacking of the monitor by pages the
		// operator's browser happens to visit.
		upgrader: websocket.Upgrader{},
		logf:     logf,
	}
}

// SetDecideHandler installs the callback invoked when the client sends a
// decide frame. Must be called before ServeHTTP to avoid a window where a
// frame arrives with no handler.
func (m *MonitorWS) SetDecideHandler(fn DecideFunc) {
	m.mu.Lock()
	m.decide = fn
	m.mu.Unlock()
}

// SetResetHandler installs the callback invoked when the client sends a
// reset frame. Must be called before ServeHTTP.
func (m *MonitorWS) SetResetHandler(fn ResetFunc) {
	m.mu.Lock()
	m.reset = fn
	m.mu.Unlock()
}

// SetConnectHandler installs the callback fired after the hello frame is
// sent on a fresh attach. The handler typically calls Application.ReplayQueue
// so batches queued while no monitor was attached get surfaced.
func (m *MonitorWS) SetConnectHandler(fn func()) {
	m.mu.Lock()
	m.connect = fn
	m.mu.Unlock()
}

// Connected reports whether a client is currently attached.
func (m *MonitorWS) Connected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn != nil && !m.closed
}

// ServeHTTP upgrades the HTTP connection to a WebSocket and attaches the
// single client. Returns 409 if another monitor is already attached, or 503
// if the stream has been closed.
func (m *MonitorWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		http.Error(w, "monitor ws closed", http.StatusServiceUnavailable)
		return
	}
	if m.conn != nil {
		m.mu.Unlock()
		http.Error(w, "monitor already connected", http.StatusConflict)
		return
	}
	m.mu.Unlock()

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		m.logf("monitor upgrade failed: %v", err)
		return
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = conn.Close()
		return
	}
	m.conn = conn
	m.mu.Unlock()

	m.logf("monitor attached from %s", r.RemoteAddr)
	_ = m.sendFrame(MonitorFrame{Type: FrameHello, Version: monitorProtocolVersion})

	m.mu.Lock()
	hook := m.connect
	m.mu.Unlock()
	if hook != nil {
		hook()
	}

	m.readLoop(conn)
}

// readLoop drains the client side of the WebSocket until the connection
// closes. Decide frames are dispatched to the handler.
func (m *MonitorWS) readLoop(conn *websocket.Conn) {
	defer func() {
		m.mu.Lock()
		if m.conn == conn {
			m.conn = nil
		}
		m.mu.Unlock()
		_ = conn.Close()
		m.logf("monitor detached")
	}()

	conn.SetReadLimit(1 << 20)
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var f MonitorFrame
		if err := json.Unmarshal(data, &f); err != nil {
			m.logf("monitor frame decode: %v", err)
			continue
		}
		switch f.Type {
		case FrameDecide:
			m.mu.Lock()
			fn := m.decide
			m.mu.Unlock()
			if fn == nil {
				m.logf("decide frame with no handler")
				continue
			}
			fn(f.BatchID, DecideAction(f.Action))
		case FrameReset:
			m.mu.Lock()
			fn := m.reset
			m.mu.Unlock()
			if fn == nil {
				m.logf("reset frame with no handler")
				continue
			}
			fn()
		case FramePing:
			// liveness only
		default:
			m.logf("unknown client frame type %q", f.Type)
		}
	}
}

// SendEvent pushes a single log event to the monitor for the live feed.
func (m *MonitorWS) SendEvent(e Event) error {
	return m.sendFrame(MonitorFrame{Type: FrameEvent, Event: &e})
}

// SendBatchQueued announces a batch entering the processing queue. Also
// used to replay the queue to a freshly attached monitor.
func (m *MonitorWS) SendBatchQueued(batchID string, s BatchSummary) error {
	return m.sendFrame(MonitorFrame{Type: FrameBatchQueued, BatchID: batchID, Batch: &s})
}

// SendBatchRemoved announces a batch leaving the queue. reason is
// "deleted" (operator X) or "processed" (analysis finished).
func (m *MonitorWS) SendBatchRemoved(batchID, reason string) error {
	return m.sendFrame(MonitorFrame{Type: FrameBatchRemoved, BatchID: batchID, Reason: reason})
}

// SendNotice pushes a non-fatal informational message (e.g. queue full).
func (m *MonitorWS) SendNotice(msg string) error {
	return m.sendFrame(MonitorFrame{Type: FrameNotice, Msg: msg})
}

// SendStatus pushes a human-readable status line (heartbeat, tool use).
func (m *MonitorWS) SendStatus(batchID, text string) error {
	return m.sendFrame(MonitorFrame{Type: FrameClaudeStatus, BatchID: batchID, Text: text})
}

// SendDelta pushes one assistant text chunk.
func (m *MonitorWS) SendDelta(batchID, text string) error {
	return m.sendFrame(MonitorFrame{Type: FrameClaudeDelta, BatchID: batchID, Text: text})
}

// SendDone marks the end of a claude run or a cleared batch.
func (m *MonitorWS) SendDone(batchID, summary string) error {
	return m.sendFrame(MonitorFrame{Type: FrameClaudeDone, BatchID: batchID, Summary: summary})
}

// SendError pushes a protocol-level error message to the monitor.
func (m *MonitorWS) SendError(msg string) error {
	return m.sendFrame(MonitorFrame{Type: FrameError, Msg: msg})
}

// SendReset acknowledges a ring-buffer clear. The client wipes its local
// log pane and returns to idle on receipt.
func (m *MonitorWS) SendReset() error {
	return m.sendFrame(MonitorFrame{Type: FrameReset})
}

func (m *MonitorWS) sendFrame(f MonitorFrame) error {
	payload, err := json.Marshal(f)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMonitorClosed
	}
	if m.conn == nil {
		// No monitor attached — drop silently. The application replays
		// the queue on the next attach.
		return nil
	}
	if err := m.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		_ = m.conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

// Close terminates the current connection and marks the stream closed.
func (m *MonitorWS) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
	return nil
}
