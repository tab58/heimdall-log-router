package stream

import (
	"encoding/json"
	"strings"

	"github.com/gorilla/websocket"
)

// WebSocketStream reads JSON-encoded Events from a WebSocket connection.
type WebSocketStream struct {
	ch   chan Event
	conn *websocket.Conn
}

// NewWebSocketStream creates a WebSocketStream and starts a goroutine that
// reads events from the connection. The channel is closed when the connection
// is closed or an error occurs.
func NewWebSocketStream(conn *websocket.Conn, bufSize int) *WebSocketStream {
	s := &WebSocketStream{
		ch:   make(chan Event, bufSize),
		conn: conn,
	}
	go s.readLoop()
	return s
}

func (s *WebSocketStream) readLoop() {
	defer close(s.ch)
	_ = readEventsInto(s.conn, s.ch)
}

// Events returns the read-only event channel.
func (s *WebSocketStream) Events() <-chan Event {
	return s.ch
}

// Close closes the underlying WebSocket connection.
func (s *WebSocketStream) Close() error {
	return s.conn.Close()
}

// readEventsInto reads JSON-encoded Events from conn and forwards them to ch.
// Malformed messages are skipped. Returns on the first read error from conn.
func readEventsInto(conn *websocket.Conn, ch chan<- Event) error {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var e Event
		if err := json.Unmarshal(msg, &e); err != nil {
			continue
		}
		e.Severity = strings.ToLower(strings.TrimSpace(e.Severity))
		ch <- e
	}
}
