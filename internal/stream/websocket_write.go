package stream

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var errWriteStreamClosed = errors.New("websocket write stream closed")

// WebSocketWriteStream is a WriteStream that fans out AnalysisResults to every
// attached WebSocket client. Clients attach by hitting ServeHTTP, which upgrades
// the connection and adds it to the broadcast set. Clients whose writes error
// are dropped.
type WebSocketWriteStream struct {
	mu       sync.Mutex
	clients  map[*websocket.Conn]struct{}
	closed   bool
	upgrader websocket.Upgrader
}

// NewWebSocketWriteStream creates an empty WebSocketWriteStream with a
// permissive origin check.
func NewWebSocketWriteStream() *WebSocketWriteStream {
	return &WebSocketWriteStream{
		clients: make(map[*websocket.Conn]struct{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// Write JSON-encodes r once and broadcasts to every attached client. Clients
// whose writes error are removed and closed.
func (s *WebSocketWriteStream) Write(r AnalysisResult) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errWriteStreamClosed
	}
	var dead []*websocket.Conn
	for c := range s.clients {
		if err := c.WriteMessage(websocket.TextMessage, payload); err != nil {
			dead = append(dead, c)
		}
	}
	for _, c := range dead {
		delete(s.clients, c)
		_ = c.Close()
	}
	s.mu.Unlock()
	return nil
}

// ServeHTTP upgrades the HTTP connection to a WebSocket and adds it to the
// broadcast set. If the sink is already closed, the upgrade is rejected.
func (s *WebSocketWriteStream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "sink closed", http.StatusServiceUnavailable)
		return
	}
	s.mu.Unlock()

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.clients[conn] = struct{}{}
	s.mu.Unlock()
}

// Close closes every attached connection and marks the sink closed. Subsequent
// Write calls return an error.
func (s *WebSocketWriteStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for c := range s.clients {
		_ = c.Close()
	}
	s.clients = nil
	return nil
}
