package stream

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// --- WebSocketStream tests ---

// TestWebSocketStreamImplementsStream verifies compile-time interface satisfaction.
var _ ReadStream = (*WebSocketStream)(nil)

// TestWebSocketStreamReceivesEvents verifies that JSON events sent over a WebSocket
// connection are received on the Events() channel.
func TestWebSocketStreamReceivesEvents(t *testing.T) {
	// Create a test HTTP server that upgrades to WebSocket and serves as the server side.
	serverUpgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	// serverConn is the server-side WebSocket connection.
	serverConnCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := serverUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("server upgrade: %v", err)
			return
		}
		serverConnCh <- conn
	}))
	defer srv.Close()

	// Dial from the client side.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	// Get the server-side connection (the stream reads from this).
	serverConn := <-serverConnCh

	s := NewWebSocketStream(serverConn, 8)

	// Send a JSON event from the client.
	e := Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "ws test error",
	}
	if err := clientConn.WriteJSON(e); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	// Close the client connection to signal end of stream.
	clientConn.Close()

	var received []Event
	for evt := range s.Events() {
		received = append(received, evt)
	}

	if len(received) != 1 {
		t.Fatalf("received %d events, want 1", len(received))
	}
	if received[0].Message != "ws test error" {
		t.Errorf("Message = %q, want %q", received[0].Message, "ws test error")
	}
}
