package stream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var _ ReadStream = (*WebSocketDialStream)(nil)

// wsTestServer wraps httptest.Server with a handler that upgrades and invokes
// a user-supplied callback with the server-side connection.
func wsTestServer(t *testing.T, handle func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		handle(conn, r)
	}))
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func writeJSONRaw(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func recvWithin(t *testing.T, ch <-chan Event, d time.Duration) (Event, bool) {
	t.Helper()
	select {
	case e, ok := <-ch:
		return e, ok
	case <-time.After(d):
		t.Fatalf("timed out waiting for event after %s", d)
		return Event{}, false
	}
}

func TestWebSocketDialStreamHappyPath(t *testing.T) {
	srv := wsTestServer(t, func(conn *websocket.Conn, _ *http.Request) {
		for range 3 {
			writeJSONRaw(t, conn, Event{Severity: "error", Message: "m", Service: "svc"})
		}
		// Hold the connection open so the client can drain before close.
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL:          wsURL(srv),
		BufferSize:   8,
		PingInterval: 0,
	})

	var got []Event
	for range 3 {
		e, ok := recvWithin(t, s.Events(), 2*time.Second)
		if !ok {
			t.Fatalf("channel closed early")
		}
		got = append(got, e)
	}
	cancel()
	_ = s.Close()

	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
}

func TestWebSocketDialStreamSkipsMalformed(t *testing.T) {
	srv := wsTestServer(t, func(conn *websocket.Conn, _ *http.Request) {
		if err := conn.WriteMessage(websocket.TextMessage, []byte("not-json")); err != nil {
			t.Errorf("write bad: %v", err)
			return
		}
		writeJSONRaw(t, conn, Event{Severity: "error", Message: "ok"})
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL: wsURL(srv), BufferSize: 8, PingInterval: 0,
	})

	e, ok := recvWithin(t, s.Events(), 2*time.Second)
	if !ok {
		t.Fatalf("channel closed early")
	}
	if e.Message != "ok" {
		t.Errorf("Message = %q, want %q", e.Message, "ok")
	}
	cancel()
	_ = s.Close()
}

func TestWebSocketDialStreamReconnects(t *testing.T) {
	var count atomic.Int32
	srv := wsTestServer(t, func(conn *websocket.Conn, _ *http.Request) {
		n := count.Add(1)
		writeJSONRaw(t, conn, Event{Severity: "error", Message: "conn", Service: "s"})
		if n == 1 {
			// First connection: close immediately after sending.
			return
		}
		// Second connection: send another event then hold.
		writeJSONRaw(t, conn, Event{Severity: "error", Message: "conn2", Service: "s"})
		time.Sleep(200 * time.Millisecond)
	})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL:          wsURL(srv),
		BufferSize:   8,
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
		PingInterval: 0,
	})

	// Expect at least 2 events, channel must not close between them.
	e1, ok := recvWithin(t, s.Events(), 2*time.Second)
	if !ok {
		t.Fatalf("channel closed before first event")
	}
	if e1.Message != "conn" {
		t.Errorf("e1.Message = %q", e1.Message)
	}
	e2, ok := recvWithin(t, s.Events(), 2*time.Second)
	if !ok {
		t.Fatalf("channel closed before second event — did not reconnect")
	}
	if e2.Message != "conn" && e2.Message != "conn2" {
		t.Errorf("e2.Message = %q", e2.Message)
	}

	if count.Load() < 2 {
		t.Errorf("dial count = %d, want >= 2", count.Load())
	}

	cancel()
	_ = s.Close()
}

func TestWebSocketDialStreamContextCancelClosesChannel(t *testing.T) {
	srv := wsTestServer(t, func(conn *websocket.Conn, _ *http.Request) {
		// Just hold the connection open.
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL: wsURL(srv), BufferSize: 8, PingInterval: 0,
	})

	// Give supervisor time to connect.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Events channel must close.
	select {
	case _, ok := <-s.Events():
		if ok {
			// Drain any straggling event, then check again.
			select {
			case _, ok2 := <-s.Events():
				if ok2 {
					t.Fatalf("events channel not closed after cancel")
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("events channel not closed after cancel (timeout)")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("events channel not closed after cancel (timeout)")
	}

	_ = s.Close()
}

func TestWebSocketDialStreamForwardsHeaders(t *testing.T) {
	authCh := make(chan string, 1)
	srv := wsTestServer(t, func(conn *websocket.Conn, r *http.Request) {
		select {
		case authCh <- r.Header.Get("Authorization"):
		default:
		}
		time.Sleep(100 * time.Millisecond)
	})
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL:     wsURL(srv),
		Headers: map[string]string{"Authorization": "Bearer xyz"},
	})

	var gotAuth string
	select {
	case gotAuth = <-authCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("server never received a connection")
	}
	cancel()
	_ = s.Close()

	if gotAuth != "Bearer xyz" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer xyz")
	}
}

func TestWebSocketDialStreamBackoffOnDialFailure(t *testing.T) {
	// Dial a port that is not listening. The supervisor should retry,
	// go through sleepBackoff, and exit cleanly on cancel.
	ctx, cancel := context.WithCancel(context.Background())
	s := NewWebSocketDialStream(ctx, WebSocketDialConfig{
		URL:          "ws://127.0.0.1:1/unreachable",
		BufferSize:   4,
		ReconnectMin: 5 * time.Millisecond,
		ReconnectMax: 20 * time.Millisecond,
		PingInterval: 0,
	})
	// Let it run through at least one retry.
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Events channel should close after cancel.
	select {
	case _, ok := <-s.Events():
		if ok {
			t.Fatalf("unexpected event from unreachable server")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("events channel did not close after cancel")
	}
	_ = s.Close()
}

func TestNextBackoff(t *testing.T) {
	tests := []struct {
		name        string
		current, m  time.Duration
		want        time.Duration
	}{
		{"double", time.Second, 30 * time.Second, 2 * time.Second},
		{"capped", 20 * time.Second, 30 * time.Second, 30 * time.Second},
		{"at-max", 30 * time.Second, 30 * time.Second, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextBackoff(tt.current, tt.m)
			if got != tt.want {
				t.Errorf("nextBackoff(%s, %s) = %s, want %s", tt.current, tt.m, got, tt.want)
			}
		})
	}
}
