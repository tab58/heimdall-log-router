package stream

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestMonitorWSOriginCheck verifies the upgrade handshake's origin policy:
// non-browser clients (no Origin header) and same-origin pages may attach;
// cross-origin pages are rejected so a malicious site the operator visits
// cannot hijack the monitor socket.
func TestMonitorWSOriginCheck(t *testing.T) {
	tests := []struct {
		name       string
		origin     func(serverURL string) string // "" = no Origin header
		wantAttach bool
	}{
		{"no origin header (non-browser client)", func(string) string { return "" }, true},
		{"same origin", func(u string) string { return u }, true},
		{"cross origin", func(string) string { return "http://evil.example" }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMonitorWS(nil)
			defer m.Close()
			srv := httptest.NewServer(m)
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
			hdr := http.Header{}
			if o := tt.origin(srv.URL); o != "" {
				hdr.Set("Origin", o)
			}

			conn, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
			if tt.wantAttach {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				defer conn.Close()
				var f MonitorFrame
				if err := conn.ReadJSON(&f); err != nil {
					t.Fatalf("read hello frame: %v", err)
				}
				if f.Type != FrameHello {
					t.Fatalf("first frame type = %q, want %q", f.Type, FrameHello)
				}
				return
			}
			if err == nil {
				conn.Close()
				t.Fatal("expected cross-origin dial to be rejected")
			}
			if resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("expected HTTP 403 on cross-origin upgrade, got %+v", resp)
			}
		})
	}
}

// TestMonitorWSQueueFrames verifies the v2 queue frames arrive with the
// documented shape: hello carries version 2, batch_queued carries the
// summary, batch_removed carries the reason, notice carries the message.
func TestMonitorWSQueueFrames(t *testing.T) {
	m := NewMonitorWS(nil)
	defer m.Close()
	srv := httptest.NewServer(m)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var hello MonitorFrame
	if err := conn.ReadJSON(&hello); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if hello.Version != "2" {
		t.Fatalf("hello version = %q, want %q", hello.Version, "2")
	}

	ts := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	summary := BatchSummary{FirstError: "boom", Service: "api", Count: 50, Timestamp: ts}
	if err := m.SendBatchQueued("abc123", summary); err != nil {
		t.Fatalf("SendBatchQueued: %v", err)
	}
	if err := m.SendBatchRemoved("abc123", "deleted"); err != nil {
		t.Fatalf("SendBatchRemoved: %v", err)
	}
	if err := m.SendNotice("queue full"); err != nil {
		t.Fatalf("SendNotice: %v", err)
	}

	var queued MonitorFrame
	if err := conn.ReadJSON(&queued); err != nil {
		t.Fatalf("read batch_queued: %v", err)
	}
	if queued.Type != FrameBatchQueued || queued.BatchID != "abc123" {
		t.Errorf("batch_queued frame = %+v", queued)
	}
	if queued.Batch == nil || queued.Batch.FirstError != "boom" ||
		queued.Batch.Service != "api" || queued.Batch.Count != 50 ||
		!queued.Batch.Timestamp.Equal(ts) {
		t.Errorf("batch summary = %+v, want %+v", queued.Batch, summary)
	}

	var removed MonitorFrame
	if err := conn.ReadJSON(&removed); err != nil {
		t.Fatalf("read batch_removed: %v", err)
	}
	if removed.Type != FrameBatchRemoved || removed.BatchID != "abc123" || removed.Reason != "deleted" {
		t.Errorf("batch_removed frame = %+v", removed)
	}

	var notice MonitorFrame
	if err := conn.ReadJSON(&notice); err != nil {
		t.Fatalf("read notice: %v", err)
	}
	if notice.Type != FrameNotice || notice.Msg != "queue full" {
		t.Errorf("notice frame = %+v", notice)
	}
}
