package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// stubApplication satisfies heimdallapp.Application; only AddStream is
// exercised by the /stream upgrade path.
type stubApplication struct {
	added chan stream.ReadStream
}

func (s *stubApplication) AddStream(rs stream.ReadStream) {
	select {
	case s.added <- rs:
	default:
	}
}
func (s *stubApplication) Start(context.Context)                    {}
func (s *stubApplication) Decide(string, stream.DecideAction) error { return nil }
func (s *stubApplication) Reset()                                   {}
func (s *stubApplication) ReplayQueue()                             {}
func (s *stubApplication) Wait()                                    {}

// TestStreamOriginCheck verifies the /stream upgrade's origin policy:
// non-browser clients (no Origin header) and same-origin requests may
// attach; cross-origin pages are rejected.
func TestStreamOriginCheck(t *testing.T) {
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
			app := &stubApplication{added: make(chan stream.ReadStream, 1)}
			srv := httptest.NewServer(getRouter(routerConfig{Application: app}))
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/stream"
			hdr := http.Header{}
			if o := tt.origin(srv.URL); o != "" {
				hdr.Set("Origin", o)
			}

			conn, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
			if tt.wantAttach {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				conn.Close()
				select {
				case <-app.added:
				case <-time.After(2 * time.Second):
					t.Fatal("expected AddStream to be called on successful upgrade")
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
