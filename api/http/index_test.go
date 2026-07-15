package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIndexRouteMonitorPathCollision(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal(`getRouter with MonitorPath "/" did not panic, want panic`)
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recovered value is %T, want string", r)
		}
		if !strings.Contains(msg, "monitor_path") {
			t.Errorf("panic message %q does not mention monitor_path", msg)
		}
	}()
	getRouter(routerConfig{MonitorPath: "/"})
}

func TestIndexRoute(t *testing.T) {
	tests := []struct {
		name        string
		monitorPath string
		wantInBody  string
	}{
		{"default monitor path", "", "/ws/monitor"},
		{"custom monitor path", "/custom/mon", "/custom/mon"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := getRouter(routerConfig{MonitorPath: tt.monitorPath})
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("GET / status = %d, want %d", w.Code, http.StatusOK)
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
				t.Errorf("Content-Type = %q, want text/html prefix", ct)
			}
			body := w.Body.String()
			if !strings.Contains(body, "<!DOCTYPE html>") {
				t.Errorf("body missing <!DOCTYPE html>")
			}
			if !strings.Contains(body, tt.wantInBody) {
				t.Errorf("body missing monitor path %q", tt.wantInBody)
			}
			if strings.Contains(body, "{{MONITOR_PATH}}") {
				t.Errorf("placeholder {{MONITOR_PATH}} not substituted")
			}
		})
	}
}
