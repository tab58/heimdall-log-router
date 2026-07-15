package http

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	heimdallapp "github.com/tab58/heimdall-log-router/internal/app"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// --- POST /ingest ---

type ingestRequest struct {
	Body struct {
		Timestamp time.Time `json:"timestamp"`
		Source    string    `json:"log_source"`
		Level     string    `json:"level"`
		Message   string    `json:"message"`
		Service   string    `json:"service"`
	}
}

type ingestResponse struct {
	Status int
	Body   struct {
		ErrorMessage string `json:"error,omitempty"`
	}
}

// HandleVectorIngest writes the incoming log entry to the HTTP ingest stream.
// All severities are accepted so the monitor can show a live feed; only
// error/fatal events trigger a batch downstream.
func HandleVectorIngest(ingestStream *stream.HTTPIngestStream) RouteHandler[ingestRequest, ingestResponse] {
	return func(_ context.Context, input *ingestRequest) (*ingestResponse, error) {
		level := strings.ToLower(strings.TrimSpace(input.Body.Level))
		e := stream.Event{
			Timestamp: input.Body.Timestamp,
			Source:    input.Body.Source,
			Severity:  level,
			Service:   input.Body.Service,
			Message:   input.Body.Message,
		}
		ingestStream.Write(e)
		return &ingestResponse{Status: http.StatusAccepted}, nil
	}
}

// --- GET /stream (WebSocket) ---

// Zero-value upgrader: gorilla's default CheckOrigin admits requests without
// an Origin header (the upstream log processes that dial this endpoint) and
// otherwise requires the Origin host to match the request host — blocking
// cross-site WebSocket hijacking from pages in an operator's browser.
var upgrader = websocket.Upgrader{}

// HandleWebSocketStream upgrades the connection to WebSocket and registers it as a stream.
func HandleWebSocketStream(application heimdallapp.Application) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("websocket upgrade failed: %v", err)
			return
		}
		ws := stream.NewWebSocketStream(conn, 256)
		application.AddStream(ws)
	}
}

// --- GET /healthz ---

type healthRequest struct{}
type healthResponse struct {
	Status int
	Body   struct {
		Status string `json:"status"`
	}
}

// HandleHealth returns a 200 OK with status "ok".
func HandleHealth() RouteHandler[healthRequest, healthResponse] {
	return func(_ context.Context, _ *healthRequest) (*healthResponse, error) {
		return &healthResponse{
			Body: struct {
				Status string `json:"status"`
			}{
				Status: "ok",
			},
		}, nil
	}
}
