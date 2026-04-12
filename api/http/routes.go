package http

import (
	"context"
	"log"
	"net/http"
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
func HandleVectorIngest(ingestStream *stream.HTTPIngestStream) RouteHandler[ingestRequest, ingestResponse] {
	return func(_ context.Context, input *ingestRequest) (*ingestResponse, error) {
		e := stream.Event{
			Timestamp: input.Body.Timestamp,
			Severity:  input.Body.Level,
			Service:   input.Body.Service,
			Message:   input.Body.Message,
		}
		ingestStream.Write(e)
		return &ingestResponse{Status: http.StatusAccepted}, nil
	}
}

// --- GET /stream (WebSocket) ---

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

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
