package http

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	heimdallapp "github.com/tab58/heimdall-log-router/internal/app"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

const (
	ServiceName = "heimdall"
	Version     = "1.0.0"
	Description = "Heimdall is a service that routes logs to the appropriate destination."
)

// ServerConfig holds the parameters for NewServer.
type ServerConfig struct {
	Address      string
	MonitorPath  string
	Application  heimdallapp.Application
	IngestStream *stream.HTTPIngestStream
	Monitor      *stream.MonitorWS
}

// NewServer builds an HTTP server with all routes registered.
func NewServer(cfg ServerConfig) *http.Server {
	return &http.Server{
		Addr: cfg.Address,
		Handler: getRouter(routerConfig{
			Application:  cfg.Application,
			IngestStream: cfg.IngestStream,
			Monitor:      cfg.Monitor,
			MonitorPath:  cfg.MonitorPath,
		}),
	}
}

type routerConfig struct {
	Application  heimdallapp.Application
	IngestStream *stream.HTTPIngestStream
	Monitor      *stream.MonitorWS
	MonitorPath  string
}

func getRouter(cfg routerConfig) *gin.Engine {
	r := gin.New()
	assignRoutes(r, cfg)
	return r
}

func assignRoutes(r *gin.Engine, cfg routerConfig) {
	monitorPath := cfg.MonitorPath
	if monitorPath == "" {
		monitorPath = "/ws/monitor"
	}
	if monitorPath == "/" {
		panic(`monitor_path may not be "/" — it collides with the web UI route`)
	}

	// Embedded web monitor UI (replaces the Bubble Tea TUI client).
	r.GET("/", HandleIndex(monitorPath))

	apiServer := humagin.New(r, huma.Config{
		OpenAPI: &huma.OpenAPI{
			OpenAPI: "3.1.0",
			Info: &huma.Info{
				Title:       ServiceName,
				Version:     Version,
				Description: Description,
			},
		},
		OpenAPIPath:   "/openapi",
		DocsPath:      "/docs",
		SchemasPath:   "/schemas",
		Formats:       huma.DefaultFormats,
		DefaultFormat: "application/json",
	})

	RegisterRoute(RegisterRouteArgs[ingestRequest, ingestResponse]{
		API: apiServer,
		Operation: huma.Operation{
			Method: "POST",
			Path:   "/ingest",
		},
		Handler: HandleVectorIngest(cfg.IngestStream),
	})

	RegisterRoute(RegisterRouteArgs[healthRequest, healthResponse]{
		API: apiServer,
		Operation: huma.Operation{
			Method: "GET",
			Path:   "/healthz",
		},
		Handler: HandleHealth(),
	})

	// WebSocket stream upgrade: GET /stream
	r.GET("/stream", HandleWebSocketStream(cfg.Application))

	// Monitor WebSocket (single client; web UI at GET /). ReplayQueue +
	// Decide are wired on the *stream.MonitorWS before the server starts.
	if cfg.Monitor != nil {
		r.GET(monitorPath, gin.WrapH(cfg.Monitor))
	}
}
