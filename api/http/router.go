package http

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	"github.com/tbright/heimdall/internal/app"
)

const (
	ServiceName = "heimdall"
	Version     = "1.0.0"
	Description = "Heimdall is a service that routes logs to the appropriate destination."
)

type ServerConfig struct {
	Address     string
	Application app.Application
}

func NewServer(cfg ServerConfig) *http.Server {
	return &http.Server{
		Addr: cfg.Address,
		Handler: getRouter(routerConfig{
			Application: cfg.Application,
		}),
	}
}

type routerConfig struct {
	Application app.Application
}

func getRouter(cfg routerConfig) *gin.Engine {
	r := gin.New()

	// assign the routes
	assignRoutes(r, cfg.Application)

	return r
}

func assignRoutes(r *gin.Engine, app app.Application) {
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
		Handler: HandleVectorIngest(app),
	})

	RegisterRoute(RegisterRouteArgs[askRequest, askResponse]{
		API: apiServer,
		Operation: huma.Operation{
			Method: "POST",
			Path:   "/ask",
		},
		Handler: HandleAsk(app),
	})

	RegisterRoute(RegisterRouteArgs[healthRequest, healthResponse]{
		API: apiServer,
		Operation: huma.Operation{
			Method: "GET",
			Path:   "/health",
		},
		Handler: HandleHealth(),
	})
}
