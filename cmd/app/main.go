package app

import (
	"context"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tbright/heimdall/api/http"
	"github.com/tbright/heimdall/internal/app"
	"github.com/tbright/heimdall/internal/config"
	"github.com/tbright/heimdall/internal/lifecycle"
	"github.com/tbright/heimdall/internal/plugin"
	"github.com/tbright/heimdall/internal/plugin/registry"
	"github.com/tbright/heimdall/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	// Load config from heimdall.yaml (falls back to defaults + env vars)
	cfg, err := config.Load("heimdall.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if cfg.APIKey == "" {
		log.Fatal("ANTHROPIC_API_KEY not configured")
	}

	// Build plugins from config
	pluginLogger := log.New(os.Stderr, "[heimdall-plugins] ", log.LstdFlags)
	plugins := registry.BuildPlugins(cfg.Plugins, pluginLogger)
	dispatcher := plugin.NewDispatcher(plugins, pluginLogger)

	appLogger := log.New(os.Stderr, "[heimdall] ", log.LstdFlags)

	// Create application
	application, err := app.NewApplication(app.ApplicationConfig{
		Store:        store.New("/tmp/heimdall/data/all.jsonl"),
		LlmApiKey:    cfg.APIKey,
		DebounceTime: cfg.DebounceTime,
		Dispatcher:   &dispatcher,
		Logger:       appLogger,
	})
	if err != nil {
		log.Fatalf("failed to create application: %v", err)
	}

	srv := http.NewServer(http.ServerConfig{
		Address:     cfg.ServerPort,
		Application: application,
	})

	// Wait for interrupt signal in a goroutine, start server on main goroutine path
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		appLogger.Printf("starting server on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-quit

	// Graceful shutdown: dispatcher first, then server
	appLogger.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := lifecycle.GracefulShutdown(ctx, application, &dispatcher, srv); err != nil {
		log.Fatalf("failed to shutdown: %v", err)
	}
}
