package main

import (
	"context"
	"fmt"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tab58/heimdall-log-router/api/http"
	"github.com/tab58/heimdall-log-router/cmd/app/config"
	"github.com/tab58/heimdall-log-router/internal/app"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

const shutdownTimeout = 10 * time.Second

var cfg *config.Config

func init() {
	var err error
	cfg, err = config.Load()
	if err != nil {
		fmt.Printf("failed to load config: %v", err)
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	appLogger := log.New(os.Stderr, "[heimdall] ", log.LstdFlags)

	// Results sink: broadcasts agent output to WebSocket subscribers.
	wsOut := stream.NewWebSocketWriteStream()

	// Create agent and application
	application := app.NewApplication(app.ApplicationConfig{
		DebounceTime: cfg.DebounceTime,
		Logger:       appLogger,
		Output:       wsOut,
	})

	// wire up streams

	// Wire HTTP ingest stream
	httpStream := stream.NewHTTPIngestStream(256)
	application.AddStream(httpStream)

	// Wire outbound WebSocket sources (dial upstream log feeds).
	for _, src := range cfg.Sources.WebSockets {
		wsSrc := stream.NewWebSocketDialStream(ctx, stream.WebSocketDialConfig{
			URL:              src.URL,
			Headers:          src.Headers,
			BufferSize:       src.BufferSize,
			HandshakeTimeout: src.HandshakeTimeout,
			ReconnectMin:     src.ReconnectMin,
			ReconnectMax:     src.ReconnectMax,
			PingInterval:     src.PingInterval,
			Logger: func(format string, args ...any) {
				appLogger.Printf(format, args...)
			},
		})
		application.AddStream(wsSrc)
		appLogger.Printf("websocket source %q dialing %s", src.Name, src.URL)
	}

	// Create HTTP server
	srv := http.NewServer(http.ServerConfig{
		Address:      cfg.ServerPort,
		Application:  application,
		IngestStream: httpStream,
		ResultsSink:  wsOut,
	})

	// Wait for interrupt signal in a goroutine, start server on main goroutine path
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Start the application event loop
	application.Start(ctx)

	go func() {
		appLogger.Printf("starting server on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-quit

	// Graceful shutdown: wait for in-flight work, then dispatcher, then server
	appLogger.Println("shutting down...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	application.Wait()
	_ = wsOut.Close()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("failed to shutdown: %v", err)
	}
}
