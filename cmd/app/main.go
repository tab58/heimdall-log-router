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

	"github.com/tab58/tenzing-agent-harness/pkg/tenzing"

	"github.com/tab58/heimdall-log-router/api/http"
	"github.com/tab58/heimdall-log-router/cmd/app/config"
	"github.com/tab58/heimdall-log-router/internal/app"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

const (
	shutdownTimeout   = 10 * time.Second
	defaultVectorPath = "/tmp/heimdall/vector.yaml"
	vectorPathEnvVar  = "HEIMDALL_VECTOR_CONFIG_PATH"
	// workspaceDirEnvVar, when set, becomes the process working directory so
	// the harness's Read/Glob/Grep tools resolve against mounted source.
	// Unset for local dev outside Docker.
	workspaceDirEnvVar = "HEIMDALL_WORKSPACE_DIR"
)

// modelDefinition builds the tenzing model definition for the configured
// provider and model id. Limits are conservative defaults for models not in
// the tenzing catalog (heimdall accepts arbitrary model ids via config).
func modelDefinition(providerName, model string) tenzing.ModelDefinition {
	if providerName == config.ProviderOllama {
		return tenzing.ModelDefinition{
			Provider:             tenzing.ProviderOllama,
			Name:                 model,
			MaxTokens:            32_768,
			ContextWindowSize:    262_144,
			DefaultContextWindow: 32_768,
		}
	}
	return tenzing.ModelDefinition{
		Provider:          tenzing.ProviderAnthropic,
		Name:              model,
		MaxTokens:         64_000,
		ContextWindowSize: 200_000,
	}
}

var cfg *config.Config

func init() {
	var err error
	cfg, err = config.Load()
	if err != nil {
		fmt.Printf("failed to load config: %v", err)
		os.Exit(1)
	}

	// If heimdall.yaml embeds a vector: sub-tree, materialize it to disk so
	// a sidecar vector process can launch against it. Skipped silently when
	// no vector: block is present (local dev, tests).
	if cfg != nil && cfg.Vector.Kind != 0 {
		path := os.Getenv(vectorPathEnvVar)
		if path == "" {
			path = defaultVectorPath
		}
		if err := cfg.WriteVectorConfig(path); err != nil {
			fmt.Printf("failed to write vector config: %v", err)
			os.Exit(1)
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	appLogger := log.New(os.Stderr, "[heimdall] ", log.LstdFlags)

	// Monitor WS — single-client sink for batch / status / delta frames
	// and source of decide frames from the web UI served at GET /.
	monitor := stream.NewMonitorWS(func(format string, args ...any) {
		appLogger.Printf(format, args...)
	})

	if cfg.APIKey == "" {
		envVar := "ANTHROPIC_API_KEY"
		if cfg.Provider == config.ProviderOllama {
			envVar = "OLLAMA_API_KEY"
		}
		log.Fatalf("%s is required — set it in heimdall.yaml (api_key) or the environment", envVar)
	}

	// The harness scopes its Read/Glob/Grep tools to the process working
	// directory, so switch to the mounted source tree before any analysis
	// runs. Config is already loaded (init), vector config path is absolute.
	if dir := os.Getenv(workspaceDirEnvVar); dir != "" {
		if err := os.Chdir(dir); err != nil {
			log.Fatalf("chdir to %s %q: %v", workspaceDirEnvVar, dir, err)
		}
	}

	model := modelDefinition(cfg.Provider, cfg.Model)
	// Feed the configured api_key (yaml wins over env) into every LLM the
	// harness builds; BaseURL defaults per provider (Ollama → Ollama Cloud).
	llmFactory := func(md tenzing.ModelDefinition) (tenzing.LLM, error) {
		return tenzing.LLMFromModel(cfg.APIKey, md)
	}
	appLogger.Printf("analyzer: provider=%s model=%s", cfg.Provider, cfg.Model)

	application := app.NewApplication(app.ApplicationConfig{
		BatchDebounce:  cfg.BatchDebounce,
		BatchSize:      cfg.BatchSize,
		QueueMax:       cfg.QueueMax,
		Logger:         appLogger,
		Monitor:        monitor,
		Model:          model,
		LLMFactory:     llmFactory,
		CodeSearchDirs: cfg.CodeSearchDirs,
	})

	// Wire the decide + connect handlers now that we have both sides.
	monitor.SetDecideHandler(func(batchID string, action stream.DecideAction) {
		if err := application.Decide(batchID, action); err != nil {
			appLogger.Printf("decide %s %s: %v", batchID, action, err)
			_ = monitor.SendError(err.Error())
		}
	})
	monitor.SetResetHandler(application.Reset)
	monitor.SetConnectHandler(application.ReplayQueue)

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

	srv := http.NewServer(http.ServerConfig{
		Address:      cfg.ServerPort,
		MonitorPath:  cfg.MonitorPath,
		Application:  application,
		IngestStream: httpStream,
		Monitor:      monitor,
	})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	application.Start(ctx)

	go func() {
		appLogger.Printf("starting server on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-quit

	appLogger.Println("shutting down... (Ctrl+C again to force quit)")
	// The graceful path below can still take a few seconds (in-flight
	// analysis unwinding, HTTP drain). A second signal must never be
	// swallowed — bail out hard instead of leaving a zombie on the port.
	go func() {
		<-quit
		appLogger.Println("force quit")
		os.Exit(130)
	}()
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	application.Wait()
	_ = monitor.Close()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("failed to shutdown: %v", err)
	}
}
