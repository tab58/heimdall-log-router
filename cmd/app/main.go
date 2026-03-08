package app

import (
	"context"
	"fmt"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tbright/log-router/api/http"
	"github.com/tbright/log-router/internal/app"
	"golang.org/x/sync/errgroup"
)

func main() {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY not configured")
	}

	srv := http.NewServer(http.ServerConfig{
		Address:     ":8080",
		Application: app.NewApplication(),
	})

	// start server
	errgroup, _ := errgroup.WithContext(context.Background())
	errgroup.Go(func() error {
		fmt.Printf("starting server on port %s\n", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
			fmt.Printf("failed to start server: %v", err)
			os.Exit(1)
		}
		return nil
	})
	if err := errgroup.Wait(); err != nil {
		fmt.Printf("failed to start server: %v", err)
		os.Exit(1)
	}

	// wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// graceful shutdown
	fmt.Println("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("failed to shutdown server: %v", err)
		os.Exit(1)
	}
}
