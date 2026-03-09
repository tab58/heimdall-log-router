package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tbright/heimdall/internal/plugin"
)

// mockServer records Shutdown calls.
type mockServer struct {
	shutdownCalled bool
	shutdownErr    error
}

func (m *mockServer) Shutdown(_ context.Context) error {
	m.shutdownCalled = true
	return m.shutdownErr
}

// mockWaiter is a no-op Waiter for testing.
type mockWaiter struct {
	waitCalled bool
}

func (m *mockWaiter) Wait() {
	m.waitCalled = true
}

func TestGracefulShutdown(t *testing.T) {
	tests := []struct {
		name       string
		dispatcher *plugin.Dispatcher
		serverErr  error
		wantErr    bool
	}{
		{
			// Nil dispatcher should not panic — just shut down server.
			name:       "nil dispatcher",
			dispatcher: nil,
			wantErr:    false,
		},
		{
			// With dispatcher should call dispatcher.Shutdown and server.Shutdown.
			name:       "with dispatcher",
			dispatcher: &plugin.Dispatcher{},
			wantErr:    false,
		},
		{
			// Server shutdown error should be returned.
			name:       "server shutdown error",
			dispatcher: nil,
			serverErr:  errors.New("shutdown failed"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &mockServer{shutdownErr: tt.serverErr}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			waiter := &mockWaiter{}
			err := GracefulShutdown(ctx, waiter, tt.dispatcher, server)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !server.shutdownCalled {
				t.Error("server.Shutdown was not called")
			}
		})
	}
}
