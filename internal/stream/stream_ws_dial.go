package stream

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketDialConfig configures an outbound WebSocket log source.
type WebSocketDialConfig struct {
	URL              string
	Headers          map[string]string
	BufferSize       int
	HandshakeTimeout time.Duration
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
	PingInterval     time.Duration

	// Logger is optional; if set, non-fatal errors (dial failures, read errors)
	// are logged through it.
	Logger func(format string, args ...any)

	// dialer allows tests to inject a custom dialer.
	dialer *websocket.Dialer
}

func (c WebSocketDialConfig) withDefaults() WebSocketDialConfig {
	if c.BufferSize <= 0 {
		c.BufferSize = 256
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.ReconnectMin <= 0 {
		c.ReconnectMin = 1 * time.Second
	}
	if c.ReconnectMax <= 0 {
		c.ReconnectMax = 30 * time.Second
	}
	if c.ReconnectMax < c.ReconnectMin {
		c.ReconnectMax = c.ReconnectMin
	}
	if c.PingInterval == 0 {
		c.PingInterval = 30 * time.Second
	}
	return c
}

// WebSocketDialStream is a client-side WebSocket source. It dials the
// configured URL, reads JSON-encoded Events, and reconnects with exponential
// backoff on failure. It satisfies ReadStream.
type WebSocketDialStream struct {
	cfg       WebSocketDialConfig
	ch        chan Event
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	chOnce    sync.Once
	done      chan struct{}

	connMu sync.Mutex
	conn   *websocket.Conn
	closed atomic.Bool
}

// NewWebSocketDialStream creates a WebSocketDialStream and starts its supervisor.
// The supervisor exits when ctx is canceled or Close is called.
func NewWebSocketDialStream(ctx context.Context, cfg WebSocketDialConfig) *WebSocketDialStream {
	cfg = cfg.withDefaults()
	sctx, cancel := context.WithCancel(ctx)
	s := &WebSocketDialStream{
		cfg:    cfg,
		ch:     make(chan Event, cfg.BufferSize),
		ctx:    sctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go s.supervise()
	return s
}

// Events returns the read-only event channel.
func (s *WebSocketDialStream) Events() <-chan Event {
	return s.ch
}

// Close cancels the supervisor and closes the underlying connection. The
// events channel is closed by the supervisor on exit.
func (s *WebSocketDialStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.connMu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.connMu.Unlock()
	})
	<-s.done
	return nil
}

func (s *WebSocketDialStream) logf(format string, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger(format, args...)
	}
}

func (s *WebSocketDialStream) supervise() {
	defer close(s.done)
	defer s.chOnce.Do(func() { close(s.ch) })

	backoff := s.cfg.ReconnectMin
	for {
		if s.ctx.Err() != nil {
			return
		}

		conn, err := s.dial()
		if err != nil {
			s.logf("websocket dial %s failed: %v", s.cfg.URL, err)
			if !s.sleepBackoff(backoff) {
				return
			}
			backoff = nextBackoff(backoff, s.cfg.ReconnectMax)
			continue
		}

		// Reset backoff on successful connection.
		backoff = s.cfg.ReconnectMin

		s.connMu.Lock()
		s.conn = conn
		s.connMu.Unlock()

		s.runConn(conn)

		s.connMu.Lock()
		s.conn = nil
		s.connMu.Unlock()

		if s.ctx.Err() != nil {
			return
		}
	}
}

func (s *WebSocketDialStream) dial() (*websocket.Conn, error) {
	d := s.cfg.dialer
	if d == nil {
		d = &websocket.Dialer{HandshakeTimeout: s.cfg.HandshakeTimeout}
	}
	hdr := http.Header{}
	for k, v := range s.cfg.Headers {
		hdr.Set(k, v)
	}
	conn, _, err := d.DialContext(s.ctx, s.cfg.URL, hdr)
	return conn, err
}

// runConn runs the read loop (and optional ping loop) for a single connection.
// It returns when the connection fails or the context is canceled.
func (s *WebSocketDialStream) runConn(conn *websocket.Conn) {
	pingDone := make(chan struct{})
	var pingWg sync.WaitGroup

	if s.cfg.PingInterval > 0 {
		// Treat pong as a liveness signal: refresh read deadline.
		pongWait := 2 * s.cfg.PingInterval
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		pingWg.Go(func() {
			t := time.NewTicker(s.cfg.PingInterval)
			defer t.Stop()
			for {
				select {
				case <-pingDone:
					return
				case <-s.ctx.Done():
					return
				case <-t.C:
					_ = conn.WriteControl(
						websocket.PingMessage,
						nil,
						time.Now().Add(s.cfg.PingInterval),
					)
				}
			}
		})
	}

	// Goroutine: if context cancels while blocked in ReadMessage, close the
	// conn so the read unblocks.
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-s.ctx.Done():
			_ = conn.Close()
		case <-watchDone:
		}
	}()

	err := readEventsInto(conn, s.ch)
	close(watchDone)
	close(pingDone)
	pingWg.Wait()
	_ = conn.Close()

	if err != nil && !errors.Is(err, context.Canceled) && s.ctx.Err() == nil {
		s.logf("websocket %s read error: %v", s.cfg.URL, err)
	}
}

// sleepBackoff waits for d or until the context is canceled. Returns false if
// canceled.
func (s *WebSocketDialStream) sleepBackoff(d time.Duration) bool {
	// Full jitter.
	j := time.Duration(rand.Int63n(int64(d) + 1))
	t := time.NewTimer(j)
	defer t.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
