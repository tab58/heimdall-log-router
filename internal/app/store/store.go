package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

const defaultRingSize = 1000

// LogStore is an in-memory ring-buffer of Events.
type LogStore struct {
	mu       sync.RWMutex
	ring     []stream.Event
	ringSize int
}

// New creates a LogStore with the default ring buffer size.
func New() *LogStore {
	return &LogStore{
		ring:     make([]stream.Event, 0, defaultRingSize),
		ringSize: defaultRingSize,
	}
}

// Append adds an event to the ring buffer, evicting the oldest if full.
func (s *LogStore) Append(e stream.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ring) >= s.ringSize {
		s.ring = s.ring[1:]
	}
	s.ring = append(s.ring, e)
}

// RecentContext returns the most recent windowSize events in chronological order.
func (s *LogStore) RecentContext(windowSize int) []stream.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if windowSize > len(s.ring) {
		windowSize = len(s.ring)
	}

	result := make([]stream.Event, windowSize)
	copy(result, s.ring[len(s.ring)-windowSize:])
	return result
}

// Snapshot returns a copy of the most recent n events in chronological order
// along with a content-hash ID (first 12 hex chars of SHA-256 over the
// serialized batch). Identical batches yield identical IDs.
func (s *LogStore) Snapshot(n int) ([]stream.Event, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if n > len(s.ring) {
		n = len(s.ring)
	}
	events := make([]stream.Event, n)
	copy(events, s.ring[len(s.ring)-n:])

	h := sha256.New()
	for _, e := range events {
		fmt.Fprintf(h, "%d|%s|%s|%s\n", e.Timestamp.UnixNano(), e.Severity, e.Service, e.Message)
	}
	id := hex.EncodeToString(h.Sum(nil))[:12]
	return events, id
}

// Search returns up to limit events whose message contains query (case-insensitive),
// scanning from newest to oldest.
func (s *LogStore) Search(query string, limit int) []stream.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lower := strings.ToLower(query)
	var results []stream.Event
	for i := len(s.ring) - 1; i >= 0 && len(results) < limit; i-- {
		if strings.Contains(strings.ToLower(s.ring[i].Message), lower) {
			results = append(results, s.ring[i])
		}
	}
	return results
}
