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

// entry pairs an event with a monotonic sequence number. Sequences are stable
// across evictions so callers can pin a point in the log history even after
// older entries have been dropped from the ring.
type entry struct {
	seq uint64
	ev  stream.Event
}

// LogStore is an in-memory ring-buffer of Events.
type LogStore struct {
	mu       sync.RWMutex
	ring     []entry
	ringSize int
	nextSeq  uint64
}

// New creates a LogStore with the default ring buffer size.
func New() *LogStore {
	return &LogStore{
		ring:     make([]entry, 0, defaultRingSize),
		ringSize: defaultRingSize,
	}
}

// Append adds an event to the ring buffer, evicting the oldest if full.
// Returns the monotonic sequence number assigned to the event.
func (s *LogStore) Append(e stream.Event) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSeq++
	seq := s.nextSeq
	if len(s.ring) >= s.ringSize {
		s.ring = s.ring[1:]
	}
	s.ring = append(s.ring, entry{seq: seq, ev: e})
	return seq
}

// RecentContext returns the most recent windowSize events in chronological order.
func (s *LogStore) RecentContext(windowSize int) []stream.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if windowSize > len(s.ring) {
		windowSize = len(s.ring)
	}

	result := make([]stream.Event, windowSize)
	for i, en := range s.ring[len(s.ring)-windowSize:] {
		result[i] = en.ev
	}
	return result
}

// Snapshot returns a copy of the most recent n events in chronological order
// along with a content-hash ID (first 12 hex chars of SHA-256 over the
// serialized batch) and the highest sequence number contained in the batch.
// The sequence number is used later to truncate the ring up to that point.
func (s *LogStore) Snapshot(n int) ([]stream.Event, string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if n > len(s.ring) {
		n = len(s.ring)
	}
	events := make([]stream.Event, n)
	var lastSeq uint64
	window := s.ring[len(s.ring)-n:]
	for i, en := range window {
		events[i] = en.ev
		if en.seq > lastSeq {
			lastSeq = en.seq
		}
	}

	h := sha256.New()
	for _, e := range events {
		fmt.Fprintf(h, "%d|%s|%s|%s\n", e.Timestamp.UnixNano(), e.Severity, e.Service, e.Message)
	}
	id := hex.EncodeToString(h.Sum(nil))[:12]
	return events, id, lastSeq
}

// Clear drops every entry from the ring buffer. Sequence numbers are
// preserved so any outstanding references (e.g. snapshot IDs) remain
// monotonic across resets.
func (s *LogStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ring = s.ring[:0]
}

// Search returns up to limit events whose message contains query (case-insensitive),
// scanning from newest to oldest.
func (s *LogStore) Search(query string, limit int) []stream.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lower := strings.ToLower(query)
	var results []stream.Event
	for i := len(s.ring) - 1; i >= 0 && len(results) < limit; i-- {
		if strings.Contains(strings.ToLower(s.ring[i].ev.Message), lower) {
			results = append(results, s.ring[i].ev)
		}
	}
	return results
}
