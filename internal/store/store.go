package store

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"log_source"`
	Level     string    `json:"level"`
	Message  string    `json:"message"`
	Service  string    `json:"service"`
}

type LogStore struct {
	filePath string
	mu       sync.RWMutex
	ring     []LogEntry
	ringSize int
}

func New(filePath string) *LogStore {
	return &LogStore{
		filePath: filePath,
		ring:     make([]LogEntry, 0, 1000),
		ringSize: 1000,
	}
}

func (s *LogStore) Append(entry LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ring) >= s.ringSize {
		s.ring = s.ring[1:]
	}
	s.ring = append(s.ring, entry)
}

func (s *LogStore) RecentContext(windowSize int) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if windowSize > len(s.ring) {
		windowSize = len(s.ring)
	}

	result := make([]LogEntry, windowSize)
	copy(result, s.ring[len(s.ring)-windowSize:])
	return result
}

func (s *LogStore) RecentContextBySources(windowSize int, sources []string) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sourceSet := make(map[string]bool, len(sources))
	for _, src := range sources {
		sourceSet[src] = true
	}

	var result []LogEntry
	for i := len(s.ring) - 1; i >= 0 && len(result) < windowSize; i-- {
		if sourceSet[s.ring[i].Source] {
			result = append(result, s.ring[i])
		}
	}

	// Reverse to chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func (s *LogStore) LoadFromFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if len(s.ring) >= s.ringSize {
			s.ring = s.ring[1:]
		}
		s.ring = append(s.ring, entry)
	}
	return scanner.Err()
}

func (s *LogStore) Search(query string, limit int) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lower := strings.ToLower(query)
	var results []LogEntry
	for i := len(s.ring) - 1; i >= 0 && len(results) < limit; i-- {
		if strings.Contains(strings.ToLower(s.ring[i].Message), lower) {
			results = append(results, s.ring[i])
		}
	}
	return results
}
