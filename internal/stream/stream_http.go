package stream

// HTTPIngestStream bridges HTTP POST handlers to the event pipeline.
// The HTTP handler calls Write; the application consumes from Events().
type HTTPIngestStream struct {
	ch chan Event
}

// NewHTTPIngestStream creates a buffered HTTPIngestStream.
func NewHTTPIngestStream(bufSize int) *HTTPIngestStream {
	return &HTTPIngestStream{ch: make(chan Event, bufSize)}
}

// Write delivers an event into the stream. Blocks if the buffer is full.
func (s *HTTPIngestStream) Write(e Event) {
	s.ch <- e
}

// Events returns the read-only event channel.
func (s *HTTPIngestStream) Events() <-chan Event {
	return s.ch
}

// Close signals no more events will be written.
func (s *HTTPIngestStream) Close() error {
	close(s.ch)
	return nil
}
