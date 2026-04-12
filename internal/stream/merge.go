package stream

import "sync"

// Merge broadcasts every event from src to n downstream streams. Each
// downstream receives every event. When src's channel closes, all downstream
// channels close. Back-pressure from any slow consumer blocks the source.
// Calling Close on a downstream stream causes future events to skip it.
func Merge(src ReadStream, n int) []ReadStream {
	outs := make([]*merged, n)
	for i := range n {
		outs[i] = &merged{
			ch:   make(chan Event, defaultBufferSize),
			done: make(chan struct{}),
		}
	}
	go func() {
		for e := range src.Events() {
			for _, o := range outs {
				select {
				case o.ch <- e:
				case <-o.done:
				}
			}
		}
		for _, o := range outs {
			o.closeCh()
		}
	}()
	result := make([]ReadStream, n)
	for i, o := range outs {
		result[i] = o
	}
	return result
}

type merged struct {
	ch       chan Event
	done     chan struct{}
	doneOnce sync.Once
	chOnce   sync.Once
}

func (m *merged) Events() <-chan Event { return m.ch }

// Close signals that this consumer no longer wants events. The broadcaster
// will skip it on future writes. The channel itself is closed when the
// source stream closes.
func (m *merged) Close() error {
	m.doneOnce.Do(func() { close(m.done) })
	return nil
}

func (m *merged) closeCh() {
	m.chOnce.Do(func() { close(m.ch) })
}
