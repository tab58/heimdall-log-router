package stream

import "sync"

const defaultBufferSize = 64

// Combine merges events from many streams into one. The returned stream's
// event channel closes only after every input stream's channel has closed
// (or Close is called). Close propagates to each input stream, unblocks any
// workers stuck forwarding into the output, and returns the first error.
func Combine(streams ...ReadStream) ReadStream {
	out := make(chan Event, defaultBufferSize)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range streams {
		wg.Go(func() {
			for e := range s.Events() {
				select {
				case out <- e:
				case <-done:
					return
				}
			}
		})
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return &combined{out: out, done: done, sources: streams}
}

type combined struct {
	out       chan Event
	done      chan struct{}
	sources   []ReadStream
	closeOnce sync.Once
}

func (c *combined) Events() <-chan Event { return c.out }

func (c *combined) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	var firstErr error
	for _, s := range c.sources {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
