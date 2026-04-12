package stream

import (
	"sync"
	"testing"
	"time"
)

func TestMerge_BroadcastsToAll(t *testing.T) {
	src := newFakeStream(4)
	outs := Merge(src, 3)

	received := make([][]string, 3)
	var wg sync.WaitGroup
	for i, o := range outs {
		wg.Go(func() {
			for e := range o.Events() {
				received[i] = append(received[i], e.Message)
			}
		})
	}

	src.Write(Event{Message: "x"})
	src.Write(Event{Message: "y"})
	src.Close()
	wg.Wait()

	for i, r := range received {
		if len(r) != 2 || r[0] != "x" || r[1] != "y" {
			t.Errorf("consumer %d got %v, want [x y]", i, r)
		}
	}
}

func TestMerge_ClosingConsumerSkipsIt(t *testing.T) {
	src := newFakeStream(4)
	outs := Merge(src, 2)

	if err := outs[0].Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var received []string
	done := make(chan struct{})
	go func() {
		for e := range outs[1].Events() {
			received = append(received, e.Message)
		}
		close(done)
	}()

	src.Write(Event{Message: "a"})
	src.Write(Event{Message: "b"})
	src.Close()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("consumer 1 never drained — closed consumer may have blocked broadcaster")
	}

	if len(received) != 2 {
		t.Errorf("consumer 1 got %d events, want 2", len(received))
	}
}

func TestMerge_SourceCloseClosesAll(t *testing.T) {
	src := newFakeStream(1)
	outs := Merge(src, 2)

	src.Close()

	for i, o := range outs {
		select {
		case _, ok := <-o.Events():
			if ok {
				t.Errorf("output %d: expected closed channel", i)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("output %d: channel not closed", i)
		}
	}
}

func TestMerge_ZeroConsumers(t *testing.T) {
	src := newFakeStream(2)
	outs := Merge(src, 0)
	if len(outs) != 0 {
		t.Errorf("expected 0 outputs, got %d", len(outs))
	}
	src.Write(Event{Message: "a"})
	src.Close()
}
