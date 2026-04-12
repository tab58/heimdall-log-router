package app

import "sync"

// inflight tracks analysis IDs currently being processed. It prevents the
// same log batch from being sent to the agent twice concurrently. The zero
// value is ready to use.
type inflight struct {
	mu  sync.Mutex
	set map[string]struct{}
}

// tryAcquire returns true if id was not already present. The caller owns
// the slot and must release it.
func (i *inflight) tryAcquire(id string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.set == nil {
		i.set = make(map[string]struct{})
	}
	if _, ok := i.set[id]; ok {
		return false
	}
	i.set[id] = struct{}{}
	return true
}

// release removes id from the set.
func (i *inflight) release(id string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.set, id)
}
