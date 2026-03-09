package app

import (
	"sync"
	"time"
)

type debouncer struct {
	mu       sync.Mutex
	lastFire time.Time
	cooldown time.Duration
}

func (d *debouncer) ShouldFire() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if time.Since(d.lastFire) < d.cooldown {
		return false
	}
	d.lastFire = time.Now()
	return true
}
