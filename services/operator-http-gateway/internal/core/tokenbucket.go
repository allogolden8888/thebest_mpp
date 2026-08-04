// Package core — enforce_tps (service_internal_methods.md §1.3a): локальный
// счётчик, инвариант владеющего инстанса (не распределённая координация,
// см. services_specifictaion.md §2.3 про instance ownership).
package core

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu              sync.Mutex
	capacity        float64
	refillPerSecond float64
	tokens          float64
	lastRefill      time.Time
}

func NewTokenBucket(capacity, refillPerSecond float64, now time.Time) *TokenBucket {
	return &TokenBucket{capacity: capacity, refillPerSecond: refillPerSecond, tokens: capacity, lastRefill: now}
}

func (b *TokenBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens = min(b.capacity, b.tokens+elapsed*b.refillPerSecond)
	b.lastRefill = now
}

// TryAcquire — enforce_tps: Permit (true) | Wait (false).
func (b *TokenBucket) TryAcquire(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}
