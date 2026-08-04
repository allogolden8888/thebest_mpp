// Package ratelimit — CODE_REVIEW.md MEDIUM finding: ни один
// партнёр-facing эндпоинт (/v1/messages/status, /v1/messages/search,
// /v1/reports) не был ограничен по частоте запросов — партнёр или
// утёкший токен мог забросать PostgreSQL/ClickHouse неограниченным
// объёмом нетривиальных фильтрованных запросов.
//
// Простой per-partner token bucket в памяти процесса — не распределённый
// (Redis) лимит, поэтому при нескольких репликах Partner API реальный
// лимит на партнёра масштабируется числом реплик; этого достаточно, чтобы
// закрыть "вообще без всякого лимита", но не заменяет полноценный
// distributed rate limit, если он когда-нибудь понадобится.
package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	ratePerSecond float64
	burst         float64
}

// New — ratePerSecond установившаяся скорость пополнения, burst —
// максимальный запас токенов (позволяет короткие всплески).
func New(ratePerSecond, burst float64) *Limiter {
	return &Limiter{buckets: make(map[string]*bucket), ratePerSecond: ratePerSecond, burst: burst}
}

// Allow — true, если запрос по ключу (partner_id) укладывается в лимит.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst - 1, lastRefill: now}
		l.buckets[key] = b
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * l.ratePerSecond
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
