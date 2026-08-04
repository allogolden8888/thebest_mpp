package core

import (
	"testing"
	"time"
)

func TestTryAcquireAllowsUpToCapacity(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(2, 1, now)
	if !b.TryAcquire(now) {
		t.Fatalf("ожидали Permit")
	}
	if !b.TryAcquire(now) {
		t.Fatalf("ожидали Permit")
	}
	if b.TryAcquire(now) {
		t.Fatalf("ожидали Wait после исчерпания ёмкости")
	}
}

func TestTryAcquireRefillsOverTime(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(1, 1, now)
	if !b.TryAcquire(now) {
		t.Fatalf("ожидали Permit")
	}
	if b.TryAcquire(now.Add(500 * time.Millisecond)) {
		t.Fatalf("ожидали Wait — прошло только 0.5с при refill=1/сек")
	}
	if !b.TryAcquire(now.Add(1 * time.Second)) {
		t.Fatalf("ожидали Permit через 1с")
	}
}
