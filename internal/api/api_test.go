package api

import (
	"testing"
	"time"
)

func TestTokenBucket_Consumption(t *testing.T) {
	// Limiter that starts with 3 tokens, refills at 1 token/sec
	tb := NewTokenBucket(3, 1)

	// Consume 3 tokens
	if !tb.Allow() {
		t.Error("expected first token consumption to succeed")
	}
	if !tb.Allow() {
		t.Error("expected second token consumption to succeed")
	}
	if !tb.Allow() {
		t.Error("expected third token consumption to succeed")
	}

	// 4th should fail
	if tb.Allow() {
		t.Error("expected fourth token consumption to be blocked")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	// Limiter that starts with 1 token, refills at 10 tokens/sec
	tb := NewTokenBucket(1, 10)

	// Consume the initial token
	if !tb.Allow() {
		t.Error("expected initial token to be allowed")
	}
	if tb.Allow() {
		t.Error("expected immediate subsequent request to be blocked")
	}

	// Sleep 150ms to allow refill of 1.5 tokens
	time.Sleep(150 * time.Millisecond)

	if !tb.Allow() {
		t.Error("expected token to be allowed after refill sleep")
	}
}

func TestIPRateLimiter_Pool(t *testing.T) {
	limiter := NewIPRateLimiter(2, 1)

	ipA := "192.168.1.1"
	ipB := "192.168.1.2"

	// IP A consumes 2
	if !limiter.Allow(ipA) || !limiter.Allow(ipA) {
		t.Error("expected IP A to be allowed 2 requests")
	}
	if limiter.Allow(ipA) {
		t.Error("expected IP A to be blocked on 3rd request")
	}

	// IP B should still be allowed since pool is isolated
	if !limiter.Allow(ipB) {
		t.Error("expected IP B request to be allowed independently")
	}
}
