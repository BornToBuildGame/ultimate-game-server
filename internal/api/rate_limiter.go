package api

import (
	"math"
	"sync"
	"time"
)

// TokenBucket implements a rate limiter using the Token Bucket algorithm.
type TokenBucket struct {
	tokens         float64
	maxTokens      float64
	refillRate     float64 // tokens per second
	lastRefillTime time.Time
	mu             sync.Mutex
}

// NewTokenBucket creates a new TokenBucket rate limiter.
func NewTokenBucket(maxTokens, refillRate float64) *TokenBucket {
	return &TokenBucket{
		tokens:         maxTokens,
		maxTokens:      maxTokens,
		refillRate:     refillRate,
		lastRefillTime: time.Now(),
	}
}

// Allow checks if a request is allowed, refilling tokens and consuming one if available.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefillTime).Seconds()
	tb.lastRefillTime = now

	// Refill tokens
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+(elapsed*tb.refillRate))

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}

	return false
}

// IPTokenBucketRateLimiter manages a pool of IP-based rate limiters.
type IPTokenBucketRateLimiter struct {
	mu         sync.RWMutex
	limiters   map[string]*TokenBucket
	maxTokens  float64
	refillRate float64
}

// NewIPRateLimiter creates a new IPRateLimiter.
func NewIPRateLimiter(maxTokens, refillRate float64) *IPTokenBucketRateLimiter {
	return &IPTokenBucketRateLimiter{
		limiters:   make(map[string]*TokenBucket),
		maxTokens:  maxTokens,
		refillRate: refillRate,
	}
}

// GetLimiter retrieves or creates a rate limiter for a specific key (e.g. IP address or userID).
func (lim *IPTokenBucketRateLimiter) GetLimiter(key string) *TokenBucket {
	lim.mu.RLock()
	tb, exists := lim.limiters[key]
	lim.mu.RUnlock()

	if exists {
		return tb
	}

	lim.mu.Lock()
	// Double check existence
	tb, exists = lim.limiters[key]
	if !exists {
		tb = NewTokenBucket(lim.maxTokens, lim.refillRate)
		lim.limiters[key] = tb
	}
	lim.mu.Unlock()

	return tb
}

// Allow checks if a request from a specific key is allowed.
func (lim *IPTokenBucketRateLimiter) Allow(key string) bool {
	return lim.GetLimiter(key).Allow()
}
