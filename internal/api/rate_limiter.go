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

func (tb *TokenBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(tb.lastRefillTime).Seconds()
	tb.lastRefillTime = now
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+(elapsed*tb.refillRate))
}

// Allow checks if a request is allowed, refilling tokens and consuming one if available.
func (tb *TokenBucket) Allow() bool {
	allowed, _, _ := tb.AllowWithInfo()
	return allowed
}

// AllowWithInfo consumes one token when available and returns remaining tokens and reset time.
// Reset is when the bucket is expected to regain at least one token if currently empty.
func (tb *TokenBucket) AllowWithInfo() (allowed bool, remaining float64, resetAt time.Time) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.refillLocked(now)

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		remaining = tb.tokens
		if tb.refillRate > 0 {
			needed := tb.maxTokens - tb.tokens
			resetAt = now.Add(time.Duration(needed/tb.refillRate) * time.Second)
		} else {
			resetAt = now
		}
		return true, remaining, resetAt
	}

	remaining = tb.tokens
	if tb.refillRate > 0 {
		wait := (1.0 - tb.tokens) / tb.refillRate
		resetAt = now.Add(time.Duration(math.Ceil(wait)) * time.Second)
	} else {
		resetAt = now.Add(time.Second)
	}
	return false, remaining, resetAt
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

// MaxTokens returns the configured bucket capacity.
func (lim *IPTokenBucketRateLimiter) MaxTokens() float64 {
	return lim.maxTokens
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
