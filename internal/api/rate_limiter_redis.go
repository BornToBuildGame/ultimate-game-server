package api

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter is implemented by in-memory and Redis-backed API rate limiters.
type RateLimiter interface {
	MaxTokens() float64
	AllowWithInfo(key string) (allowed bool, remaining float64, resetAt time.Time)
}
// Falls back to in-memory token bucket when Redis is unavailable.
type RedisIPRateLimiter struct {
	rdb        *redis.Client
	fallback   *IPTokenBucketRateLimiter
	window     time.Duration
	maxTokens  float64
	prefix     string
	localCache sync.Map // key -> *TokenBucket for fallback path
}

// NewRedisIPRateLimiter creates a cluster-aware rate limiter with local fallback.
func NewRedisIPRateLimiter(rdb *redis.Client, maxTokens, refillRate float64, window time.Duration) *RedisIPRateLimiter {
	if window <= 0 {
		window = time.Minute
	}
	return &RedisIPRateLimiter{
		rdb:       rdb,
		fallback:  NewIPRateLimiter(maxTokens, refillRate),
		window:    window,
		maxTokens: maxTokens,
		prefix:    "uge:ratelimit:",
	}
}

// MaxTokens returns configured bucket capacity.
func (lim *RedisIPRateLimiter) MaxTokens() float64 {
	return lim.maxTokens
}

// Allow checks if a request is allowed for the key.
func (lim *RedisIPRateLimiter) Allow(key string) bool {
	allowed, _, _ := lim.AllowWithInfo(key)
	return allowed
}

// RedisIPRateLimiter applies a fixed-window counter per key using Redis INCR.
// AllowWithInfo returns allowance, remaining tokens, and reset time.
func (lim *RedisIPRateLimiter) AllowWithInfo(key string) (bool, float64, time.Time) {
	if lim.rdb == nil {
		tb := lim.fallback.GetLimiter(key)
		return tb.AllowWithInfo()
	}
	ctx := context.Background()
	redisKey := lim.prefix + key
	count, err := lim.rdb.Incr(ctx, redisKey).Result()
	if err != nil {
		tb := lim.fallback.GetLimiter(key)
		return tb.AllowWithInfo()
	}
	if count == 1 {
		_ = lim.rdb.Expire(ctx, redisKey, lim.window).Err()
	}
	ttl, _ := lim.rdb.TTL(ctx, redisKey).Result()
	resetAt := time.Now().Add(ttl)
	if ttl <= 0 {
		resetAt = time.Now().Add(lim.window)
	}
	remaining := lim.maxTokens - float64(count)
	if remaining < 0 {
		remaining = 0
	}
	if float64(count) > lim.maxTokens {
		return false, remaining, resetAt
	}
	return true, remaining, resetAt
}
