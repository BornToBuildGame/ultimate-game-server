package leaderboard

import (
	"sync"
	"time"
)

// ConfigCache is an in-memory cache of leaderboard configurations.
type ConfigCache struct {
	mu    sync.RWMutex
	byID  map[string]*Leaderboard
	loaded bool
}

// SharedConfigCache is the process-wide leaderboard config cache.
var SharedConfigCache = &ConfigCache{
	byID: make(map[string]*Leaderboard),
}

// Get returns a cached leaderboard by ID (nil if missing).
func (c *ConfigCache) Get(id string) *Leaderboard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	lb := c.byID[id]
	if lb == nil {
		return nil
	}
	cp := *lb
	return &cp
}

// Put inserts or replaces a leaderboard config.
func (c *ConfigCache) Put(lb *Leaderboard) {
	if lb == nil {
		return
	}
	cp := *lb
	c.mu.Lock()
	c.byID[lb.ID] = &cp
	c.mu.Unlock()
}

// Delete removes a leaderboard from the cache.
func (c *ConfigCache) Delete(id string) {
	c.mu.Lock()
	delete(c.byID, id)
	c.mu.Unlock()
}

// ListAll returns all cached configs (copy).
func (c *ConfigCache) ListAll() []*Leaderboard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Leaderboard, 0, len(c.byID))
	for _, lb := range c.byID {
		cp := *lb
		out = append(out, &cp)
	}
	return out
}

// ListLeaderboardsCached returns non-tournament leaderboards from cache.
func (c *ConfigCache) ListLeaderboardsCached() []*Leaderboard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Leaderboard, 0)
	for _, lb := range c.byID {
		if lb.IsTournament() {
			continue
		}
		cp := *lb
		out = append(out, &cp)
	}
	return out
}

// ListTournamentsCached returns tournament configs from cache.
func (c *ConfigCache) ListTournamentsCached() []*Leaderboard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Leaderboard, 0)
	for _, lb := range c.byID {
		if !lb.IsTournament() {
			continue
		}
		cp := *lb
		out = append(out, &cp)
	}
	return out
}

// LoadAll loads all leaderboard rows from the database into the cache.
func (c *ConfigCache) LoadAll(lbs []*Leaderboard) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byID = make(map[string]*Leaderboard, len(lbs))
	for _, lb := range lbs {
		if lb == nil {
			continue
		}
		cp := *lb
		c.byID[lb.ID] = &cp
	}
	c.loaded = true
}

// InvalidateAge is unused placeholder for future TTL policies.
func (c *ConfigCache) InvalidateAge(_ time.Duration) {}
