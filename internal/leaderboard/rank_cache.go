package leaderboard

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"sync"
	"time"
)

// RankEntry is a single ordered entry in the in-memory rank cache.
type RankEntry struct {
	OwnerID  string
	Score    int64
	Subscore int64
}

// RankCache holds ordered ranks for one leaderboard+expiry partition using a skip list.
type RankCache struct {
	mu      sync.RWMutex
	sortAsc bool
	enabled bool
	sl      *SkipListRankCache
}

type leaderboardExpiryKey struct {
	LeaderboardID string
	ExpiryUnix    int64
}

// GlobalRankCache manages per-partition rank caches with incremental updates.
type GlobalRankCache struct {
	mu    sync.RWMutex
	cache map[leaderboardExpiryKey]*RankCache
}

// SharedRankCache is the process-wide rank cache instance.
var SharedRankCache = &GlobalRankCache{
	cache: make(map[leaderboardExpiryKey]*RankCache),
}

func (g *GlobalRankCache) getOrCreate(leaderboardID string, expiryUnix int64, sortOrder int, enableRanks bool) *RankCache {
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.Lock()
	defer g.mu.Unlock()
	if rc, ok := g.cache[key]; ok {
		return rc
	}
	rc := &RankCache{
		sortAsc:  sortOrder == SortOrderAscending,
		enabled:  enableRanks,
		sl:       newSkipListRankCache(sortOrder == SortOrderAscending, enableRanks),
	}
	g.cache[key] = rc
	return rc
}

// Insert upserts a score and returns the new 1-based rank (0 if ranks disabled).
func (g *GlobalRankCache) Insert(leaderboardID string, sortOrder int, score, subscore int64, expiryUnix int64, ownerID string, enableRanks bool) int64 {
	if IsRankCacheBlacklisted(leaderboardID) {
		return 0
	}
	rc := g.getOrCreate(leaderboardID, expiryUnix, sortOrder, enableRanks)
	if !enableRanks {
		rc.enabled = false
		rc.sl.enabled = false
		return 0
	}
	rc.enabled = true
	rc.sl.enabled = true
	return rc.sl.Insert(RankEntry{OwnerID: ownerID, Score: score, Subscore: subscore})
}

// GetRank returns 1-based rank for owner, or 0 if disabled/missing.
func (g *GlobalRankCache) GetRank(leaderboardID string, expiryUnix int64, ownerID string) int64 {
	if IsRankCacheBlacklisted(leaderboardID) {
		return 0
	}
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.RLock()
	rc, ok := g.cache[key]
	g.mu.RUnlock()
	if !ok || !rc.enabled {
		return 0
	}
	return rc.sl.GetRank(ownerID)
}

// Delete removes an owner from the rank cache.
func (g *GlobalRankCache) Delete(leaderboardID string, expiryUnix int64, ownerID string) {
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.RLock()
	rc, ok := g.cache[key]
	g.mu.RUnlock()
	if !ok {
		return
	}
	rc.sl.Delete(ownerID)
}

// DeleteLeaderboard removes all partitions for a leaderboard.
func (g *GlobalRankCache) DeleteLeaderboard(leaderboardID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.cache {
		if k.LeaderboardID == leaderboardID {
			delete(g.cache, k)
		}
	}
}

// EvictPartition removes one expiry partition.
func (g *GlobalRankCache) EvictPartition(leaderboardID string, expiryUnix int64) {
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.Lock()
	delete(g.cache, key)
	g.mu.Unlock()
}

// TrimExpired removes partitions with expiryUnix <= nowUnix (non-zero expiries only).
func (g *GlobalRankCache) TrimExpired(nowUnix int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.cache {
		if k.ExpiryUnix > 0 && k.ExpiryUnix <= nowUnix {
			delete(g.cache, k)
		}
	}
}

// FillRanks assigns Rank fields on records using the cache when available.
func (g *GlobalRankCache) FillRanks(leaderboardID string, expiryUnix int64, records []*LeaderboardRecord, enableRanks bool) {
	if !enableRanks {
		for _, r := range records {
			r.Rank = 0
		}
		return
	}
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.RLock()
	rc, ok := g.cache[key]
	g.mu.RUnlock()
	if !ok {
		return
	}
	for _, r := range records {
		r.Rank = rc.sl.GetRank(r.OwnerID)
	}
}

// LoadFromRecords seeds the cache from a full sorted record slice.
func (g *GlobalRankCache) LoadFromRecords(leaderboardID string, expiryUnix int64, sortOrder int, enableRanks bool, records []*LeaderboardRecord) {
	rc := g.getOrCreate(leaderboardID, expiryUnix, sortOrder, enableRanks)
	rc.sl = newSkipListRankCache(sortOrder == SortOrderAscending, enableRanks)
	rc.enabled = enableRanks
	for i, r := range records {
		rc.sl.Insert(RankEntry{OwnerID: r.OwnerID, Score: r.Score, Subscore: r.Subscore})
		if enableRanks {
			r.Rank = int64(i + 1)
		}
	}
}

// GetDataByRank returns owner/score at 1-based rank for a partition.
func (g *GlobalRankCache) GetDataByRank(leaderboardID string, expiryUnix, rank int64) (ownerID string, score, subscore int64, ok bool) {
	key := leaderboardExpiryKey{LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix}
	g.mu.RLock()
	rc, found := g.cache[key]
	g.mu.RUnlock()
	if !found || !rc.enabled {
		return "", 0, 0, false
	}
	e, ok := rc.sl.GetDataByRank(rank)
	if !ok {
		return "", 0, 0, false
	}
	return e.OwnerID, e.Score, e.Subscore, true
}

// RecordListCursor encodes keyset pagination state.
type RecordListCursor struct {
	IsNext        bool
	LeaderboardID string
	ExpiryUnix    int64
	Score         int64
	Subscore      int64
	OwnerID       string
	Rank          int64
}

// EncodeCursor serializes a pagination cursor to a URL-safe string.
func EncodeCursor(c *RecordListCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

// DecodeCursor deserializes a pagination cursor.
func DecodeCursor(s string) (*RecordListCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c RecordListCursor
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// StartRankCacheTrimmer runs hourly expiry trim in the background.
func StartRankCacheTrimmer(stop <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Hour)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case t := <-ticker.C:
				SharedRankCache.TrimExpired(t.Unix())
			}
		}
	}()
}
