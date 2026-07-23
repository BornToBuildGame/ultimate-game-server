package leaderboard

import (
	"os"
	"strings"
	"sync"
)

var (
	rankBlacklistMu sync.RWMutex
	rankBlacklist   = map[string]struct{}{}
	rankBlacklistAll bool
)

func init() {
	ConfigureRankCacheBlacklist(os.Getenv("UGE_BLACKLIST_RANK_CACHE"))
}

// ConfigureRankCacheBlacklist configures blacklist from a comma-separated list.
// Use "*" to disable rank cache for all leaderboards.
func ConfigureRankCacheBlacklist(csv string) {
	rankBlacklistMu.Lock()
	defer rankBlacklistMu.Unlock()
	rankBlacklist = map[string]struct{}{}
	rankBlacklistAll = false
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return
	}
	for _, part := range strings.Split(csv, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		if id == "*" {
			rankBlacklistAll = true
			continue
		}
		rankBlacklist[id] = struct{}{}
	}
}

// IsRankCacheBlacklisted reports whether rank cache is disabled for the given board.
func IsRankCacheBlacklisted(leaderboardID string) bool {
	rankBlacklistMu.RLock()
	defer rankBlacklistMu.RUnlock()
	if rankBlacklistAll {
		return true
	}
	_, ok := rankBlacklist[leaderboardID]
	return ok
}
