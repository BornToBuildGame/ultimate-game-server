package leaderboard

import "testing"

func TestRankCacheBlacklist(t *testing.T) {
	ConfigureRankCacheBlacklist("weekly,daily")
	if !IsRankCacheBlacklisted("weekly") {
		t.Fatal("expected weekly blacklisted")
	}
	if IsRankCacheBlacklisted("alltime") {
		t.Fatal("alltime should not be blacklisted")
	}
	ConfigureRankCacheBlacklist("*")
	if !IsRankCacheBlacklisted("anything") {
		t.Fatal("expected all blacklisted")
	}
	ConfigureRankCacheBlacklist("")
	if IsRankCacheBlacklisted("weekly") {
		t.Fatal("expected empty blacklist")
	}
}
