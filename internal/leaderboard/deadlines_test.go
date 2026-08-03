package leaderboard

import (
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/cronexpr"
)

func TestCalculateTournamentDeadlines_NoReset(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC).Unix()
	duration := int64(3600)
	now := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)

	sa, ea, exp := CalculateTournamentDeadlines(start, end, duration, nil, now)
	if sa != start {
		t.Fatalf("startActive=%d want %d", sa, start)
	}
	if ea != start+duration {
		t.Fatalf("endActive=%d want %d", ea, start+duration)
	}
	if exp != end {
		t.Fatalf("expiry=%d want %d", exp, end)
	}
}

func TestCalculateTournamentDeadlines_WithCron(t *testing.T) {
	expr, err := cronexpr.Parse("0 0 * * *") // daily midnight
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	duration := int64(3600)

	sa, ea, exp := CalculateTournamentDeadlines(start, 0, duration, expr, now)
	if sa != time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("unexpected startActive %d", sa)
	}
	if ea != sa+duration {
		t.Fatalf("unexpected endActive %d", ea)
	}
	if exp != time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("unexpected expiry %d", exp)
	}
}

func TestCalculateExpiry_AllTime(t *testing.T) {
	lb := &Leaderboard{ID: "alltime", Duration: 0}
	expiry, ok := CalculateExpiry(lb, 0, time.Now().UTC())
	if !ok || expiry != 0 {
		t.Fatalf("expected epoch expiry, got %d ok=%v", expiry, ok)
	}
}

func TestRankCache_InsertGet(t *testing.T) {
	cache := &GlobalRankCache{cache: make(map[leaderboardExpiryKey]*RankCache)}
	r1 := cache.Insert("lb1", SortOrderDescending, 100, 0, 0, "a", true)
	r2 := cache.Insert("lb1", SortOrderDescending, 200, 0, 0, "b", true)
	if r2 != 1 {
		t.Fatalf("expected b at rank 1, got %d", r2)
	}
	if cache.GetRank("lb1", 0, "a") != 2 {
		t.Fatalf("expected a at rank 2 after insert")
	}
	_ = r1
}
