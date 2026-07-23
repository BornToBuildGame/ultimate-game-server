package leaderboard

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const skiplistMaxLevel = 16

type skipNode struct {
	entry RankEntry
	next  []*skipNode
}

// SkipListRankCache is an O(log n) ordered rank structure per partition.
type SkipListRankCache struct {
	mu      sync.RWMutex
	sortAsc bool
	enabled bool
	level   int
	head    *skipNode
	size    int
	ownerIdx map[string]*skipNode
}

func newSkipListRankCache(sortAsc, enabled bool) *SkipListRankCache {
	return &SkipListRankCache{
		sortAsc:  sortAsc,
		enabled:  enabled,
		level:    1,
		head:     &skipNode{next: make([]*skipNode, skiplistMaxLevel)},
		ownerIdx: make(map[string]*skipNode),
	}
}

func (s *SkipListRankCache) less(a, b RankEntry) bool {
	if a.Score != b.Score {
		if s.sortAsc {
			return a.Score < b.Score
		}
		return a.Score > b.Score
	}
	if a.Subscore != b.Subscore {
		if s.sortAsc {
			return a.Subscore < b.Subscore
		}
		return a.Subscore > b.Subscore
	}
	return a.OwnerID < b.OwnerID
}

func (s *SkipListRankCache) randomLevel() int {
	lvl := 1
	for lvl < skiplistMaxLevel && rand.Intn(2) == 0 {
		lvl++
	}
	return lvl
}

func (s *SkipListRankCache) Insert(entry RankEntry) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return 0
	}
	if old, ok := s.ownerIdx[entry.OwnerID]; ok {
		s.deleteLocked(old.entry.OwnerID)
	}
	update := make([]*skipNode, skiplistMaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && s.less(x.next[i].entry, entry) {
			x = x.next[i]
		}
		update[i] = x
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	node := &skipNode{entry: entry, next: make([]*skipNode, lvl)}
	for i := 0; i < lvl; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}
	s.ownerIdx[entry.OwnerID] = node
	s.size++
	return s.rankOfLocked(entry.OwnerID)
}

func (s *SkipListRankCache) deleteLocked(ownerID string) {
	node, ok := s.ownerIdx[ownerID]
	if !ok {
		return
	}
	update := make([]*skipNode, skiplistMaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && s.less(x.next[i].entry, node.entry) {
			x = x.next[i]
		}
		update[i] = x
	}
	for i := 0; i < len(node.next); i++ {
		if update[i].next[i] == node {
			update[i].next[i] = node.next[i]
		}
	}
	delete(s.ownerIdx, ownerID)
	s.size--
}

func (s *SkipListRankCache) Delete(ownerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteLocked(ownerID)
}

func (s *SkipListRankCache) rankOfLocked(ownerID string) int64 {
	node, ok := s.ownerIdx[ownerID]
	if !ok {
		return 0
	}
	rank := int64(1)
	x := s.head.next[0]
	for x != nil && x != node {
		rank++
		x = x.next[0]
	}
	return rank
}

func (s *SkipListRankCache) GetRank(ownerID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.enabled {
		return 0
	}
	return s.rankOfLocked(ownerID)
}

func (s *SkipListRankCache) GetDataByRank(rank int64) (RankEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.enabled || rank < 1 || int(rank) > s.size {
		return RankEntry{}, false
	}
	x := s.head.next[0]
	for i := int64(1); i < rank && x != nil; i++ {
		x = x.next[0]
	}
	if x == nil {
		return RankEntry{}, false
	}
	return x.entry, true
}

func (s *SkipListRankCache) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// LeaderboardsGetId returns non-tournament leaderboards by ID.
func LeaderboardsGetId(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]*Leaderboard, error) {
	out := make([]*Leaderboard, 0, len(ids))
	for _, id := range ids {
		if lb := SharedConfigCache.Get(id); lb != nil {
			if !lb.IsTournament() {
				out = append(out, lb)
			}
			continue
		}
		lb, err := GetLeaderboard(ctx, pool, id)
		if err != nil {
			if err == ErrLeaderboardNotFound {
				continue
			}
			return nil, err
		}
		SharedConfigCache.Put(lb)
		if !lb.IsTournament() {
			out = append(out, lb)
		}
	}
	return out, nil
}

// TournamentsGetId returns tournament configs by ID.
func TournamentsGetId(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]*Leaderboard, error) {
	out := make([]*Leaderboard, 0, len(ids))
	for _, id := range ids {
		lb, err := GetLeaderboard(ctx, pool, id)
		if err != nil {
			if err == ErrLeaderboardNotFound {
				continue
			}
			return nil, err
		}
		if lb.IsTournament() {
			out = append(out, lb)
		}
	}
	return out, nil
}

// DisableRanks sets enable_ranks=false for a leaderboard or tournament.
func DisableRanks(ctx context.Context, pool *pgxpool.Pool, id string, tournament bool) error {
	lb, err := GetLeaderboard(ctx, pool, id)
	if err != nil {
		return err
	}
	if tournament && !lb.IsTournament() {
		return ErrLeaderboardNotFound
	}
	if !tournament && lb.IsTournament() {
		return ErrLeaderboardNotFound
	}
	_, err = pool.Exec(ctx, `UPDATE leaderboard SET enable_ranks = false WHERE id = $1`, id)
	if err != nil {
		return err
	}
	lb.EnableRanks = false
	SharedConfigCache.Put(lb)
	SharedRankCache.DeleteLeaderboard(id)
	return nil
}

// RecordsListCursorFromRank builds a list cursor that starts just before the given 1-based rank.
func RecordsListCursorFromRank(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, rank, expiryOverride int64) (string, error) {
	if rank < 1 {
		return "", fmt.Errorf("rank must be >= 1")
	}
	expiryTime, lb, err := ResolveCurrentExpiry(ctx, pool, leaderboardID, expiryOverride)
	if err != nil {
		return "", err
	}
	expiryUnix := expiryTime.Unix()
	rank-- // previous entry so requested rank is included
	if rank == 0 {
		return "", nil
	}
	ownerID, score, subscore, ok := SharedRankCache.GetDataByRank(leaderboardID, expiryUnix, rank)
	if !ok {
		// Seed cache then retry.
		_, _ = getOrBuildRankCache(ctx, pool, leaderboardID, expiryTime, lb)
		ownerID, score, subscore, ok = SharedRankCache.GetDataByRank(leaderboardID, expiryUnix, rank)
		if !ok {
			return "", fmt.Errorf("rank %d not found", rank+1)
		}
	}
	return EncodeCursor(&RecordListCursor{
		IsNext: true, LeaderboardID: leaderboardID, ExpiryUnix: expiryUnix,
		Score: score, Subscore: subscore, OwnerID: ownerID, Rank: rank,
	})
}

// RecordsHaystack returns records around an owner with optional cursor pagination.
func RecordsHaystack(ctx context.Context, pool *pgxpool.Pool, leaderboardID, ownerID string, limit int, cursor string, expiryOverride int64) ([]*LeaderboardRecord, string, string, error) {
	if cursor != "" {
		expiryTime := time.Time{}
		if expiryOverride != 0 {
			expiryTime = ResolveExpiryTime(expiryOverride)
		}
		return GetLeaderboardRecordsPaged(ctx, pool, leaderboardID, limit, cursor, expiryTime, expiryOverride)
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}
	expiryTime, lb, err := ResolveCurrentExpiry(ctx, pool, leaderboardID, expiryOverride)
	if err != nil {
		if err == ErrNoRecordsPossible {
			return []*LeaderboardRecord{}, "", "", nil
		}
		return nil, "", "", err
	}

	var ownerScore, ownerSubscore int64
	err = pool.QueryRow(ctx, `
		SELECT score, subscore FROM leaderboard_record
		WHERE owner_id = $1 AND leaderboard_id = $2 AND expiry_time = $3`,
		ownerID, leaderboardID, expiryTime,
	).Scan(&ownerScore, &ownerSubscore)
	if err == pgx.ErrNoRows {
		return []*LeaderboardRecord{}, "", "", nil
	}
	if err != nil {
		return nil, "", "", err
	}

	half := limit / 2
	if half < 1 {
		half = 1
	}
	orderAsc := lb.SortOrder == SortOrderAscending

	firstQuery := `SELECT owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		FROM leaderboard_record WHERE leaderboard_id = $1 AND expiry_time = $2`
	secondQuery := firstQuery
	if orderAsc {
		firstQuery += ` AND (score, subscore, owner_id) < ($3, $4, $5) ORDER BY score DESC, subscore DESC, owner_id DESC LIMIT $6`
		secondQuery += ` AND (score, subscore, owner_id) > ($3, $4, $5) ORDER BY score ASC, subscore ASC, owner_id ASC LIMIT $6`
	} else {
		firstQuery += ` AND (score, subscore, owner_id) > ($3, $4, $5) ORDER BY score ASC, subscore ASC, owner_id ASC LIMIT $6`
		secondQuery += ` AND (score, subscore, owner_id) < ($3, $4, $5) ORDER BY score DESC, subscore DESC, owner_id DESC LIMIT $6`
	}

	scanRows := func(query string) ([]*LeaderboardRecord, error) {
		rows, qerr := pool.Query(ctx, query, leaderboardID, expiryTime, ownerScore, ownerSubscore, ownerID, half+1)
		if qerr != nil {
			return nil, qerr
		}
		defer rows.Close()
		var recs []*LeaderboardRecord
		for rows.Next() {
			r := &LeaderboardRecord{LeaderboardID: leaderboardID}
			if serr := rows.Scan(&r.OwnerID, &r.Username, &r.Score, &r.Subscore, &r.NumScore, &r.MaxNumScore,
				&r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime); serr != nil {
				return nil, serr
			}
			recs = append(recs, r)
		}
		return recs, rows.Err()
	}

	first, err := scanRows(firstQuery)
	if err != nil {
		return nil, "", "", err
	}
	for i, j := 0, len(first)-1; i < j; i, j = i+1, j-1 {
		first[i], first[j] = first[j], first[i]
	}
	second, err := scanRows(secondQuery)
	if err != nil {
		return nil, "", "", err
	}

	ownerRecs, err := GetOwnerRecords(ctx, pool, leaderboardID, []string{ownerID}, expiryTime)
	if err != nil {
		return nil, "", "", err
	}
	var owner *LeaderboardRecord
	if len(ownerRecs) > 0 {
		owner = ownerRecs[0]
	}

	combined := make([]*LeaderboardRecord, 0, len(first)+1+len(second))
	combined = append(combined, first...)
	if owner != nil {
		combined = append(combined, owner)
	}
	combined = append(combined, second...)
	if len(combined) > limit {
		// Center window on owner.
		ownerIdx := len(first)
		start := ownerIdx - half
		if start < 0 {
			start = 0
		}
		end := start + limit
		if end > len(combined) {
			end = len(combined)
			start = end - limit
			if start < 0 {
				start = 0
			}
		}
		combined = combined[start:end]
	}
	SharedRankCache.FillRanks(leaderboardID, expiryTime.Unix(), combined, lb.EnableRanks)

	var nextCursor, prevCursor string
	if len(combined) > 0 {
		firstR, lastR := combined[0], combined[len(combined)-1]
		prevCursor, _ = EncodeCursor(&RecordListCursor{
			IsNext: false, LeaderboardID: leaderboardID, ExpiryUnix: expiryTime.Unix(),
			Score: firstR.Score, Subscore: firstR.Subscore, OwnerID: firstR.OwnerID,
		})
		nextCursor, _ = EncodeCursor(&RecordListCursor{
			IsNext: true, LeaderboardID: leaderboardID, ExpiryUnix: expiryTime.Unix(),
			Score: lastR.Score, Subscore: lastR.Subscore, OwnerID: lastR.OwnerID,
		})
	}
	return combined, nextCursor, prevCursor, nil
}

// RecordsDeleteAll deletes all leaderboard records for a user and clears rank cache entries.
func RecordsDeleteAll(ctx context.Context, pool *pgxpool.Pool, ownerID string) error {
	rows, err := pool.Query(ctx, `
		DELETE FROM leaderboard_record WHERE owner_id = $1
		RETURNING leaderboard_id, EXTRACT(EPOCH FROM expiry_time)::bigint`, ownerID)
	if err != nil {
		return err
	}
	defer rows.Close()
	nowUnix := time.Now().UTC().Unix()
	for rows.Next() {
		var lbID string
		var expiryUnix int64
		if err := rows.Scan(&lbID, &expiryUnix); err != nil {
			return err
		}
		if expiryUnix > 0 && expiryUnix <= nowUnix {
			continue
		}
		SharedRankCache.Delete(lbID, expiryUnix, ownerID)
	}
	return rows.Err()
}

// RecordsReadAll returns all leaderboard records owned by a user.
func RecordsReadAll(ctx context.Context, pool *pgxpool.Pool, ownerID string) ([]*LeaderboardRecord, error) {
	rows, err := pool.Query(ctx, `
		SELECT leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score,
		       metadata, create_time, update_time, expiry_time
		FROM leaderboard_record WHERE owner_id = $1`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*LeaderboardRecord
	for rows.Next() {
		r := &LeaderboardRecord{}
		if err := rows.Scan(&r.LeaderboardID, &r.OwnerID, &r.Username, &r.Score, &r.Subscore,
			&r.NumScore, &r.MaxNumScore, &r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LoadAllLeaderboards loads every leaderboard config (for cache warmup).
func LoadAllLeaderboards(ctx context.Context, pool *pgxpool.Pool) ([]*Leaderboard, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
		       category, description, duration, end_time, join_required, max_size, max_num_score,
		       title, size, start_time, enable_ranks
		FROM leaderboard`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*Leaderboard
	for rows.Next() {
		lb := &Leaderboard{}
		if err := rows.Scan(
			&lb.ID, &lb.Authoritative, &lb.SortOrder, &lb.Operator, &lb.ResetSchedule, &lb.Metadata, &lb.CreateTime,
			&lb.Category, &lb.Description, &lb.Duration, &lb.EndTime, &lb.JoinRequired, &lb.MaxSize, &lb.MaxNumScore,
			&lb.Title, &lb.Size, &lb.StartTime, &lb.EnableRanks,
		); err != nil {
			return nil, err
		}
		list = append(list, lb)
	}
	return list, rows.Err()
}
