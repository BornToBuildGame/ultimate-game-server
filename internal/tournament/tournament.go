package tournament

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"ultimate-game-server/internal/leaderboard"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	ErrTournamentNotFound       = errors.New("tournament not found")
	ErrTournamentMaxSizeReached = errors.New("tournament max size reached")
	ErrTournamentOutsideDuration = errors.New("tournament is outside active duration")
	ErrTournamentEnded          = errors.New("tournament has already ended")
)

// RewardHook is triggered when a tournament active window ends.
type RewardHook func(ctx context.Context, pool *pgxpool.Pool, tournamentID string, expiryTime time.Time, topRecords []*leaderboard.LeaderboardRecord) error

// ResetHook is triggered when a tournament occurrence resets/expires.
type ResetHook func(ctx context.Context, pool *pgxpool.Pool, tournamentID string, endActive, nextReset int64) error

// LeaderboardResetHook is triggered when a regular leaderboard resets.
type LeaderboardResetHook func(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, resetUnix int64) error

// TournamentScheduler manages tournament end/reset and leaderboard reset callbacks.
type TournamentScheduler struct {
	pool             *pgxpool.Pool
	rdb              *redis.Client
	logger           *zap.Logger
	rewardHook       RewardHook
	resetHook        ResetHook
	leaderboardReset LeaderboardResetHook
	localRewarded    map[string]bool
	localRewardedMu  sync.Mutex
	localReset       map[string]bool
	localResetMu     sync.Mutex
	stopChan         chan struct{}
	wg               sync.WaitGroup

	mu               sync.Mutex
	endActiveTimer   *time.Timer
	expiryTimer      *time.Timer
	lastEndActive    int64
	lastExpiry       int64
	callbackQueue    chan schedulerCallback
	workers          int
}

type schedulerCallback struct {
	kind          string // "tournament_end", "tournament_reset", "leaderboard_reset"
	id            string
	endActive     int64
	expiry        int64
	expiryTime    time.Time
}

// NewTournamentScheduler creates a new scheduler.
func NewTournamentScheduler(pool *pgxpool.Pool, rdb *redis.Client, logger *zap.Logger, hook RewardHook) *TournamentScheduler {
	return &TournamentScheduler{
		pool:          pool,
		rdb:           rdb,
		logger:        logger,
		rewardHook:    hook,
		localRewarded: make(map[string]bool),
		localReset:    make(map[string]bool),
		stopChan:      make(chan struct{}),
		callbackQueue: make(chan schedulerCallback, 1024),
		workers:       4,
	}
}

// SetRewardHook configures the tournament end/reward callback.
func (ts *TournamentScheduler) SetRewardHook(hook RewardHook) {
	ts.rewardHook = hook
}

// SetResetHook configures the tournament reset callback.
func (ts *TournamentScheduler) SetResetHook(hook ResetHook) {
	ts.resetHook = hook
}

// SetLeaderboardResetHook configures the leaderboard reset callback.
func (ts *TournamentScheduler) SetLeaderboardResetHook(hook LeaderboardResetHook) {
	ts.leaderboardReset = hook
}

// Start runs timer-based end/expiry evaluation with a callback worker queue.
// tickInterval is retained for compatibility; timers drive precision scheduling.
func (ts *TournamentScheduler) Start(tickInterval time.Duration) {
	if tickInterval <= 0 {
		tickInterval = 30 * time.Second
	}
	// Warm config cache.
	if ts.pool != nil {
		if lbs, err := leaderboard.LoadAllLeaderboards(context.Background(), ts.pool); err == nil {
			leaderboard.SharedConfigCache.LoadAll(lbs)
		}
	}

	for i := 0; i < ts.workers; i++ {
		ts.wg.Add(1)
		go ts.callbackWorker()
	}

	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()
		ts.logger.Info("Starting tournament/leaderboard timer scheduler...")
		ts.Update()
		safety := time.NewTicker(tickInterval)
		defer safety.Stop()
		hourly := time.NewTicker(time.Hour)
		defer hourly.Stop()
		for {
			select {
			case <-ts.stopChan:
				ts.logger.Info("Stopping tournament/leaderboard scheduler daemon...")
				return
			case <-safety.C:
				ts.Update()
			case t := <-hourly.C:
				leaderboard.SharedRankCache.TrimExpired(t.Unix())
				if ts.pool != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
					_, _ = leaderboard.PruneExpiredRecords(ctx, ts.pool, t.UTC())
					cancel()
				}
			}
		}
	}()
}

// Update recalculates the next end-active and expiry timers from cached/DB configs.
func (ts *TournamentScheduler) Update() {
	if ts.pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UTC()
	var nearestEnd, nearestExpiry int64

	lbs, err := leaderboard.LoadAllLeaderboards(ctx, ts.pool)
	if err != nil {
		ts.logger.Warn("scheduler update load failed", zap.Error(err))
		return
	}
	leaderboard.SharedConfigCache.LoadAll(lbs)

	type dueItem struct {
		kind string
		id   string
		end  int64
		exp  int64
	}
	var dueNow []dueItem

	for _, lb := range lbs {
		if lb.IsTournament() {
			_, endActive, expiry := leaderboard.ActiveDeadlines(lb, now)
			if endActive > now.Unix() {
				if nearestEnd == 0 || endActive < nearestEnd {
					nearestEnd = endActive
				}
			} else if endActive > 0 {
				dueNow = append(dueNow, dueItem{kind: "tournament_end", id: lb.ID, end: endActive, exp: expiry})
			}
			if expiry > now.Unix() {
				if nearestExpiry == 0 || expiry < nearestExpiry {
					nearestExpiry = expiry
				}
			} else if expiry > 0 {
				dueNow = append(dueNow, dueItem{kind: "tournament_reset", id: lb.ID, end: endActive, exp: expiry})
			}
		} else if lb.ResetSchedule != "" {
			sched := leaderboard.MustParseResetSchedule(lb.ResetSchedule)
			if sched == nil {
				continue
			}
			next := sched.Next(now).Unix()
			if nearestExpiry == 0 || next < nearestExpiry {
				nearestExpiry = next
			}
			prev := leaderboard.CalculatePrevReset(now, lb.StartTime.Unix(), sched)
			if prev > 0 && now.Unix()-prev <= int64(tickWindowSeconds()) {
				dueNow = append(dueNow, dueItem{kind: "leaderboard_reset", id: lb.ID, exp: prev})
			}
		}
	}

	for _, d := range dueNow {
		ts.enqueue(schedulerCallback{
			kind: d.kind, id: d.id, endActive: d.end, expiry: d.exp,
			expiryTime: leaderboard.ResolveExpiryTime(d.exp),
		})
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	if nearestEnd > 0 && nearestEnd != ts.lastEndActive {
		ts.lastEndActive = nearestEnd
		if ts.endActiveTimer != nil {
			ts.endActiveTimer.Stop()
		}
		delay := time.Until(time.Unix(nearestEnd, 0).UTC())
		if delay < 0 {
			delay = 0
		}
		ts.endActiveTimer = time.AfterFunc(delay, func() { ts.Update() })
	}
	if nearestExpiry > 0 && nearestExpiry != ts.lastExpiry {
		ts.lastExpiry = nearestExpiry
		if ts.expiryTimer != nil {
			ts.expiryTimer.Stop()
		}
		delay := time.Until(time.Unix(nearestExpiry, 0).UTC())
		if delay < 0 {
			delay = 0
		}
		ts.expiryTimer = time.AfterFunc(delay, func() { ts.Update() })
	}
}

func tickWindowSeconds() int {
	return 120
}

func (ts *TournamentScheduler) enqueue(cb schedulerCallback) {
	select {
	case ts.callbackQueue <- cb:
	default:
		ts.logger.Warn("scheduler callback queue full", zap.String("kind", cb.kind), zap.String("id", cb.id))
	}
}

func (ts *TournamentScheduler) callbackWorker() {
	defer ts.wg.Done()
	for {
		select {
		case <-ts.stopChan:
			return
		case cb, ok := <-ts.callbackQueue:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			ts.invokeCallback(ctx, cb)
			cancel()
		}
	}
}

func (ts *TournamentScheduler) invokeCallback(ctx context.Context, cb schedulerCallback) {
	switch cb.kind {
	case "tournament_end":
		rewardKey := fmt.Sprintf("tournament:end:%s:%d", cb.id, cb.endActive)
		if ts.isAlreadyRewarded(ctx, rewardKey) || !ts.tryAcquireRewardLock(ctx, rewardKey) {
			return
		}
		expiryTime := cb.expiryTime
		if expiryTime.IsZero() || expiryTime.Unix() == 0 {
			expiryTime = leaderboard.ResolveExpiryTime(cb.endActive)
		}
		topRecords, _, err := leaderboard.GetLeaderboardRecords(ctx, ts.pool, ts.rdb, cb.id, 100, "", expiryTime)
		if err != nil {
			ts.logger.Error("Failed to fetch top records for tournament reward", zap.String("id", cb.id), zap.Error(err))
			return
		}
		if ts.rewardHook != nil {
			if err := ts.rewardHook(ctx, ts.pool, cb.id, expiryTime, topRecords); err != nil {
				ts.logger.Error("Reward hook execution failed", zap.String("id", cb.id), zap.Error(err))
			}
		}
	case "tournament_reset":
		resetKey := fmt.Sprintf("tournament:reset:%s:%d", cb.id, cb.expiry)
		if ts.isAlreadyReset(ctx, resetKey) || !ts.tryAcquireResetLock(ctx, resetKey) {
			return
		}
		_, _ = leaderboard.ArchiveRecordsForExpiry(ctx, ts.pool, cb.id, cb.expiryTime, strconv.FormatInt(cb.expiry, 10))
		if _, err := ts.pool.Exec(ctx, `UPDATE leaderboard SET size = 0 WHERE id = $1`, cb.id); err != nil {
			ts.logger.Error("Could not reset tournament size", zap.Error(err), zap.String("id", cb.id))
		}
		leaderboard.SharedRankCache.EvictPartition(cb.id, cb.expiry)
		if ts.resetHook != nil {
			if err := ts.resetHook(ctx, ts.pool, cb.id, cb.endActive, cb.expiry); err != nil {
				ts.logger.Error("Tournament reset hook failed", zap.String("id", cb.id), zap.Error(err))
			}
		}
	case "leaderboard_reset":
		resetKey := fmt.Sprintf("leaderboard:reset:%s:%d", cb.id, cb.expiry)
		if ts.isAlreadyReset(ctx, resetKey) || !ts.tryAcquireResetLock(ctx, resetKey) {
			return
		}
		expiryTime := leaderboard.ResolveExpiryTime(cb.expiry)
		_, _ = leaderboard.ArchiveRecordsForExpiry(ctx, ts.pool, cb.id, expiryTime, strconv.FormatInt(cb.expiry, 10))
		if ts.leaderboardReset != nil {
			if err := ts.leaderboardReset(ctx, ts.pool, cb.id, cb.expiry); err != nil {
				ts.logger.Error("Leaderboard reset hook failed", zap.String("id", cb.id), zap.Error(err))
			}
		}
	}
}

// Stop halts the scheduler loop.
func (ts *TournamentScheduler) Stop() {
	close(ts.stopChan)
	ts.mu.Lock()
	if ts.endActiveTimer != nil {
		ts.endActiveTimer.Stop()
	}
	if ts.expiryTimer != nil {
		ts.expiryTimer.Stop()
	}
	ts.mu.Unlock()
	ts.wg.Wait()
}

func (ts *TournamentScheduler) evaluateTournaments(ctx context.Context) error {
	if ts.pool == nil {
		return nil
	}

	query := `
		SELECT id, reset_schedule, duration, start_time, end_time 
		FROM leaderboard 
		WHERE duration > 0
	`
	rows, err := ts.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	now := time.Now().UTC()
	type tourneyItem struct {
		id            string
		resetSchedule string
		duration      int
		startTime     time.Time
		endTime       time.Time
	}

	var items []tourneyItem
	for rows.Next() {
		var item tourneyItem
		if err = rows.Scan(&item.id, &item.resetSchedule, &item.duration, &item.startTime, &item.endTime); err != nil {
			return err
		}
		items = append(items, item)
	}

	for _, item := range items {
		lb := &leaderboard.Leaderboard{
			ID:            item.id,
			ResetSchedule: item.resetSchedule,
			Duration:      item.duration,
			StartTime:     item.startTime,
			EndTime:       item.endTime,
		}
		startActive, endActive, expiry := leaderboard.ActiveDeadlines(lb, now)

		// Tournament end: active window closed but occurrence not yet fully expired, OR just after endActive.
		if endActive > 0 && now.Unix() >= endActive {
			rewardKey := fmt.Sprintf("tournament:end:%s:%d", item.id, endActive)
			if !ts.isAlreadyRewarded(ctx, rewardKey) && ts.tryAcquireRewardLock(ctx, rewardKey) {
				ts.logger.Info("Tournament active window ended, processing rewards",
					zap.String("id", item.id), zap.Int64("end_active", endActive), zap.Int64("expiry", expiry))

				expiryTime := leaderboard.ResolveExpiryTime(expiry)
				if expiry == 0 {
					expiryTime = leaderboard.ResolveExpiryTime(endActive)
				}
				topRecords, _, err := leaderboard.GetLeaderboardRecords(ctx, ts.pool, ts.rdb, item.id, 100, "", expiryTime)
				if err != nil {
					ts.logger.Error("Failed to fetch top records for tournament reward", zap.String("id", item.id), zap.Error(err))
				} else if ts.rewardHook != nil {
					if err = ts.rewardHook(ctx, ts.pool, item.id, expiryTime, topRecords); err != nil {
						ts.logger.Error("Reward hook execution failed", zap.String("id", item.id), zap.Error(err))
					}
				}
			}
		}

		// Tournament reset at expiry boundary.
		if expiry > 0 && now.Unix() >= expiry {
			resetKey := fmt.Sprintf("tournament:reset:%s:%d", item.id, expiry)
			if !ts.isAlreadyReset(ctx, resetKey) && ts.tryAcquireResetLock(ctx, resetKey) {
				ts.logger.Info("Tournament occurrence expired, resetting size",
					zap.String("id", item.id), zap.Int64("expiry", expiry))

				if _, err := ts.pool.Exec(ctx, `UPDATE leaderboard SET size = 0 WHERE id = $1`, item.id); err != nil {
					ts.logger.Error("Could not reset tournament size", zap.Error(err), zap.String("id", item.id))
				}
				leaderboard.SharedRankCache.EvictPartition(item.id, expiry)

				if ts.resetHook != nil {
					if err := ts.resetHook(ctx, ts.pool, item.id, endActive, expiry); err != nil {
						ts.logger.Error("Tournament reset hook failed", zap.String("id", item.id), zap.Error(err))
					}
				}
			}
		}

		_ = startActive
	}
	return nil
}

func (ts *TournamentScheduler) evaluateLeaderboardResets(ctx context.Context) error {
	if ts.pool == nil {
		return nil
	}
	query := `
		SELECT id, reset_schedule, start_time
		FROM leaderboard
		WHERE duration = 0 AND reset_schedule IS NOT NULL AND reset_schedule != ''
	`
	rows, err := ts.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	now := time.Now().UTC()
	for rows.Next() {
		var id, resetSchedule string
		var startTime time.Time
		if err := rows.Scan(&id, &resetSchedule, &startTime); err != nil {
			return err
		}
		sched := leaderboard.MustParseResetSchedule(resetSchedule)
		if sched == nil {
			continue
		}
		// Previous reset boundary = last fire; if we just passed a fire second within last tick window, fire hook.
		prev := leaderboard.CalculatePrevReset(now, startTime.Unix(), sched)
		if prev == 0 {
			continue
		}
		// Fire once per reset boundary when within the last ~2 minutes (covers 30s poll).
		if now.Unix()-prev > 120 {
			continue
		}
		resetKey := fmt.Sprintf("leaderboard:reset:%s:%d", id, prev)
		if ts.isAlreadyReset(ctx, resetKey) || !ts.tryAcquireResetLock(ctx, resetKey) {
			continue
		}
		ts.logger.Info("Leaderboard reset boundary reached", zap.String("id", id), zap.Int64("reset", prev))
		if ts.leaderboardReset != nil {
			if err := ts.leaderboardReset(ctx, ts.pool, id, prev); err != nil {
				ts.logger.Error("Leaderboard reset hook failed", zap.String("id", id), zap.Error(err))
			}
		}
	}
	return nil
}

func (ts *TournamentScheduler) isAlreadyRewarded(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		exists, err := ts.rdb.Exists(ctx, key).Result()
		if err == nil && exists > 0 {
			return true
		}
	}
	ts.localRewardedMu.Lock()
	defer ts.localRewardedMu.Unlock()
	return ts.localRewarded[key]
}

func (ts *TournamentScheduler) tryAcquireRewardLock(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		success, err := ts.rdb.SetNX(ctx, key, "1", 24*time.Hour).Result()
		if err == nil && success {
			return true
		}
		return false
	}
	ts.localRewardedMu.Lock()
	defer ts.localRewardedMu.Unlock()
	if ts.localRewarded[key] {
		return false
	}
	ts.localRewarded[key] = true
	return true
}

func (ts *TournamentScheduler) isAlreadyReset(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		exists, err := ts.rdb.Exists(ctx, key).Result()
		if err == nil && exists > 0 {
			return true
		}
	}
	ts.localResetMu.Lock()
	defer ts.localResetMu.Unlock()
	return ts.localReset[key]
}

func (ts *TournamentScheduler) tryAcquireResetLock(ctx context.Context, key string) bool {
	if ts.rdb != nil {
		success, err := ts.rdb.SetNX(ctx, key, "1", 24*time.Hour).Result()
		if err == nil && success {
			return true
		}
		return false
	}
	ts.localResetMu.Lock()
	defer ts.localResetMu.Unlock()
	if ts.localReset[key] {
		return false
	}
	ts.localReset[key] = true
	return true
}

// JoinTournament registers a player for the current tournament occurrence.
func JoinTournament(ctx context.Context, pool *pgxpool.Pool, tournamentID, ownerID, username string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var lb leaderboard.Leaderboard
	query := `SELECT join_required, end_time, start_time, duration, reset_schedule, max_size, max_num_score, enable_ranks, sort_order
		FROM leaderboard WHERE id = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, query, tournamentID).Scan(
		&lb.JoinRequired, &lb.EndTime, &lb.StartTime, &lb.Duration, &lb.ResetSchedule, &lb.MaxSize, &lb.MaxNumScore, &lb.EnableRanks, &lb.SortOrder,
	)
	if err == pgx.ErrNoRows {
		return ErrTournamentNotFound
	}
	if err != nil {
		return err
	}
	lb.ID = tournamentID
	if !lb.IsTournament() {
		return ErrTournamentNotFound
	}

	now := time.Now().UTC()
	if !lb.EndTime.IsZero() && lb.EndTime.Unix() > 0 && now.After(lb.EndTime) {
		return ErrTournamentEnded
	}

	startActive, endActive, expiry := leaderboard.ActiveDeadlines(&lb, now)
	nowUnix := now.Unix()
	if startActive > nowUnix || (endActive != 0 && endActive < nowUnix) {
		return ErrTournamentOutsideDuration
	}

	expiryTime := leaderboard.ResolveExpiryTime(expiry)
	maxNumScore := lb.MaxNumScore
	if maxNumScore <= 0 {
		maxNumScore = 1000000
	}

	result, err := tx.Exec(ctx, `
		INSERT INTO leaderboard_record (
			leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		) VALUES ($1, $2, $3, 0, 0, 0, $4, '{}', now(), now(), $5)
		ON CONFLICT (owner_id, leaderboard_id, expiry_time) DO NOTHING
	`, tournamentID, ownerID, username, maxNumScore, expiryTime)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 1 {
		if lb.MaxSize > 0 && lb.MaxSize < 100000000 {
			upd, err := tx.Exec(ctx, `UPDATE leaderboard SET size = size + 1 WHERE id = $1 AND size < max_size`, tournamentID)
			if err != nil {
				return err
			}
			if upd.RowsAffected() == 0 {
				return ErrTournamentMaxSizeReached
			}
		} else {
			_, _ = tx.Exec(ctx, `UPDATE leaderboard SET size = size + 1 WHERE id = $1`, tournamentID)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	_ = leaderboard.SharedRankCache.Insert(tournamentID, lb.SortOrder, 0, 0, expiry, ownerID, lb.EnableRanks)
	return nil
}

// AddAttempt increases max_num_score for a player in the current tournament occurrence.
func AddAttempt(ctx context.Context, pool *pgxpool.Pool, tournamentID, ownerID string, count int) error {
	if count == 0 {
		return nil
	}
	lb, err := leaderboard.GetLeaderboard(ctx, pool, tournamentID)
	if err != nil {
		return ErrTournamentNotFound
	}
	if !lb.IsTournament() {
		return ErrTournamentNotFound
	}
	now := time.Now().UTC()
	_, endActive, expiry := leaderboard.ActiveDeadlines(lb, now)
	if endActive <= now.Unix() {
		return ErrTournamentOutsideDuration
	}
	expiryTime := leaderboard.ResolveExpiryTime(expiry)
	_, err = pool.Exec(ctx, `
		UPDATE leaderboard_record SET max_num_score = max_num_score + $1
		WHERE leaderboard_id = $2 AND owner_id = $3 AND expiry_time = $4
	`, count, tournamentID, ownerID, expiryTime)
	return err
}

// TournamentView is a list/detail projection with computed active fields.
type TournamentView struct {
	*leaderboard.Leaderboard
	CanEnter    bool  `json:"can_enter"`
	StartActive int64 `json:"start_active"`
	EndActive   int64 `json:"end_active"`
	PrevReset   int64 `json:"prev_reset"`
	NextReset   int64 `json:"next_reset"`
}

// ToView computes active-state fields for a tournament leaderboard.
func ToView(lb *leaderboard.Leaderboard, now time.Time) *TournamentView {
	startActive, endActive, expiry := leaderboard.ActiveDeadlines(lb, now)
	canEnter := true
	nowUnix := now.Unix()
	if startActive > nowUnix || (endActive != 0 && endActive < nowUnix) {
		canEnter = false
	}
	prevReset := leaderboard.CalculatePrevReset(now, lb.StartTime.Unix(), leaderboard.MustParseResetSchedule(lb.ResetSchedule))
	return &TournamentView{
		Leaderboard: lb,
		CanEnter:    canEnter,
		StartActive: startActive,
		EndActive:   endActive,
		PrevReset:   prevReset,
		NextReset:   expiry,
	}
}

// ListTournaments retrieves tournaments filterable by category, time, and active flag.
func ListTournaments(
	ctx context.Context,
	pool *pgxpool.Pool,
	categoryStart, categoryEnd int,
	startTime, endTime time.Time,
	limit int,
	cursor string,
	activeOnly bool,
) ([]*TournamentView, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	if categoryEnd == 0 {
		categoryEnd = 1<<31 - 1
	}

	query := `
		SELECT id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
		       category, description, duration, end_time, join_required, max_size, max_num_score,
		       title, size, start_time, enable_ranks
		FROM leaderboard
		WHERE duration > 0 AND category >= $1 AND category <= $2
	`
	args := []interface{}{categoryStart, categoryEnd}
	argIdx := 3

	if !startTime.IsZero() {
		query += fmt.Sprintf(" AND start_time >= $%d", argIdx)
		args = append(args, startTime)
		argIdx++
	}
	if !endTime.IsZero() {
		query += fmt.Sprintf(" AND end_time <= $%d", argIdx)
		args = append(args, endTime)
		argIdx++
	}
	if cursor != "" {
		query += fmt.Sprintf(" AND id > $%d", argIdx)
		args = append(args, cursor)
		argIdx++
	}
	query += " ORDER BY id ASC"

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	now := time.Now().UTC()
	var views []*TournamentView
	for rows.Next() {
		lb := &leaderboard.Leaderboard{}
		if err = rows.Scan(
			&lb.ID, &lb.Authoritative, &lb.SortOrder, &lb.Operator, &lb.ResetSchedule, &lb.Metadata, &lb.CreateTime,
			&lb.Category, &lb.Description, &lb.Duration, &lb.EndTime, &lb.JoinRequired, &lb.MaxSize, &lb.MaxNumScore,
			&lb.Title, &lb.Size, &lb.StartTime, &lb.EnableRanks,
		); err != nil {
			return nil, "", err
		}
		v := ToView(lb, now)
		if activeOnly && !v.CanEnter {
			continue
		}
		views = append(views, v)
	}

	if len(views) > limit {
		next := views[limit-1].ID
		return views[:limit], next, nil
	}
	return views, "", nil
}

// GetTournaments fetches tournaments by ID.
func GetTournaments(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]*TournamentView, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
		       category, description, duration, end_time, join_required, max_size, max_num_score,
		       title, size, start_time, enable_ranks
		FROM leaderboard
		WHERE id = ANY($1::text[]) AND duration > 0
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	var out []*TournamentView
	for rows.Next() {
		lb := &leaderboard.Leaderboard{}
		if err = rows.Scan(
			&lb.ID, &lb.Authoritative, &lb.SortOrder, &lb.Operator, &lb.ResetSchedule, &lb.Metadata, &lb.CreateTime,
			&lb.Category, &lb.Description, &lb.Duration, &lb.EndTime, &lb.JoinRequired, &lb.MaxSize, &lb.MaxNumScore,
			&lb.Title, &lb.Size, &lb.StartTime, &lb.EnableRanks,
		); err != nil {
			return nil, err
		}
		out = append(out, ToView(lb, now))
	}
	return out, nil
}
