package leaderboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	ErrLeaderboardNotFound = errors.New("leaderboard not found")
	ErrAuthoritative       = errors.New("leaderboard is authoritative and rejects client submissions")
	ErrMaxAttemptsReached  = errors.New("max score submission attempts reached")
	ErrInvalidOperator     = errors.New("invalid score operator")
	ErrJoinRequired        = errors.New("join required before submitting score")
	ErrInvalidLeaderboardID = errors.New("invalid leaderboard id")
	ErrMetadataTooLarge    = errors.New("metadata exceeds max size")
	ErrRateLimited         = errors.New("score submission rate limit exceeded")
	ErrInvalidCursor       = errors.New("leaderboard cursor invalid")
	ErrNoRecordsPossible   = errors.New("no records available for current expiry")
)

const (
	SortOrderAscending  = 0
	SortOrderDescending = 1

	OperatorBest      = 0
	OperatorSet       = 1
	OperatorIncrement = 2
	OperatorDecrement = 3

	MaxMetadataBytes     = 2 * 1024
	MaxPageSize            = 1000
	ScoreSubmissionsPerMin = 10
)

var leaderboardIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// Leaderboard represents a leaderboard configuration.
type Leaderboard struct {
	ID            string    `json:"id"`
	Authoritative bool      `json:"authoritative"`
	SortOrder     int       `json:"sort_order"`
	Operator      int       `json:"operator"`
	ResetSchedule string    `json:"reset_schedule"`
	Metadata      string    `json:"metadata"`
	CreateTime    time.Time `json:"create_time"`
	Category      int       `json:"category"`
	Description   string    `json:"description"`
	Duration      int       `json:"duration"`
	EndTime       time.Time `json:"end_time"`
	JoinRequired  bool      `json:"join_required"`
	MaxSize       int       `json:"max_size"`
	MaxNumScore   int       `json:"max_num_score"`
	Title         string    `json:"title"`
	Size          int       `json:"size"`
	StartTime     time.Time `json:"start_time"`
	EnableRanks   bool      `json:"enable_ranks"`
}

// LeaderboardRecord represents a score entry.
type LeaderboardRecord struct {
	LeaderboardID string    `json:"leaderboard_id"`
	OwnerID       string    `json:"owner_id"`
	Username      string    `json:"username"`
	Score         int64     `json:"score"`
	Subscore      int64     `json:"subscore"`
	NumScore      int       `json:"num_score"`
	MaxNumScore   int       `json:"max_num_score"`
	Metadata      string    `json:"metadata"`
	CreateTime    time.Time `json:"create_time"`
	UpdateTime    time.Time `json:"update_time"`
	ExpiryTime    time.Time `json:"expiry_time"`
	Rank          int64     `json:"rank"`
}

// InvalidationPayload is published to Redis Pub/Sub on score updates.
type InvalidationPayload struct {
	LeaderboardID string `json:"leaderboard_id"`
	ExpiryTime    int64  `json:"expiry_time"`
}

// Local record list cache (full sorted slices for around-owner).
type rankListCache struct {
	mu    sync.RWMutex
	cache map[string][]*LeaderboardRecord
}

var localCache = &rankListCache{
	cache: make(map[string][]*LeaderboardRecord),
}

// Per-user-per-board score submission rate limiting.
type scoreRateLimiter struct {
	mu   sync.Mutex
	hits map[string][]time.Time
}

var scoreLimiter = &scoreRateLimiter{hits: make(map[string][]time.Time)}

func (l *scoreRateLimiter) allow(leaderboardID, ownerID string) bool {
	key := leaderboardID + ":" + ownerID
	now := time.Now()
	cutoff := now.Add(-1 * time.Minute)
	l.mu.Lock()
	defer l.mu.Unlock()
	times := l.hits[key]
	filtered := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	if len(filtered) >= ScoreSubmissionsPerMin {
		l.hits[key] = filtered
		return false
	}
	l.hits[key] = append(filtered, now)
	return true
}

func ValidateLeaderboardID(id string) error {
	if !leaderboardIDPattern.MatchString(id) {
		return ErrInvalidLeaderboardID
	}
	return nil
}

func ValidateMetadata(metadata string) error {
	if len(metadata) > MaxMetadataBytes {
		return ErrMetadataTooLarge
	}
	return nil
}

// CreateLeaderboard inserts a new leaderboard configuration.
func CreateLeaderboard(ctx context.Context, pool *pgxpool.Pool, lb *Leaderboard) error {
	if err := ValidateLeaderboardID(lb.ID); err != nil {
		return err
	}
	if lb.ResetSchedule != "" {
		if _, err := ParseResetSchedule(lb.ResetSchedule); err != nil {
			return fmt.Errorf("invalid reset_schedule: %w", err)
		}
	}
	if err := ValidateMetadata(lb.Metadata); err != nil {
		return err
	}

	query := `
		INSERT INTO leaderboard (
			id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
			category, description, duration, end_time, join_required, max_size, max_num_score,
			title, size, start_time, enable_ranks
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`
	if lb.CreateTime.IsZero() {
		lb.CreateTime = time.Now().UTC()
	}
	if lb.StartTime.IsZero() {
		lb.StartTime = time.Now().UTC()
	}
	if lb.EndTime.IsZero() {
		lb.EndTime = time.Unix(0, 0).UTC()
	}
	if lb.Metadata == "" {
		lb.Metadata = "{}"
	}
	if lb.MaxSize == 0 {
		lb.MaxSize = 100000000
	}
	if lb.MaxNumScore == 0 {
		lb.MaxNumScore = 1000000
	}

	_, err := pool.Exec(ctx, query,
		lb.ID, lb.Authoritative, lb.SortOrder, lb.Operator, lb.ResetSchedule, lb.Metadata, lb.CreateTime,
		lb.Category, lb.Description, lb.Duration, lb.EndTime, lb.JoinRequired, lb.MaxSize, lb.MaxNumScore,
		lb.Title, lb.Size, lb.StartTime, lb.EnableRanks,
	)
	if err != nil {
		return err
	}
	SharedConfigCache.Put(lb)
	return nil
}

// GetLeaderboard fetches a leaderboard config.
func GetLeaderboard(ctx context.Context, pool *pgxpool.Pool, id string) (*Leaderboard, error) {
	query := `
		SELECT id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
		       category, description, duration, end_time, join_required, max_size, max_num_score,
		       title, size, start_time, enable_ranks
		FROM leaderboard WHERE id = $1
	`
	lb := &Leaderboard{}
	err := pool.QueryRow(ctx, query, id).Scan(
		&lb.ID, &lb.Authoritative, &lb.SortOrder, &lb.Operator, &lb.ResetSchedule, &lb.Metadata, &lb.CreateTime,
		&lb.Category, &lb.Description, &lb.Duration, &lb.EndTime, &lb.JoinRequired, &lb.MaxSize, &lb.MaxNumScore,
		&lb.Title, &lb.Size, &lb.StartTime, &lb.EnableRanks,
	)
	if err == pgx.ErrNoRows {
		return nil, ErrLeaderboardNotFound
	}
	return lb, err
}

// ListLeaderboards returns paginated leaderboard configs (non-tournament first-class list).
func ListLeaderboards(ctx context.Context, pool *pgxpool.Pool, limit int, cursor string) ([]*Leaderboard, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}
	query := `
		SELECT id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
		       category, description, duration, end_time, join_required, max_size, max_num_score,
		       title, size, start_time, enable_ranks
		FROM leaderboard
		WHERE duration = 0
	`
	args := []interface{}{}
	if cursor != "" {
		query += ` AND id > $1`
		args = append(args, cursor)
	}
	query += fmt.Sprintf(` ORDER BY id ASC LIMIT $%d`, len(args)+1)
	args = append(args, limit+1)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
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
			return nil, "", err
		}
		list = append(list, lb)
	}

	nextCursor := ""
	if len(list) > limit {
		nextCursor = list[limit-1].ID
		list = list[:limit]
	}
	return list, nextCursor, nil
}

// StartInvalidationListener subscribes to Redis Pub/Sub and evicts local caches.
func StartInvalidationListener(ctx context.Context, rdb *redis.Client) {
	if rdb == nil {
		return
	}
	pubsub := rdb.Subscribe(ctx, "leaderboard:invalidation")
	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				_ = pubsub.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var payload InvalidationPayload
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err == nil {
					key := fmt.Sprintf("%s:%d", payload.LeaderboardID, payload.ExpiryTime)
					localCache.mu.Lock()
					delete(localCache.cache, key)
					localCache.mu.Unlock()
					SharedRankCache.EvictPartition(payload.LeaderboardID, payload.ExpiryTime)
				}
			}
		}
	}()
}

func publishInvalidation(ctx context.Context, rdb *redis.Client, leaderboardID string, expiryUnix int64) {
	key := fmt.Sprintf("%s:%d", leaderboardID, expiryUnix)
	localCache.mu.Lock()
	delete(localCache.cache, key)
	localCache.mu.Unlock()

	if rdb != nil {
		payload := InvalidationPayload{LeaderboardID: leaderboardID, ExpiryTime: expiryUnix}
		pBytes, _ := json.Marshal(payload)
		rdb.Publish(ctx, "leaderboard:invalidation", string(pBytes))
	}
}

// OperatorNoOverride sentinel for SubmitScore override (use board operator).
const OperatorNoOverride = -1

// SubmitScore writes a player score, enforcing constraints and operators.
// Optional overrideOp: when provided and != OperatorNoOverride / when first element >= 0,
// overrides the leaderboard's configured operator (BEST/SET/INCREMENT/DECREMENT).
func SubmitScore(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID, ownerID, username string, score, subscore int64, metadata string, byPlayer bool, overrideOp ...int) (*LeaderboardRecord, error) {
	override := OperatorNoOverride
	if len(overrideOp) > 0 {
		override = overrideOp[0]
	}
	if score < 0 || subscore < 0 {
		return nil, fmt.Errorf("score and subscore must be non-negative")
	}
	if byPlayer && !scoreLimiter.allow(leaderboardID, ownerID) {
		return nil, ErrRateLimited
	}
	if metadata == "" {
		metadata = "{}"
	}
	if err := ValidateMetadata(metadata); err != nil {
		return nil, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	lbQuery := `SELECT authoritative, sort_order, operator, max_num_score, duration, start_time, end_time, join_required, reset_schedule, enable_ranks
		FROM leaderboard WHERE id = $1 FOR SHARE`
	var authoritative bool
	var sortOrder, operator, maxNumScore, duration int
	var startTime, endTime time.Time
	var joinRequired, enableRanks bool
	var resetSchedule string
	err = tx.QueryRow(ctx, lbQuery, leaderboardID).Scan(
		&authoritative, &sortOrder, &operator, &maxNumScore, &duration, &startTime, &endTime, &joinRequired, &resetSchedule, &enableRanks,
	)
	if err == pgx.ErrNoRows {
		return nil, ErrLeaderboardNotFound
	} else if err != nil {
		return nil, err
	}

	if byPlayer && authoritative {
		return nil, ErrAuthoritative
	}

	lb := &Leaderboard{
		ID:            leaderboardID,
		Duration:      duration,
		StartTime:     startTime,
		EndTime:       endTime,
		ResetSchedule: resetSchedule,
		EnableRanks:   enableRanks,
		SortOrder:     sortOrder,
	}
	expiryUnix, ok := CalculateExpiry(lb, 0, time.Now().UTC())
	if !ok {
		return nil, ErrNoRecordsPossible
	}
	expiryTime := ResolveExpiryTime(expiryUnix)

	if joinRequired {
		var existsCheck bool
		checkQuery := `SELECT EXISTS(SELECT 1 FROM leaderboard_record WHERE owner_id = $1 AND leaderboard_id = $2 AND expiry_time = $3)`
		err = tx.QueryRow(ctx, checkQuery, ownerID, leaderboardID, expiryTime).Scan(&existsCheck)
		if err != nil {
			return nil, err
		}
		if !existsCheck {
			return nil, ErrJoinRequired
		}
	}

	recordQuery := `
		SELECT score, subscore, num_score 
		FROM leaderboard_record 
		WHERE owner_id = $1 AND leaderboard_id = $2 AND expiry_time = $3 
		FOR UPDATE
	`
	var oldScore, oldSubscore int64
	var numScore int
	exists := true

	err = tx.QueryRow(ctx, recordQuery, ownerID, leaderboardID, expiryTime).Scan(&oldScore, &oldSubscore, &numScore)
	if err == pgx.ErrNoRows {
		exists = false
	} else if err != nil {
		return nil, err
	}

	newScore := score
	newSubscore := subscore
	now := time.Now().UTC()

	if override >= 0 && override <= OperatorDecrement {
		operator = override
	}

	if exists {
		if numScore >= maxNumScore {
			return nil, ErrMaxAttemptsReached
		}

		switch operator {
		case OperatorBest:
			if sortOrder == SortOrderDescending {
				if oldScore > score {
					newScore = oldScore
					newSubscore = oldSubscore
				} else if oldScore == score && oldSubscore > subscore {
					newSubscore = oldSubscore
				}
			} else {
				if oldScore < score {
					newScore = oldScore
					newSubscore = oldSubscore
				} else if oldScore == score && oldSubscore < subscore {
					newSubscore = oldSubscore
				}
			}
		case OperatorSet:
		case OperatorIncrement:
			newScore = oldScore + score
			newSubscore = oldSubscore + subscore
		case OperatorDecrement:
			newScore = oldScore - score
			if newScore < 0 {
				newScore = 0
			}
			newSubscore = oldSubscore - subscore
			if newSubscore < 0 {
				newSubscore = 0
			}
		default:
			return nil, ErrInvalidOperator
		}

		updateQuery := `
			UPDATE leaderboard_record 
			SET score = $1, subscore = $2, num_score = num_score + 1, metadata = $3, update_time = now(), username = $7
			WHERE owner_id = $4 AND leaderboard_id = $5 AND expiry_time = $6
		`
		_, err = tx.Exec(ctx, updateQuery, newScore, newSubscore, metadata, ownerID, leaderboardID, expiryTime, username)
		if err != nil {
			return nil, err
		}
		numScore++
	} else {
		insertQuery := `
			INSERT INTO leaderboard_record (
				leaderboard_id, owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
			) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, now(), now(), $8)
		`
		_, err = tx.Exec(ctx, insertQuery, leaderboardID, ownerID, username, score, subscore, maxNumScore, metadata, expiryTime)
		if err != nil {
			return nil, err
		}
		numScore = 1
		oldScore = 0
		oldSubscore = 0
	}

	_, err = tx.Exec(ctx, `UPDATE leaderboard SET size = (SELECT COUNT(*) FROM leaderboard_record WHERE leaderboard_id = $1 AND expiry_time = $2) WHERE id = $1`, leaderboardID, expiryTime)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, `
INSERT INTO leaderboard_score_audit (leaderboard_id, owner_id, old_score, new_score, old_subscore, new_subscore, operator)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		leaderboardID, ownerID, oldScore, newScore, oldSubscore, newSubscore, operator)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	rank := SharedRankCache.Insert(leaderboardID, sortOrder, newScore, newSubscore, expiryUnix, ownerID, enableRanks)
	publishInvalidation(ctx, rdb, leaderboardID, expiryUnix)

	return &LeaderboardRecord{
		LeaderboardID: leaderboardID,
		OwnerID:       ownerID,
		Username:      username,
		Score:         newScore,
		Subscore:      newSubscore,
		NumScore:      numScore,
		MaxNumScore:   maxNumScore,
		Metadata:      metadata,
		ExpiryTime:    expiryTime,
		CreateTime:    now,
		UpdateTime:    now,
		Rank:          rank,
	}, nil
}

// ResolveCurrentExpiry loads the leaderboard and returns the active expiry time.
func ResolveCurrentExpiry(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, overrideExpiry int64) (time.Time, *Leaderboard, error) {
	lb, err := GetLeaderboard(ctx, pool, leaderboardID)
	if err != nil {
		return time.Time{}, nil, err
	}
	expiryUnix, ok := CalculateExpiry(lb, overrideExpiry, time.Now().UTC())
	if !ok {
		return time.Time{}, lb, ErrNoRecordsPossible
	}
	return ResolveExpiryTime(expiryUnix), lb, nil
}

// GetLeaderboardRecords retrieves sorted, paginated records with keyset cursors.
func GetLeaderboardRecords(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID string, limit int, cursor string, expiryTime time.Time) ([]*LeaderboardRecord, string, error) {
	records, next, _, err := GetLeaderboardRecordsPaged(ctx, pool, leaderboardID, limit, cursor, expiryTime, 0)
	return records, next, err
}

// GetLeaderboardRecordsPaged returns next and prev cursors.
func GetLeaderboardRecordsPaged(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, limit int, cursor string, expiryTime time.Time, overrideExpiry int64) ([]*LeaderboardRecord, string, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}

	lb, err := GetLeaderboard(ctx, pool, leaderboardID)
	if err != nil {
		return nil, "", "", err
	}

	if expiryTime.IsZero() || (expiryTime.Unix() == 0 && overrideExpiry == 0 && (lb.IsTournament() || lb.ResetSchedule != "")) {
		et, _, err := ResolveCurrentExpiry(ctx, pool, leaderboardID, overrideExpiry)
		if err != nil {
			if errors.Is(err, ErrNoRecordsPossible) {
				return []*LeaderboardRecord{}, "", "", nil
			}
			return nil, "", "", err
		}
		expiryTime = et
	} else if overrideExpiry != 0 {
		expiryTime = ResolveExpiryTime(overrideExpiry)
	}

	incoming, err := DecodeCursor(cursor)
	if err != nil {
		return nil, "", "", ErrInvalidCursor
	}

	orderAsc := lb.SortOrder == SortOrderAscending
	query := `SELECT owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		FROM leaderboard_record WHERE leaderboard_id = $1 AND expiry_time = $2`
	params := []interface{}{leaderboardID, expiryTime}

	if incoming == nil {
		if orderAsc {
			query += " ORDER BY score ASC, subscore ASC, owner_id ASC"
		} else {
			query += " ORDER BY score DESC, subscore DESC, owner_id DESC"
		}
	} else {
		goingForward := incoming.IsNext
		if (orderAsc && goingForward) || (!orderAsc && !goingForward) {
			query += " AND (score, subscore, owner_id) > ($3, $4, $5) ORDER BY score ASC, subscore ASC, owner_id ASC"
		} else {
			query += " AND (score, subscore, owner_id) < ($3, $4, $5) ORDER BY score DESC, subscore DESC, owner_id DESC"
		}
		params = append(params, incoming.Score, incoming.Subscore, incoming.OwnerID)
	}
	query += fmt.Sprintf(" LIMIT $%d", len(params)+1)
	params = append(params, limit+1)

	rows, err := pool.Query(ctx, query, params...)
	if err != nil {
		return nil, "", "", err
	}
	defer rows.Close()

	var records []*LeaderboardRecord
	for rows.Next() {
		r := &LeaderboardRecord{LeaderboardID: leaderboardID}
		if err := rows.Scan(&r.OwnerID, &r.Username, &r.Score, &r.Subscore, &r.NumScore, &r.MaxNumScore,
			&r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime); err != nil {
			return nil, "", "", err
		}
		records = append(records, r)
	}

	// If we walked backward, reverse to ascending display order for desc boards etc.
	if incoming != nil && !incoming.IsNext {
		for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
			records[i], records[j] = records[j], records[i]
		}
	}

	var nextCursor, prevCursor string
	if len(records) > limit {
		records = records[:limit]
		last := records[len(records)-1]
		nextCursor, _ = EncodeCursor(&RecordListCursor{
			IsNext: true, LeaderboardID: leaderboardID, ExpiryUnix: expiryTime.Unix(),
			Score: last.Score, Subscore: last.Subscore, OwnerID: last.OwnerID,
		})
	}
	if len(records) > 0 && (incoming != nil) {
		first := records[0]
		prevCursor, _ = EncodeCursor(&RecordListCursor{
			IsNext: false, LeaderboardID: leaderboardID, ExpiryUnix: expiryTime.Unix(),
			Score: first.Score, Subscore: first.Subscore, OwnerID: first.OwnerID,
		})
	}

	expiryUnix := expiryTime.Unix()
	SharedRankCache.FillRanks(leaderboardID, expiryUnix, records, lb.EnableRanks)
	// Assign ranks from cache; if missing, build from list cache for this page.
	for i, r := range records {
		if r.Rank == 0 && lb.EnableRanks {
			base := int64(0)
			if incoming != nil && incoming.IsNext {
				base = incoming.Rank
			}
			r.Rank = base + int64(i) + 1
		}
	}

	return records, nextCursor, prevCursor, nil
}

// GetLeaderboardRecordsAroundPlayer retrieves records centered around the target player.
func GetLeaderboardRecordsAroundPlayer(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID, ownerID string, limit int, expiryTime time.Time) ([]*LeaderboardRecord, error) {
	if limit <= 0 {
		limit = 5
	}

	lb, err := GetLeaderboard(ctx, pool, leaderboardID)
	if err != nil {
		return nil, err
	}
	if expiryTime.IsZero() || (expiryTime.Unix() == 0 && (lb.IsTournament() || lb.ResetSchedule != "")) {
		et, _, err := ResolveCurrentExpiry(ctx, pool, leaderboardID, 0)
		if err != nil {
			if errors.Is(err, ErrNoRecordsPossible) {
				return []*LeaderboardRecord{}, nil
			}
			return nil, err
		}
		expiryTime = et
	}

	records, err := getOrBuildRankCache(ctx, pool, leaderboardID, expiryTime, lb)
	if err != nil {
		return nil, err
	}

	targetIdx := -1
	for i, r := range records {
		if r.OwnerID == ownerID {
			targetIdx = i
			break
		}
	}
	if targetIdx == -1 {
		return []*LeaderboardRecord{}, nil
	}

	startIdx := targetIdx - limit
	if startIdx < 0 {
		startIdx = 0
	}
	endIdx := targetIdx + limit + 1
	if endIdx > len(records) {
		endIdx = len(records)
	}

	out := make([]*LeaderboardRecord, endIdx-startIdx)
	copy(out, records[startIdx:endIdx])
	return out, nil
}

func getOrBuildRankCache(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, expiryTime time.Time, lb *Leaderboard) ([]*LeaderboardRecord, error) {
	key := fmt.Sprintf("%s:%d", leaderboardID, expiryTime.Unix())

	localCache.mu.RLock()
	cached, exists := localCache.cache[key]
	localCache.mu.RUnlock()
	if exists {
		return cached, nil
	}

	localCache.mu.Lock()
	defer localCache.mu.Unlock()
	if cached, exists = localCache.cache[key]; exists {
		return cached, nil
	}

	if lb == nil {
		var err error
		lb, err = GetLeaderboard(ctx, pool, leaderboardID)
		if err != nil {
			return nil, err
		}
	}

	recordQuery := `
		SELECT owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		FROM leaderboard_record
		WHERE leaderboard_id = $1 AND expiry_time = $2
	`
	rows, err := pool.Query(ctx, recordQuery, leaderboardID, expiryTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*LeaderboardRecord
	for rows.Next() {
		r := &LeaderboardRecord{LeaderboardID: leaderboardID}
		if err = rows.Scan(
			&r.OwnerID, &r.Username, &r.Score, &r.Subscore, &r.NumScore, &r.MaxNumScore,
			&r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime,
		); err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	sortOrder := lb.SortOrder
	sortRecords(records, sortOrder)

	SharedRankCache.LoadFromRecords(leaderboardID, expiryTime.Unix(), sortOrder, lb.EnableRanks, records)
	if !lb.EnableRanks {
		for _, r := range records {
			r.Rank = 0
		}
	} else {
		for i, r := range records {
			r.Rank = int64(i + 1)
		}
	}

	localCache.cache[key] = records
	return records, nil
}

func sortRecords(records []*LeaderboardRecord, sortOrder int) {
	sort.Slice(records, func(i, j int) bool {
		r1, r2 := records[i], records[j]
		if r1.Score != r2.Score {
			if sortOrder == SortOrderDescending {
				return r1.Score > r2.Score
			}
			return r1.Score < r2.Score
		}
		if r1.Subscore != r2.Subscore {
			if sortOrder == SortOrderDescending {
				return r1.Subscore > r2.Subscore
			}
			return r1.Subscore < r2.Subscore
		}
		if !r1.UpdateTime.Equal(r2.UpdateTime) {
			return r1.UpdateTime.Before(r2.UpdateTime)
		}
		return r1.OwnerID < r2.OwnerID
	})
}

// DeleteLeaderboard deletes a leaderboard configuration and all its records.
func DeleteLeaderboard(ctx context.Context, pool *pgxpool.Pool, id string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx, "DELETE FROM leaderboard_record WHERE leaderboard_id = $1", id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM leaderboard WHERE id = $1", id); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	SharedConfigCache.Delete(id)
	SharedRankCache.DeleteLeaderboard(id)
	localCache.mu.Lock()
	for k := range localCache.cache {
		if len(k) >= len(id)+1 && k[:len(id)+1] == id+":" {
			delete(localCache.cache, k)
		}
	}
	localCache.mu.Unlock()
	return nil
}

// DeleteRecord deletes a specific leaderboard record for a user across all expiries for the board.
func DeleteRecord(ctx context.Context, pool *pgxpool.Pool, leaderboardID, ownerID string) error {
	_, err := pool.Exec(ctx, `DELETE FROM leaderboard_record WHERE leaderboard_id = $1 AND owner_id = $2`, leaderboardID, ownerID)
	if err != nil {
		return err
	}

	localCache.mu.Lock()
	for k := range localCache.cache {
		if len(k) >= len(leaderboardID)+1 && k[:len(leaderboardID)+1] == leaderboardID+":" {
			delete(localCache.cache, k)
		}
	}
	localCache.mu.Unlock()
	SharedRankCache.DeleteLeaderboard(leaderboardID)
	return nil
}

// DeleteRecordForExpiry deletes a record for a specific expiry partition.
func DeleteRecordForExpiry(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID, ownerID string, expiryUnix int64) error {
	expiryTime := ResolveExpiryTime(expiryUnix)
	_, err := pool.Exec(ctx, `DELETE FROM leaderboard_record WHERE leaderboard_id = $1 AND owner_id = $2 AND expiry_time = $3`,
		leaderboardID, ownerID, expiryTime)
	if err != nil {
		return err
	}
	SharedRankCache.Delete(leaderboardID, expiryUnix, ownerID)
	publishInvalidation(ctx, rdb, leaderboardID, expiryUnix)
	return nil
}

// PruneExpiredRecords deletes records whose expiry_time has passed (and is not epoch).
func PruneExpiredRecords(ctx context.Context, pool *pgxpool.Pool, now time.Time) (int64, error) {
	tag, err := pool.Exec(ctx, `
		DELETE FROM leaderboard_record
		WHERE expiry_time > TIMESTAMPTZ '1970-01-01 00:00:00+00'
		  AND expiry_time < $1
	`, now.UTC())
	if err != nil {
		return 0, err
	}
	SharedRankCache.TrimExpired(now.Unix())
	return tag.RowsAffected(), nil
}

// GetOwnerRecords fetches specific owners' records for a leaderboard expiry.
func GetOwnerRecords(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, ownerIDs []string, expiryTime time.Time) ([]*LeaderboardRecord, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}
	lb, err := GetLeaderboard(ctx, pool, leaderboardID)
	if err != nil {
		return nil, err
	}
	if expiryTime.IsZero() {
		et, _, err := ResolveCurrentExpiry(ctx, pool, leaderboardID, 0)
		if err != nil {
			if errors.Is(err, ErrNoRecordsPossible) {
				return []*LeaderboardRecord{}, nil
			}
			return nil, err
		}
		expiryTime = et
	}

	rows, err := pool.Query(ctx, `
		SELECT owner_id, username, score, subscore, num_score, max_num_score, metadata, create_time, update_time, expiry_time
		FROM leaderboard_record
		WHERE leaderboard_id = $1 AND expiry_time = $2 AND owner_id = ANY($3::uuid[])
	`, leaderboardID, expiryTime, ownerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*LeaderboardRecord
	for rows.Next() {
		r := &LeaderboardRecord{LeaderboardID: leaderboardID}
		if err := rows.Scan(&r.OwnerID, &r.Username, &r.Score, &r.Subscore, &r.NumScore, &r.MaxNumScore,
			&r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	SharedRankCache.FillRanks(leaderboardID, expiryTime.Unix(), records, lb.EnableRanks)
	for _, r := range records {
		if r.Rank == 0 && lb.EnableRanks {
			r.Rank = SharedRankCache.GetRank(leaderboardID, expiryTime.Unix(), r.OwnerID)
		}
	}
	return records, nil
}
