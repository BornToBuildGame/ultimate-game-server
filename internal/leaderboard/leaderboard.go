package leaderboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
)

const (
	SortOrderAscending  = 0
	SortOrderDescending = 1

	OperatorBest      = 0
	OperatorSet       = 1
	OperatorIncrement = 2
	OperatorDecrement = 3
)

// Leaderboard represents a leaderboard configuration.
type Leaderboard struct {
	ID            string    `json:"id"`
	Authoritative bool      `json:"authoritative"`
	SortOrder     int       `json:"sort_order"` // 0=asc, 1=desc
	Operator      int       `json:"operator"`   // 0=best, 1=set, 2=increment, 3=decrement
	ResetSchedule string    `json:"reset_schedule"`
	Metadata      string    `json:"metadata"` // JSON
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
	Metadata      string    `json:"metadata"` // JSON
	CreateTime    time.Time `json:"create_time"`
	UpdateTime    time.Time `json:"update_time"`
	ExpiryTime    time.Time `json:"expiry_time"`
	Rank          int64     `json:"rank"`
}

// InvalidationPayload is published to Redis Pub/Sub on score updates.
type InvalidationPayload struct {
	LeaderboardID string `json:"leaderboard_id"`
	ExpiryTime    int64  `json:"expiry_time"` // unix timestamp
}

// Local Rank Cache structures
type rankCache struct {
	mu    sync.RWMutex
	cache map[string][]*LeaderboardRecord // key: "leaderboardID:expiryTimeUnix"
}

var localCache = &rankCache{
	cache: make(map[string][]*LeaderboardRecord),
}

// CreateLeaderboard inserts a new leaderboard configuration.
func CreateLeaderboard(ctx context.Context, pool *pgxpool.Pool, lb *Leaderboard) error {
	query := `
		INSERT INTO leaderboard (
			id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
			category, description, duration, end_time, join_required, max_size, max_num_score,
			title, size, start_time, enable_ranks
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`
	if lb.CreateTime.IsZero() {
		lb.CreateTime = time.Now()
	}
	if lb.StartTime.IsZero() {
		lb.StartTime = time.Now()
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
	return err
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

// StartInvalidationListener subscribes to Redis Pub/Sub and evicts local caches dynamically.
func StartInvalidationListener(ctx context.Context, rdb *redis.Client) {
	pubsub := rdb.Subscribe(ctx, "leaderboard:invalidation")
	go func() {
		ch := pubsub.Channel()
		for msg := range ch {
			var payload InvalidationPayload
			if err := json.Unmarshal([]byte(msg.Payload), &payload); err == nil {
				key := fmt.Sprintf("%s:%d", payload.LeaderboardID, payload.ExpiryTime)
				localCache.mu.Lock()
				delete(localCache.cache, key)
				localCache.mu.Unlock()
			}
		}
	}()
}

// SubmitScore writes a player score to the database, enforcing constraints and operators.
func SubmitScore(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID, ownerID, username string, score, subscore int64, metadata string, byPlayer bool) (*LeaderboardRecord, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 1. Lock and check leaderboard config
	lbQuery := `SELECT authoritative, sort_order, operator, max_num_score, duration, start_time, join_required FROM leaderboard WHERE id = $1 FOR SHARE`
	var authoritative bool
	var sortOrder, operator, maxNumScore, duration int
	var startTime time.Time
	var joinRequired bool
	err = tx.QueryRow(ctx, lbQuery, leaderboardID).Scan(&authoritative, &sortOrder, &operator, &maxNumScore, &duration, &startTime, &joinRequired)
	if err == pgx.ErrNoRows {
		return nil, ErrLeaderboardNotFound
	} else if err != nil {
		return nil, err
	}

	if byPlayer && authoritative {
		return nil, ErrAuthoritative
	}

	// Calculate correct expiry time for the score partition (default Epoch if no duration/schedule)
	expiryTime := time.Unix(0, 0).UTC()
	if duration > 0 {
		now := time.Now()
		elapsed := now.Sub(startTime)
		occIdx := int(elapsed.Seconds() / float64(duration))
		expiryTime = startTime.Add(time.Duration(occIdx+1) * time.Duration(duration) * time.Second)
	}

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

	if metadata == "" {
		metadata = "{}"
	}

	// 2. Fetch existing score entry
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

	if exists {
		if numScore >= maxNumScore {
			return nil, ErrMaxAttemptsReached
		}

		// Calculate based on operator rules
		switch operator {
		case OperatorBest:
			if sortOrder == SortOrderDescending {
				if oldScore > score {
					newScore = oldScore
					newSubscore = oldSubscore
				} else if oldScore == score && oldSubscore > subscore {
					newSubscore = oldSubscore
				}
			} else { // Ascending
				if oldScore < score {
					newScore = oldScore
					newSubscore = oldSubscore
				} else if oldScore == score && oldSubscore < subscore {
					newSubscore = oldSubscore
				}
			}
		case OperatorSet:
			// newScore and newSubscore already hold the input arguments
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
			SET score = $1, subscore = $2, num_score = num_score + 1, metadata = $3, update_time = now() 
			WHERE owner_id = $4 AND leaderboard_id = $5 AND expiry_time = $6
		`
		_, err = tx.Exec(ctx, updateQuery, newScore, newSubscore, metadata, ownerID, leaderboardID, expiryTime)
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
	}

	// Update size in leaderboard config
	_, err = tx.Exec(ctx, `UPDATE leaderboard SET size = (SELECT COUNT(*) FROM leaderboard_record WHERE leaderboard_id = $1 AND expiry_time = $2) WHERE id = $1`, leaderboardID, expiryTime)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	// 3. Publish to Redis & Evict locally
	key := fmt.Sprintf("%s:%d", leaderboardID, expiryTime.Unix())
	localCache.mu.Lock()
	delete(localCache.cache, key)
	localCache.mu.Unlock()

	if rdb != nil {
		payload := InvalidationPayload{
			LeaderboardID: leaderboardID,
			ExpiryTime:    expiryTime.Unix(),
		}
		pBytes, _ := json.Marshal(payload)
		rdb.Publish(ctx, "leaderboard:invalidation", string(pBytes))
	}

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
	}, nil
}

// GetLeaderboardRecords retrieves sorted, paginated records.
func GetLeaderboardRecords(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID string, limit int, cursor string, expiryTime time.Time) ([]*LeaderboardRecord, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100 // Enforce max page size rule
	}

	records, err := getOrBuildRankCache(ctx, pool, leaderboardID, expiryTime)
	if err != nil {
		return nil, "", err
	}

	startIdx := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err == nil {
			startIdx = parsed
		}
	}

	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(records) {
		return []*LeaderboardRecord{}, "", nil
	}

	endIdx := startIdx + limit
	if endIdx > len(records) {
		endIdx = len(records)
	}

	nextCursor := ""
	if endIdx < len(records) {
		nextCursor = strconv.Itoa(endIdx)
	}

	// Return a copy slice to prevent mutation of the cache
	out := make([]*LeaderboardRecord, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		out[i-startIdx] = records[i]
	}

	return out, nextCursor, nil
}

// GetLeaderboardRecordsAroundPlayer retrieves records centered around the target player's rank.
func GetLeaderboardRecordsAroundPlayer(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, leaderboardID, ownerID string, limit int, expiryTime time.Time) ([]*LeaderboardRecord, error) {
	if limit <= 0 {
		limit = 5
	}

	records, err := getOrBuildRankCache(ctx, pool, leaderboardID, expiryTime)
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
	for i := startIdx; i < endIdx; i++ {
		out[i-startIdx] = records[i]
	}

	return out, nil
}

func getOrBuildRankCache(ctx context.Context, pool *pgxpool.Pool, leaderboardID string, expiryTime time.Time) ([]*LeaderboardRecord, error) {
	key := fmt.Sprintf("%s:%d", leaderboardID, expiryTime.Unix())

	// 1. Read Lock Check
	localCache.mu.RLock()
	cached, exists := localCache.cache[key]
	localCache.mu.RUnlock()
	if exists {
		return cached, nil
	}

	// 2. Write Lock build
	localCache.mu.Lock()
	defer localCache.mu.Unlock()

	// Double-checked locking
	if cached, exists = localCache.cache[key]; exists {
		return cached, nil
	}

	// Load leaderboard SortOrder
	lbQuery := `SELECT sort_order FROM leaderboard WHERE id = $1`
	var sortOrder int
	err := pool.QueryRow(ctx, lbQuery, leaderboardID).Scan(&sortOrder)
	if err == pgx.ErrNoRows {
		return nil, ErrLeaderboardNotFound
	} else if err != nil {
		return nil, err
	}

	// Load all records from PostgreSQL
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
		err = rows.Scan(
			&r.OwnerID, &r.Username, &r.Score, &r.Subscore, &r.NumScore, &r.MaxNumScore,
			&r.Metadata, &r.CreateTime, &r.UpdateTime, &r.ExpiryTime,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	// Sort records strictly to guarantee unique rank determination
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
			return r1.UpdateTime.Before(r2.UpdateTime) // Earliest submission wins
		}
		return r1.OwnerID < r2.OwnerID // Stable tie-breaker
	})

	// Assign dense/sequential ranks
	for i, r := range records {
		r.Rank = int64(i + 1)
	}

	localCache.cache[key] = records
	return records, nil
}

// DeleteLeaderboard deletes a leaderboard configuration and all its records.
func DeleteLeaderboard(ctx context.Context, pool *pgxpool.Pool, id string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DELETE FROM leaderboard_record WHERE leaderboard_id = $1", id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "DELETE FROM leaderboard WHERE id = $1", id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
