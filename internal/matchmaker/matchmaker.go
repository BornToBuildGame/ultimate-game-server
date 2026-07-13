package matchmaker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"

	"github.com/blevesearch/bleve/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Ticket represents a player's entry in the matchmaking queue.
type Ticket struct {
	ID                string             `json:"id"`
	UserID            string             `json:"user_id"`
	Username          string             `json:"username"`
	SkillRating       int                `json:"skill_rating"`
	Region            string             `json:"region"`
	CreatedAt         time.Time          `json:"created_at"`
	Query             string             `json:"query"`
	MinCount          int                `json:"min_count"`
	MaxCount          int                `json:"max_count"`
	StringProperties  map[string]string  `json:"string_properties"`
	NumericProperties map[string]float64 `json:"numeric_properties"`
	CountMultiple     int                `json:"count_multiple"`
	ReversePrecision  bool               `json:"reverse_precision"`
	QueueName         string             `json:"queue_name"`
	Status            string             `json:"status"`
	MatchID           string             `json:"match_id"`
	MatchToken        string             `json:"match_token"`
}

// MatchResult represents a successful matchmaking pairing.
type MatchResult struct {
	MatchID    string   `json:"match_id"`
	PlayerIDs  []string `json:"player_ids"`
	Usernames  []string `json:"usernames"`
	MatchToken string   `json:"match_token"`
	QueueName  string   `json:"queue_name"`
}

// SkillMatchConfig configuration for skill-based matchmaking.
type SkillMatchConfig struct {
	Enabled              bool    `json:"enabled" yaml:"enabled"`
	InitialRange         float64 `json:"initial_range" yaml:"initial_range"`
	MaxRange             float64 `json:"max_range" yaml:"max_range"`
	ExpansionIntervalSec int     `json:"expansion_interval_sec" yaml:"expansion_interval_sec"`
	ExpansionStep        float64 `json:"expansion_step" yaml:"expansion_step"`
}

// RegionMatchConfig configuration for region-based matchmaking.
type RegionMatchConfig struct {
	Strict           bool `json:"strict" yaml:"strict"`
	FallbackDelaySec int  `json:"fallback_delay_sec" yaml:"fallback_delay_sec"`
}

// ReversePrecisionConfig configuration for bidirectional validation.
type ReversePrecisionConfig struct {
	Enabled               bool `json:"enabled" yaml:"enabled"`
	ReverseThresholdTicks int  `json:"reverse_threshold_ticks" yaml:"reverse_threshold_ticks"`
}

// QueueConfig configuration for an individual matchmaking queue.
type QueueConfig struct {
	Name             string                 `json:"name" yaml:"name"`
	MinPlayers       int                    `json:"min_players" yaml:"min_players"`
	MaxPlayers       int                    `json:"max_players" yaml:"max_players"`
	CountMultiple    int                    `json:"count_multiple" yaml:"count_multiple"`
	SkillMatch       SkillMatchConfig       `json:"skill_match" yaml:"skill_match"`
	RegionMatch      RegionMatchConfig      `json:"region_match" yaml:"region_match"`
	ReversePrecision ReversePrecisionConfig `json:"reverse_precision" yaml:"reverse_precision"`
}

// MatchmakerConfig overall configuration for matchmaking service.
type MatchmakerConfig struct {
	ProcessingIntervalMs int                    `json:"processing_interval_ms" yaml:"processing_interval_ms"`
	TicketExpirySec      int                    `json:"ticket_expiry_sec" yaml:"ticket_expiry_sec"`
	Queues               map[string]QueueConfig `json:"queues" yaml:"queues"`
}

// DefaultConfig returns default matchmaking configurations.
func DefaultConfig() *MatchmakerConfig {
	return &MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		Queues: map[string]QueueConfig{
			"default": {
				Name:          "default",
				MinPlayers:    2,
				MaxPlayers:    2,
				CountMultiple: 1,
				SkillMatch: SkillMatchConfig{
					Enabled:              true,
					InitialRange:         50,
					MaxRange:             500,
					ExpansionIntervalSec: 5,
					ExpansionStep:        25,
				},
				RegionMatch: RegionMatchConfig{
					Strict:           false,
					FallbackDelaySec: 15,
				},
				ReversePrecision: ReversePrecisionConfig{
					Enabled:               true,
					ReverseThresholdTicks: 10,
				},
			},
		},
	}
}

// Matchmaker processes matchmaking queues.
type Matchmaker struct {
	mu           sync.Mutex
	logger       *zap.Logger
	rdb          *redis.Client
	db           *sql.DB
	nk           runtime.RuntimeModule
	hookRegistry *runtime.HookRegistry
	onMatched    func(result MatchResult)

	config *MatchmakerConfig
	nodeID string

	// Local state fallbacks (used if rdb is nil)
	tickets      map[string]*Ticket
	ticketStatus map[string]string // ticketID -> JSON status payload string

	tickTicker *time.Ticker
	stopChan   chan struct{}
}

// NewMatchmaker creates a new Matchmaker instance.
func NewMatchmaker(
	logger *zap.Logger,
	rdb *redis.Client,
	db *sql.DB,
	nk runtime.RuntimeModule,
	hookRegistry *runtime.HookRegistry,
	onMatched func(result MatchResult),
) *Matchmaker {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Matchmaker{
		logger:       logger,
		rdb:          rdb,
		db:           db,
		nk:           nk,
		hookRegistry: hookRegistry,
		onMatched:    onMatched,
		config:       DefaultConfig(),
		nodeID:       uuid.New().String(),
		tickets:      make(map[string]*Ticket),
		ticketStatus: make(map[string]string),
		stopChan:     make(chan struct{}),
	}
}

// Configure updates the matchmaking configurations.
func (mm *Matchmaker) Configure(cfg *MatchmakerConfig) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if cfg != nil {
		mm.config = cfg
	}
}

// SetDependencies allows updating dependencies dynamically after initialization.
func (mm *Matchmaker) SetDependencies(db *sql.DB, nk runtime.RuntimeModule, hookRegistry *runtime.HookRegistry) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.db = db
	mm.nk = nk
	mm.hookRegistry = hookRegistry
}


// Submit adds a ticket to the matchmaking queue.
func (mm *Matchmaker) Submit(ctx context.Context, t *Ticket) error {
	if t.ID == "" {
		t.ID = uuid.New().String()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if t.QueueName == "" {
		t.QueueName = "default"
	}
	t.Status = "queued"

	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Enforce 1 active ticket per user
	if mm.rdb != nil {
		// Verify if user already has ticket
		userKey := "matchmaker:user:" + t.UserID
		exists, err := mm.rdb.Exists(ctx, userKey).Result()
		if err == nil && exists > 0 {
			oldTicketID, err := mm.rdb.Get(ctx, userKey).Result()
			if err == nil && oldTicketID != "" {
				// Cancel old ticket
				mm.cancelLocked(ctx, oldTicketID)
			}
		}

		payload, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal ticket: %w", err)
		}

		tx := mm.rdb.TxPipeline()
		tx.Set(ctx, userKey, t.ID, time.Duration(mm.config.TicketExpirySec)*time.Second)
		tx.HSet(ctx, "matchmaker:ticket:"+t.ID, "payload", string(payload))
		tx.ZAdd(ctx, "matchmaker:queue:"+t.QueueName, redis.Z{
			Score:  float64(t.CreatedAt.Unix()),
			Member: t.ID,
		})
		// Set status record
		tx.Set(ctx, "matchmaker:status:"+t.ID, string(payload), 5*time.Minute)
		_, err = tx.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to submit ticket to Redis: %w", err)
		}
	} else {
		// Local fallback
		for _, ticket := range mm.tickets {
			if ticket.UserID == t.UserID {
				delete(mm.tickets, ticket.ID)
			}
		}
		mm.tickets[t.ID] = t
		payload, _ := json.Marshal(t)
		mm.ticketStatus[t.ID] = string(payload)
	}

	mm.logger.Debug("Matchmaking ticket submitted", zap.String("ticket_id", t.ID), zap.String("user_id", t.UserID))
	return nil
}

// Cancel removes a ticket from the matchmaking queue.
func (mm *Matchmaker) Cancel(ctx context.Context, ticketID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return mm.cancelLocked(ctx, ticketID)
}

func (mm *Matchmaker) cancelLocked(ctx context.Context, ticketID string) error {
	if mm.rdb != nil {
		// Fetch ticket to get user_id and queue_name
		payload, err := mm.rdb.HGet(ctx, "matchmaker:ticket:"+ticketID, "payload").Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil // already deleted
			}
			return fmt.Errorf("failed to get ticket for cancel: %w", err)
		}

		var t Ticket
		if err := json.Unmarshal([]byte(payload), &t); err != nil {
			return fmt.Errorf("failed to unmarshal ticket for cancel: %w", err)
		}
		t.Status = "cancelled"
		cancelledPayload, _ := json.Marshal(t)

		tx := mm.rdb.TxPipeline()
		tx.Del(ctx, "matchmaker:user:"+t.UserID)
		tx.Del(ctx, "matchmaker:ticket:"+ticketID)
		tx.ZRem(ctx, "matchmaker:queue:"+t.QueueName, ticketID)
		tx.Set(ctx, "matchmaker:status:"+ticketID, string(cancelledPayload), 5*time.Minute)
		_, err = tx.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to cancel ticket in Redis: %w", err)
		}
	} else {
		t, ok := mm.tickets[ticketID]
		if ok {
			t.Status = "cancelled"
			cancelledPayload, _ := json.Marshal(t)
			mm.ticketStatus[ticketID] = string(cancelledPayload)
			delete(mm.tickets, ticketID)
		}
	}

	mm.logger.Debug("Matchmaking ticket cancelled", zap.String("ticket_id", ticketID))
	return nil
}

// GetTicket retrieves a ticket status (either active or historical).
func (mm *Matchmaker) GetTicket(ctx context.Context, ticketID string) (*Ticket, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.rdb != nil {
		// First try status mapping (active or historical)
		payload, err := mm.rdb.Get(ctx, "matchmaker:status:"+ticketID).Result()
		if err == nil && payload != "" {
			var t Ticket
			if err := json.Unmarshal([]byte(payload), &t); err == nil {
				return &t, nil
			}
		}

		// Fallback to active ticket
		payload, err = mm.rdb.HGet(ctx, "matchmaker:ticket:"+ticketID, "payload").Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, errors.New("ticket not found")
			}
			return nil, err
		}
		var t Ticket
		if err := json.Unmarshal([]byte(payload), &t); err != nil {
			return nil, err
		}
		return &t, nil
	}

	payload, ok := mm.ticketStatus[ticketID]
	if ok {
		var t Ticket
		if err := json.Unmarshal([]byte(payload), &t); err == nil {
			return &t, nil
		}
	}

	t, ok := mm.tickets[ticketID]
	if !ok {
		return nil, errors.New("ticket not found")
	}
	return t, nil
}

// Start runs the matchmaking tick loop.
func (mm *Matchmaker) Start(ctx context.Context, interval time.Duration) {
	mm.tickTicker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-mm.tickTicker.C:
				mm.Tick(ctx)
			case <-mm.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the matchmaking loop.
func (mm *Matchmaker) Stop() {
	if mm.tickTicker != nil {
		mm.tickTicker.Stop()
	}
	close(mm.stopChan)
}

// GetQueueStats retrieves queue statistics.
func (mm *Matchmaker) GetQueueStats(ctx context.Context, queueName string) (int, int, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.rdb != nil {
		count, err := mm.rdb.ZCard(ctx, "matchmaker:queue:"+queueName).Result()
		if err != nil {
			return 0, 0, err
		}
		return int(count), 0, nil
	}

	count := 0
	for _, t := range mm.tickets {
		if t.QueueName == queueName {
			count++
		}
	}
	return count, 0, nil
}

// Tick evaluates candidate tickets, pairing them based on region, skill, and query.
func (mm *Matchmaker) Tick(ctx context.Context) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	queuesToProcess := []string{"default"}
	if mm.config != nil {
		queuesToProcess = make([]string, 0, len(mm.config.Queues))
		for qName := range mm.config.Queues {
			queuesToProcess = append(queuesToProcess, qName)
		}
	}

	for _, queueName := range queuesToProcess {
		mm.processQueue(ctx, queueName)
	}
}

func (mm *Matchmaker) processQueue(ctx context.Context, queueName string) {
	queueCfg, exists := mm.config.Queues[queueName]
	if !exists {
		queueCfg = mm.config.Queues["default"]
		queueCfg.Name = queueName
	}

	// 1. Multi-node coordination lock
	if mm.rdb != nil {
		lockKey := "lock:matchmaker:" + queueName
		// Acquire lock for 800ms
		ok, err := mm.rdb.SetNX(ctx, lockKey, mm.nodeID, 800*time.Millisecond).Result()
		if err != nil || !ok {
			return // Lock acquired by other node, or error
		}
		defer func() {
			// Release lock if owned by this node
			val, err := mm.rdb.Get(ctx, lockKey).Result()
			if err == nil && val == mm.nodeID {
				mm.rdb.Del(ctx, lockKey)
			}
		}()
	}

	// 2. Fetch active tickets
	var activeTickets []*Ticket
	now := time.Now()

	if mm.rdb != nil {
		ticketIDs, err := mm.rdb.ZRangeByScore(ctx, "matchmaker:queue:"+queueName, &redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
		if err != nil || len(ticketIDs) == 0 {
			return
		}

		// Fetch payloads in pipeline
		pipe := mm.rdb.Pipeline()
		for _, tid := range ticketIDs {
			pipe.HGet(ctx, "matchmaker:ticket:"+tid, "payload")
		}
		cmds, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			mm.logger.Error("Failed to fetch tickets pipeline", zap.Error(err))
			return
		}

		for _, cmd := range cmds {
			payload, err := cmd.(*redis.StringCmd).Result()
			if err != nil {
				continue
			}
			var t Ticket
			if err := json.Unmarshal([]byte(payload), &t); err == nil {
				// Enforce ticket expiration GC
				if now.Sub(t.CreatedAt).Seconds() > float64(mm.config.TicketExpirySec) {
					mm.logger.Info("Garbage collecting expired ticket", zap.String("ticket_id", t.ID))
					t.Status = "expired"
					expiredPayload, _ := json.Marshal(t)
					mm.rdb.Del(ctx, "matchmaker:user:"+t.UserID)
					mm.rdb.Del(ctx, "matchmaker:ticket:"+t.ID)
					mm.rdb.ZRem(ctx, "matchmaker:queue:"+queueName, t.ID)
					mm.rdb.Set(ctx, "matchmaker:status:"+t.ID, string(expiredPayload), 5*time.Minute)
					continue
				}
				activeTickets = append(activeTickets, &t)
			}
		}
	} else {
		// Local state GC and collection
		for id, t := range mm.tickets {
			if t.QueueName != queueName {
				continue
			}
			if now.Sub(t.CreatedAt).Seconds() > float64(mm.config.TicketExpirySec) {
				t.Status = "expired"
				expiredPayload, _ := json.Marshal(t)
				mm.ticketStatus[t.ID] = string(expiredPayload)
				delete(mm.tickets, id)
				continue
			}
			activeTickets = append(activeTickets, t)
		}
	}

	if len(activeTickets) < queueCfg.MinPlayers {
		return
	}

	// 3. Pairing algorithm
	matchedTicketIDs := make(map[string]bool)

	for i := 0; i < len(activeTickets); i++ {
		t1 := activeTickets[i]
		if matchedTicketIDs[t1.ID] {
			continue
		}

		matchGroup := []*Ticket{t1}

		for j := i + 1; j < len(activeTickets); j++ {
			t2 := activeTickets[j]
			if matchedTicketIDs[t2.ID] {
				continue
			}

			// Validate compatibility
			if mm.evaluatePairing(t1, t2, &queueCfg, now) {
				matchGroup = append(matchGroup, t2)
				if len(matchGroup) == queueCfg.MaxPlayers {
					break
				}
			}
		}

		// Verify minimum constraints
		if len(matchGroup) >= queueCfg.MinPlayers {
			// Apply count_multiple check
			if queueCfg.CountMultiple > 1 {
				rem := len(matchGroup) % queueCfg.CountMultiple
				if rem != 0 {
					// Slice down to largest multiple
					validSize := len(matchGroup) - rem
					if validSize >= queueCfg.MinPlayers {
						matchGroup = matchGroup[:validSize]
					} else {
						continue // Cannot satisfy count_multiple
					}
				}
			}

			// Complete match formed!
			for _, t := range matchGroup {
				matchedTicketIDs[t.ID] = true
			}

			// Finalize Match
			mm.finalizeMatch(ctx, matchGroup, queueName)
		}
	}
}

func (mm *Matchmaker) evaluatePairing(t1, t2 *Ticket, queueCfg *QueueConfig, now time.Time) bool {
	// 1. Region match check
	wait1 := now.Sub(t1.CreatedAt).Seconds()
	wait2 := now.Sub(t2.CreatedAt).Seconds()
	strictRegion := queueCfg.RegionMatch.Strict

	if t1.Region != t2.Region {
		if strictRegion {
			return false
		}
		// Fallback delay check
		fallbackDelay := float64(queueCfg.RegionMatch.FallbackDelaySec)
		if wait1 < fallbackDelay || wait2 < fallbackDelay {
			return false
		}
	}

	// 2. Skill MMR check with progressive expansion
	if queueCfg.SkillMatch.Enabled {
		delta1 := getSkillDelta(wait1)
		delta2 := getSkillDelta(wait2)
		maxAllowedDelta := delta1
		if delta2 > maxAllowedDelta {
			maxAllowedDelta = delta2
		}

		actualDelta := t1.SkillRating - t2.SkillRating
		if actualDelta < 0 {
			actualDelta = -actualDelta
		}
		if actualDelta > maxAllowedDelta {
			return false
		}
	}

	// 3. Bleve custom properties matching
	if t1.Query != "" || t2.Query != "" {
		mapping := bleve.NewIndexMapping()
		index, err := bleve.NewMemOnly(mapping)
		if err != nil {
			return false
		}

		// Index t2
		doc2 := map[string]interface{}{
			"properties": mergeProperties(t2),
			"skill":      t2.SkillRating,
			"region":     t2.Region,
		}
		_ = index.Index("t2", doc2)

		if t1.Query != "" {
			q := bleve.NewQueryStringQuery(t1.Query)
			req := bleve.NewSearchRequest(q)
			res, err := index.Search(req)
			if err != nil || res.Total == 0 {
				return false
			}
		}

		// Bidirectional reverse precision check
		isReverseEnabled := queueCfg.ReversePrecision.Enabled
		if isReverseEnabled && t2.Query != "" {
			thresholdTicks := queueCfg.ReversePrecision.ReverseThresholdTicks
			if thresholdTicks <= 0 {
				thresholdTicks = 10
			}

			// Bidirectional check only within threshold wait times
			if wait1 < float64(thresholdTicks) && wait2 < float64(thresholdTicks) {
				index1, err := bleve.NewMemOnly(mapping)
				if err == nil {
					doc1 := map[string]interface{}{
						"properties": mergeProperties(t1),
						"skill":      t1.SkillRating,
						"region":     t1.Region,
					}
					_ = index1.Index("t1", doc1)
					q2 := bleve.NewQueryStringQuery(t2.Query)
					req2 := bleve.NewSearchRequest(q2)
					res2, err := index1.Search(req2)
					if err != nil || res2.Total == 0 {
						return false
					}
				}
			}
		}
	}

	return true
}

func getSkillDelta(waitSeconds float64) int {
	if waitSeconds < 5 {
		return 50
	}
	if waitSeconds < 10 {
		return 75
	}
	if waitSeconds < 20 {
		return 125
	}
	if waitSeconds < 30 {
		return 200
	}
	if waitSeconds < 60 {
		return 350
	}
	return 500
}

func mergeProperties(t *Ticket) map[string]interface{} {
	props := make(map[string]interface{})
	for k, v := range t.StringProperties {
		props[k] = v
	}
	for k, v := range t.NumericProperties {
		props[k] = v
	}
	return props
}

func (mm *Matchmaker) finalizeMatch(ctx context.Context, tickets []*Ticket, queueName string) {
	playerIDs := make([]string, len(tickets))
	usernames := make([]string, len(tickets))
	for idx, t := range tickets {
		playerIDs[idx] = t.UserID
		usernames[idx] = t.Username
	}

	// 1. Remove tickets from queue
	if mm.rdb != nil {
		tx := mm.rdb.TxPipeline()
		for _, t := range tickets {
			tx.Del(ctx, "matchmaker:user:"+t.UserID)
			tx.Del(ctx, "matchmaker:ticket:"+t.ID)
			tx.ZRem(ctx, "matchmaker:queue:"+queueName, t.ID)
		}
		_, err := tx.Exec(ctx)
		if err != nil {
			mm.logger.Error("Failed to delete matched tickets in Redis transaction", zap.Error(err))
			return
		}
	} else {
		for _, t := range tickets {
			delete(mm.tickets, t.ID)
		}
	}

	// 2. Trigger runtime hook matchmaker_matched
	matchID := ""
	if mm.hookRegistry != nil {
		handler := mm.hookRegistry.GetMatchmakerMatched()
		if handler != nil {
			entries := make([]interface{}, len(tickets))
			for idx, t := range tickets {
				entries[idx] = map[string]interface{}{
					"ticket_id":          t.ID,
					"user_id":            t.UserID,
					"username":           t.Username,
					"skill_rating":       t.SkillRating,
					"region":             t.Region,
					"created_at":         t.CreatedAt.Unix(),
					"string_properties":  t.StringProperties,
					"numeric_properties": t.NumericProperties,
				}
			}

			// Execute hook
			var err error
			matchID, err = handler(ctx, &runtimeLogger{zapLogger: mm.logger}, mm.db, mm.nk, entries)
			if err != nil {
				mm.logger.Warn("matchmaker_matched runtime hook returned error, falling back", zap.Error(err))
			}
		}
	}

	// 3. Fallback to default Match ID and spawn authoritative match loop
	if matchID == "" {
		matchID = "match_" + uuid.New().String()
	}

	// Generate secure token
	tokenBytes := make([]byte, 16)
	_, _ = rand.Read(tokenBytes)
	matchToken := hex.EncodeToString(tokenBytes)

	// Register match in Redis (if distributed)
	if mm.rdb != nil {
		matchKey := "matchmaker:match:" + matchID
		_ = mm.rdb.HSet(ctx, matchKey, map[string]interface{}{
			"players":     playerIDs,
			"match_token": matchToken,
			"region":      tickets[0].Region,
			"node_id":     mm.nodeID,
		}).Err()
	}

	// Notify and update ticket status records
	for _, t := range tickets {
		t.Status = "matched"
		t.MatchID = matchID
		t.MatchToken = matchToken
		matchedPayload, _ := json.Marshal(t)
		if mm.rdb != nil {
			mm.rdb.Set(ctx, "matchmaker:status:"+t.ID, string(matchedPayload), 5*time.Minute)
		} else {
			mm.ticketStatus[t.ID] = string(matchedPayload)
		}
	}

	result := MatchResult{
		MatchID:    matchID,
		PlayerIDs:  playerIDs,
		Usernames:  usernames,
		MatchToken: matchToken,
		QueueName:  queueName,
	}

	if mm.onMatched != nil {
		go mm.onMatched(result)
	}

	mm.logger.Info("Match formed successfully!", zap.String("match_id", matchID), zap.Int("players_count", len(playerIDs)))
}

type runtimeLogger struct {
	zapLogger *zap.Logger
}

func (l *runtimeLogger) Debug(format string, args ...interface{}) {
	l.zapLogger.Debug(fmt.Sprintf(format, args...))
}

func (l *runtimeLogger) Info(format string, args ...interface{}) {
	l.zapLogger.Info(fmt.Sprintf(format, args...))
}

func (l *runtimeLogger) Warn(format string, args ...interface{}) {
	l.zapLogger.Warn(fmt.Sprintf(format, args...))
}

func (l *runtimeLogger) Error(format string, args ...interface{}) {
	l.zapLogger.Error(fmt.Sprintf(format, args...))
}
