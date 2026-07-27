package matchmaker

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"ultimate-game-server/internal/runtime"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	ErrTooManyTickets = errors.New("too many tickets for session")
	ErrRateLimited    = errors.New("matchmaker rate limited")
	ErrInvalidTicket  = errors.New("invalid ticket parameters")
	ErrTokenInvalid   = errors.New("match token invalid or expired")
)

// Presence is a player bound to a matchmaker ticket.
type Presence struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	SessionID string `json:"session_id"`
	Node      string `json:"node,omitempty"`
}

// Ticket represents a player's (or party's) entry in the matchmaking pool.
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
	CountMultiple     int                `json:"count_multiple"`
	Count             int                `json:"count"`
	PartyID           string             `json:"party_id"`
	SessionID         string             `json:"session_id"`
	Presences         []*Presence        `json:"presences,omitempty"`
	Intervals         int                `json:"intervals"`
	StringProperties  map[string]string  `json:"string_properties"`
	NumericProperties map[string]float64 `json:"numeric_properties"`
	ReversePrecision  bool               `json:"reverse_precision"`
	QueueName         string             `json:"queue_name"`
	Status            string             `json:"status"`
	MatchID           string             `json:"match_id"`
	MatchToken        string             `json:"match_token"`
}

// MatchResult represents a successful matchmaking pairing.
type MatchResult struct {
	MatchID       string               `json:"match_id"`
	PlayerIDs     []string             `json:"player_ids"`
	Usernames     []string             `json:"usernames"`
	Users         []*Presence          `json:"users"`
	MatchedUsers  []MatchedUser        `json:"matched_users,omitempty"`
	TicketIDs     map[string]string    `json:"ticket_ids"` // user_id -> ticket_id
	MatchToken    string               `json:"match_token"`
	QueueName     string               `json:"queue_name"`
	Authoritative bool                 `json:"authoritative"`
	Module        string               `json:"module"`
}

// MatchedUser is a reference-shaped matchmaker_matched user entry.
type MatchedUser struct {
	UserID            string             `json:"user_id"`
	Username          string             `json:"username"`
	SessionID         string             `json:"session_id"`
	PartyID           string             `json:"party_id,omitempty"`
	TicketID          string             `json:"ticket_id,omitempty"`
	StringProperties  map[string]string  `json:"string_properties,omitempty"`
	NumericProperties map[string]float64 `json:"numeric_properties,omitempty"`
}

// CompletionRecord tracks a recently completed match for stats.
type CompletionRecord struct {
	MatchID     string    `json:"match_id"`
	CompletedAt time.Time `json:"completed_at"`
	PlayerCount int       `json:"player_count"`
}

// Stats is aggregate matchmaker health information.
type Stats struct {
	TicketCount            int                `json:"ticket_count"`
	OldestTicketCreateTime time.Time          `json:"oldest_ticket_create_time"`
	Completions            []CompletionRecord `json:"completions"`
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

// QueueConfig configuration for an individual matchmaking queue partition.
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
	MaxTickets           int                    `json:"max_tickets" yaml:"max_tickets"`
	MaxIntervals         int                    `json:"max_intervals" yaml:"max_intervals"`
	TokenTTLSec          int                    `json:"token_ttl_sec" yaml:"token_ttl_sec"`
	RevPrecision         bool                   `json:"rev_precision" yaml:"rev_precision"`
	UseRedlock           bool                   `json:"use_redlock" yaml:"use_redlock"`
	RateLimitMax         int                    `json:"rate_limit_max" yaml:"rate_limit_max"`
	RateLimitWindowSec   int                    `json:"rate_limit_window_sec" yaml:"rate_limit_window_sec"`
	TokenSecret          string                 `json:"-" yaml:"-"`
	Queues               map[string]QueueConfig `json:"queues" yaml:"queues"`
}

// MatchmakerProcessorHandler replaces default matching for a queue tick.
// Return nil to form no matches this tick; otherwise each inner slice is a match group.
type MatchmakerProcessorHandler func(ctx context.Context, tickets []*Ticket) [][]*Ticket

// MatchmakerOverrideHandler rewrites candidate groups after default matching.
type MatchmakerOverrideHandler func(ctx context.Context, candidates [][]*Ticket) [][]*Ticket

// DefaultConfig returns default matchmaking configurations.
func DefaultConfig() *MatchmakerConfig {
	return &MatchmakerConfig{
		ProcessingIntervalMs: 1000,
		TicketExpirySec:      300,
		MaxTickets:           3,
		MaxIntervals:         2,
		TokenTTLSec:          30,
		RevPrecision:         false,
		UseRedlock:           false,
		RateLimitMax:         10,
		RateLimitWindowSec:   300,
		TokenSecret:          "uge-matchmaker-default-secret",
		Queues: map[string]QueueConfig{
			"default": {
				Name:          "default",
				MinPlayers:    2,
				MaxPlayers:    8,
				CountMultiple: 1,
				SkillMatch: SkillMatchConfig{
					Enabled:              false,
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
					Enabled:               false,
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
	onSpawnMatch func(result MatchResult)

	config *MatchmakerConfig
	nodeID string

	tickets        map[string]*Ticket
	ticketStatus   map[string]string
	sessionTickets map[string]map[string]struct{} // sessionID -> set(ticketID)
	submitTimes    map[string][]time.Time         // userID -> submit timestamps
	matchTokens    map[string]*matchTokenRecord   // token -> record
	completions    []CompletionRecord             // ring of last 10

	processorHandler MatchmakerProcessorHandler
	overrideHandler  MatchmakerOverrideHandler

	tickTicker *time.Ticker
	stopChan   chan struct{}
	stopOnce   sync.Once
	started    bool
}

type matchTokenRecord struct {
	MatchID   string
	Users     []*Presence
	TicketIDs map[string]string
	ExpiresAt time.Time
	Consumed  bool
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
		logger:         logger,
		rdb:            rdb,
		db:             db,
		nk:             nk,
		hookRegistry:   hookRegistry,
		onMatched:      onMatched,
		config:         DefaultConfig(),
		nodeID:         uuid.New().String(),
		tickets:        make(map[string]*Ticket),
		ticketStatus:   make(map[string]string),
		sessionTickets: make(map[string]map[string]struct{}),
		submitTimes:    make(map[string][]time.Time),
		matchTokens:    make(map[string]*matchTokenRecord),
		completions:    make([]CompletionRecord, 0, 10),
		stopChan:       make(chan struct{}),
	}
}

// Configure updates the matchmaking configurations.
func (mm *Matchmaker) Configure(cfg *MatchmakerConfig) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if cfg != nil {
		mm.normalizeConfig(cfg)
		mm.config = cfg
	}
}

func (mm *Matchmaker) normalizeConfig(cfg *MatchmakerConfig) {
	if cfg.MaxTickets <= 0 {
		cfg.MaxTickets = 3
	}
	if cfg.MaxIntervals <= 0 {
		cfg.MaxIntervals = 2
	}
	if cfg.TokenTTLSec <= 0 {
		cfg.TokenTTLSec = 30
	}
	if cfg.TicketExpirySec <= 0 {
		cfg.TicketExpirySec = 300
	}
	if cfg.RateLimitMax <= 0 {
		cfg.RateLimitMax = 10
	}
	if cfg.RateLimitWindowSec <= 0 {
		cfg.RateLimitWindowSec = 300
	}
	if cfg.TokenSecret == "" {
		cfg.TokenSecret = "uge-matchmaker-default-secret"
	}
	if cfg.Queues == nil {
		cfg.Queues = DefaultConfig().Queues
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

// SetSpawnMatch registers a callback invoked for authoritative matches that should be spawned.
func (mm *Matchmaker) SetSpawnMatch(fn func(result MatchResult)) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.onSpawnMatch = fn
}

// SetMatchedCallback updates the onMatched notification callback.
func (mm *Matchmaker) SetMatchedCallback(fn func(result MatchResult)) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.onMatched = fn
}

// SetProcessorHandler sets a processor that replaces default matching.
func (mm *Matchmaker) SetProcessorHandler(fn MatchmakerProcessorHandler) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.processorHandler = fn
}

// SetOverrideHandler sets an override that rewrites default candidate groups.
func (mm *Matchmaker) SetOverrideHandler(fn MatchmakerOverrideHandler) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.overrideHandler = fn
}

// Submit adds a ticket to the matchmaking queue.
func (mm *Matchmaker) Submit(ctx context.Context, t *Ticket) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return mm.submitLocked(ctx, t)
}

func (mm *Matchmaker) submitLocked(ctx context.Context, t *Ticket) error {
	if t == nil {
		return ErrInvalidTicket
	}
	if t.ID == "" {
		t.ID = uuid.New().String()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if t.QueueName == "" {
		t.QueueName = "default"
	}
	if t.SessionID == "" {
		t.SessionID = uuid.New().String()
	}
	if t.Query == "" {
		t.Query = "*"
	}

	queueCfg := mm.queueConfig(t.QueueName)
	if t.MinCount <= 0 {
		t.MinCount = queueCfg.MinPlayers
	}
	if t.MaxCount <= 0 {
		t.MaxCount = queueCfg.MaxPlayers
	}
	if t.CountMultiple <= 0 {
		t.CountMultiple = queueCfg.CountMultiple
		if t.CountMultiple <= 0 {
			t.CountMultiple = 1
		}
	}
	if t.Count <= 0 {
		if len(t.Presences) > 0 {
			t.Count = len(t.Presences)
		} else {
			t.Count = 1
		}
	}
	if len(t.Presences) == 0 {
		t.Presences = []*Presence{{
			UserID:    t.UserID,
			Username:  t.Username,
			SessionID: t.SessionID,
		}}
	}
	if t.StringProperties == nil {
		t.StringProperties = map[string]string{}
	}
	if t.NumericProperties == nil {
		t.NumericProperties = map[string]float64{}
	}
	if region, ok := t.StringProperties["region"]; ok && t.Region == "" {
		t.Region = region
	}
	t.Status = "queued"
	t.Intervals = 0

	if t.MinCount < 2 || t.MaxCount < t.MinCount || t.CountMultiple < 1 {
		return fmt.Errorf("%w: min_count/max_count/count_multiple invalid", ErrInvalidTicket)
	}
	if t.Count > t.MaxCount {
		return fmt.Errorf("%w: party/ticket count exceeds max_count", ErrInvalidTicket)
	}

	if err := mm.checkRateLimitLocked(t.UserID); err != nil {
		return err
	}

	sessionSet := mm.sessionTickets[t.SessionID]
	if sessionSet == nil {
		sessionSet = make(map[string]struct{})
		mm.sessionTickets[t.SessionID] = sessionSet
	}
	if len(sessionSet) >= mm.config.MaxTickets {
		return ErrTooManyTickets
	}

	if mm.rdb != nil {
		payload, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal ticket: %w", err)
		}

		tx := mm.rdb.TxPipeline()
		tx.Set(ctx, "matchmaker:user:"+t.UserID, t.ID, time.Duration(mm.config.TicketExpirySec)*time.Second)
		tx.HSet(ctx, "matchmaker:ticket:"+t.ID, "payload", string(payload))
		tx.ZAdd(ctx, "matchmaker:queue:"+t.QueueName, redis.Z{
			Score:  float64(t.CreatedAt.UnixNano()),
			Member: t.ID,
		})
		tx.SAdd(ctx, "matchmaker:session:"+t.SessionID, t.ID)
		tx.Expire(ctx, "matchmaker:session:"+t.SessionID, time.Duration(mm.config.TicketExpirySec)*time.Second)
		tx.Set(ctx, "matchmaker:status:"+t.ID, string(payload), 5*time.Minute)
		if _, err = tx.Exec(ctx); err != nil {
			return fmt.Errorf("failed to submit ticket to Redis: %w", err)
		}
	}

	mm.tickets[t.ID] = t
	payload, _ := json.Marshal(t)
	mm.ticketStatus[t.ID] = string(payload)
	sessionSet[t.ID] = struct{}{}

	mm.logger.Debug("Matchmaking ticket submitted",
		zap.String("ticket_id", t.ID),
		zap.String("user_id", t.UserID),
		zap.String("session_id", t.SessionID),
	)
	return nil
}

func (mm *Matchmaker) checkRateLimitLocked(userID string) error {
	now := time.Now()
	window := time.Duration(mm.config.RateLimitWindowSec) * time.Second
	cutoff := now.Add(-window)
	times := mm.submitTimes[userID]
	filtered := times[:0]
	for _, ts := range times {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	if len(filtered) >= mm.config.RateLimitMax {
		mm.submitTimes[userID] = filtered
		return ErrRateLimited
	}
	mm.submitTimes[userID] = append(filtered, now)
	return nil
}

// SubmitParty submits an atomic party ticket listing leader + members.
func (mm *Matchmaker) SubmitParty(ctx context.Context, leader *Presence, members []*Presence, ticket *Ticket) error {
	if ticket == nil {
		ticket = &Ticket{}
	}
	if leader == nil {
		return ErrInvalidTicket
	}

	presences := make([]*Presence, 0, 1+len(members))
	presences = append(presences, leader)
	seen := map[string]struct{}{leader.UserID: {}}
	for _, m := range members {
		if m == nil {
			continue
		}
		if _, ok := seen[m.UserID]; ok {
			continue
		}
		seen[m.UserID] = struct{}{}
		presences = append(presences, m)
	}

	ticket.UserID = leader.UserID
	ticket.Username = leader.Username
	ticket.SessionID = leader.SessionID
	ticket.Presences = presences
	ticket.Count = len(presences)
	if ticket.PartyID == "" {
		ticket.PartyID = uuid.New().String()
	}

	return mm.Submit(ctx, ticket)
}

// Cancel removes a ticket from the matchmaking queue.
func (mm *Matchmaker) Cancel(ctx context.Context, ticketID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return mm.cancelLocked(ctx, ticketID)
}

func (mm *Matchmaker) cancelLocked(ctx context.Context, ticketID string) error {
	var t *Ticket
	if mm.rdb != nil {
		payload, err := mm.rdb.HGet(ctx, "matchmaker:ticket:"+ticketID, "payload").Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Also try local cleanup
				if local, ok := mm.tickets[ticketID]; ok {
					t = local
				} else {
					return nil
				}
			} else {
				return fmt.Errorf("failed to get ticket for cancel: %w", err)
			}
		} else {
			var parsed Ticket
			if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
				return fmt.Errorf("failed to unmarshal ticket for cancel: %w", err)
			}
			t = &parsed
		}
	} else {
		local, ok := mm.tickets[ticketID]
		if !ok {
			return nil
		}
		t = local
	}

	t.Status = "cancelled"
	cancelledPayload, _ := json.Marshal(t)

	if mm.rdb != nil {
		tx := mm.rdb.TxPipeline()
		tx.Del(ctx, "matchmaker:user:"+t.UserID)
		tx.Del(ctx, "matchmaker:ticket:"+ticketID)
		tx.ZRem(ctx, "matchmaker:queue:"+t.QueueName, ticketID)
		tx.SRem(ctx, "matchmaker:session:"+t.SessionID, ticketID)
		tx.Set(ctx, "matchmaker:status:"+ticketID, string(cancelledPayload), 5*time.Minute)
		if _, err := tx.Exec(ctx); err != nil {
			return fmt.Errorf("failed to cancel ticket in Redis: %w", err)
		}
	}

	mm.ticketStatus[ticketID] = string(cancelledPayload)
	delete(mm.tickets, ticketID)
	if set, ok := mm.sessionTickets[t.SessionID]; ok {
		delete(set, ticketID)
		if len(set) == 0 {
			delete(mm.sessionTickets, t.SessionID)
		}
	}

	mm.logger.Debug("Matchmaking ticket cancelled", zap.String("ticket_id", ticketID))
	return nil
}

// RemovePartyAll cancels every ticket associated with the given party ID.
func (mm *Matchmaker) RemovePartyAll(ctx context.Context, partyID string) error {
	if partyID == "" {
		return nil
	}
	mm.mu.Lock()
	defer mm.mu.Unlock()

	ids := make([]string, 0)
	for id, t := range mm.tickets {
		if t != nil && t.PartyID == partyID {
			ids = append(ids, id)
		}
	}
	var firstErr error
	for _, id := range ids {
		if err := mm.cancelLocked(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RemoveSessionAll cancels every ticket owned by the session.
func (mm *Matchmaker) RemoveSessionAll(ctx context.Context, sessionID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	ids := make([]string, 0)
	if set, ok := mm.sessionTickets[sessionID]; ok {
		for id := range set {
			ids = append(ids, id)
		}
	}
	if mm.rdb != nil {
		members, err := mm.rdb.SMembers(ctx, "matchmaker:session:"+sessionID).Result()
		if err == nil {
			seen := map[string]struct{}{}
			for _, id := range ids {
				seen[id] = struct{}{}
			}
			for _, id := range members {
				if _, ok := seen[id]; !ok {
					ids = append(ids, id)
				}
			}
		}
	}

	var firstErr error
	for _, id := range ids {
		if err := mm.cancelLocked(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	delete(mm.sessionTickets, sessionID)
	if mm.rdb != nil {
		_ = mm.rdb.Del(ctx, "matchmaker:session:"+sessionID).Err()
	}
	return firstErr
}

// GetTicket retrieves a ticket status (either active or historical).
func (mm *Matchmaker) GetTicket(ctx context.Context, ticketID string) (*Ticket, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.rdb != nil {
		payload, err := mm.rdb.Get(ctx, "matchmaker:status:"+ticketID).Result()
		if err == nil && payload != "" {
			var t Ticket
			if err := json.Unmarshal([]byte(payload), &t); err == nil {
				return &t, nil
			}
		}
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

	if payload, ok := mm.ticketStatus[ticketID]; ok {
		var t Ticket
		if err := json.Unmarshal([]byte(payload), &t); err == nil {
			return &t, nil
		}
	}
	t, ok := mm.tickets[ticketID]
	if !ok {
		return nil, errors.New("ticket not found")
	}
	cp := *t
	return &cp, nil
}

// Start runs the matchmaking tick loop.
func (mm *Matchmaker) Start(ctx context.Context, interval time.Duration) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.started {
		return
	}
	mm.started = true
	mm.stopOnce = sync.Once{}
	mm.stopChan = make(chan struct{})
	mm.tickTicker = time.NewTicker(interval)
	ticker := mm.tickTicker
	stopChan := mm.stopChan
	go func() {
		for {
			select {
			case <-ticker.C:
				mm.Tick(ctx)
			case <-stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the matchmaking loop. Safe to call multiple times.
func (mm *Matchmaker) Stop() {
	mm.stopOnce.Do(func() {
		mm.mu.Lock()
		if mm.tickTicker != nil {
			mm.tickTicker.Stop()
			mm.tickTicker = nil
		}
		mm.started = false
		ch := mm.stopChan
		mm.mu.Unlock()
		if ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
}

// GetQueueStats retrieves queue statistics.
func (mm *Matchmaker) GetQueueStats(ctx context.Context, queueName string) (int, int, error) {
	stats := mm.GetStats(ctx)
	return stats.TicketCount, len(stats.Completions), nil
}

// GetStats returns ticket_count, oldest create time, and recent completions.
func (mm *Matchmaker) GetStats(ctx context.Context) Stats {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	stats := Stats{
		TicketCount: len(mm.tickets),
		Completions: append([]CompletionRecord(nil), mm.completions...),
	}
	var oldest time.Time
	for _, t := range mm.tickets {
		if oldest.IsZero() || t.CreatedAt.Before(oldest) {
			oldest = t.CreatedAt
		}
	}
	stats.OldestTicketCreateTime = oldest

	if mm.rdb != nil && stats.TicketCount == 0 {
		// Best-effort Redis count across configured queues
		for qName := range mm.config.Queues {
			n, err := mm.rdb.ZCard(ctx, "matchmaker:queue:"+qName).Result()
			if err == nil {
				stats.TicketCount += int(n)
			}
		}
	}
	return stats
}

// ConsumeMatchToken validates and consumes a single-use join token.
func (mm *Matchmaker) ConsumeMatchToken(ctx context.Context, token string) (*MatchResult, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if token == "" {
		return nil, ErrTokenInvalid
	}

	if mm.rdb != nil {
		key := "matchmaker:token:" + token
		payload, err := mm.rdb.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, ErrTokenInvalid
			}
			return nil, err
		}
		if err := mm.rdb.Del(ctx, key).Err(); err != nil {
			return nil, err
		}
		var rec matchTokenRecord
		if err := json.Unmarshal([]byte(payload), &rec); err != nil {
			return nil, ErrTokenInvalid
		}
		if time.Now().After(rec.ExpiresAt) {
			return nil, ErrTokenInvalid
		}
		return &MatchResult{
			MatchID:    rec.MatchID,
			Users:      rec.Users,
			TicketIDs:  rec.TicketIDs,
			MatchToken: token,
		}, nil
	}

	rec, ok := mm.matchTokens[token]
	if !ok || rec.Consumed || time.Now().After(rec.ExpiresAt) {
		return nil, ErrTokenInvalid
	}
	rec.Consumed = true
	return &MatchResult{
		MatchID:    rec.MatchID,
		Users:      rec.Users,
		TicketIDs:  rec.TicketIDs,
		MatchToken: token,
	}, nil
}

func (mm *Matchmaker) queueConfig(queueName string) QueueConfig {
	if mm.config != nil {
		if q, ok := mm.config.Queues[queueName]; ok {
			return q
		}
		if q, ok := mm.config.Queues["default"]; ok {
			q.Name = queueName
			return q
		}
	}
	return QueueConfig{Name: queueName, MinPlayers: 2, MaxPlayers: 8, CountMultiple: 1}
}

func (mm *Matchmaker) mintMatchToken(matchID string, users []*Presence, ticketIDs map[string]string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(mm.config.TokenSecret))
	_, _ = mac.Write([]byte(matchID))
	_, _ = mac.Write(nonce)
	token := hex.EncodeToString(mac.Sum(nil)) + hex.EncodeToString(nonce)

	rec := &matchTokenRecord{
		MatchID:   matchID,
		Users:     users,
		TicketIDs: ticketIDs,
		ExpiresAt: time.Now().Add(time.Duration(mm.config.TokenTTLSec) * time.Second),
	}
	mm.matchTokens[token] = rec
	return token, nil
}

func (mm *Matchmaker) storeMatchTokenRedis(ctx context.Context, token string, rec *matchTokenRecord) error {
	if mm.rdb == nil {
		return nil
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ttl := time.Duration(mm.config.TokenTTLSec) * time.Second
	return mm.rdb.Set(ctx, "matchmaker:token:"+token, string(payload), ttl).Err()
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
