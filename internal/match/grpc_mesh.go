package match

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"ultimate-game-server/internal/runtime"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Router maps active match IDs to their local loop execution thread.
// It abstracts cross-node routing by providing local in-memory lookups and falling back to cluster forwarding.
type Router struct {
	mu               sync.RWMutex
	matches          map[string]*MatchLoop
	rdb              *redis.Client
	nodeID           string
	clusterForwarder func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error

	HookRegistry     *runtime.HookRegistry
	Logger           runtime.Logger
	ZapLogger        *zap.Logger
	DB               *sql.DB
	NK               runtime.RuntimeModule
}

// NewRouter creates a new match Router.
func NewRouter() *Router {
	return &Router{
		matches: make(map[string]*MatchLoop),
	}
}

// SetDependencies injects dependencies required to instantiate Go native matches.
func (r *Router) SetDependencies(hr *runtime.HookRegistry, logger runtime.Logger, zapLogger *zap.Logger, db *sql.DB, nk runtime.RuntimeModule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.HookRegistry = hr
	r.Logger = logger
	r.ZapLogger = zapLogger
	r.DB = db
	r.NK = nk
}

// CreateAndRegisterMatch instantiates a Go native match and registers its MatchLoop.
func (r *Router) CreateAndRegisterMatch(ctx context.Context, matchID string, module string, params map[string]interface{}) error {
	r.mu.RLock()
	hr := r.HookRegistry
	logger := r.Logger
	zapLogger := r.ZapLogger
	db := r.DB
	nk := r.NK
	r.mu.RUnlock()

	if hr == nil {
		return errors.New("HookRegistry is not set on Match Router")
	}

	factory, ok := hr.GetMatch(module)
	if !ok {
		return fmt.Errorf("match handler factory %q not found", module)
	}

	// Instantiate match
	goMatch, err := factory(ctx, logger, db, nk)
	if err != nil {
		return fmt.Errorf("failed to instantiate match %q: %w", module, err)
	}

	// Initialize state
	state, tickRate, _ := goMatch.MatchInit(ctx, logger, db, nk, params)

	// Create MatchLoop
	loop := NewMatchLoop(matchID, nil, tickRate, zapLogger, nil, nil)
	loop.SetGoMatch(goMatch, state, logger, db, nk)

	// Register in Router
	r.Register(matchID, loop)

	// Start tick loop in goroutine
	go loop.Start(context.Background())

	return nil
}

// SetClusterConfig configures the router for multi-node operations.
func (r *Router) SetClusterConfig(nodeID string, rdb *redis.Client, forwarder func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeID = nodeID
	r.rdb = rdb
	r.clusterForwarder = forwarder
}

// Register adds a match loop to the routing registry.
func (r *Router) Register(matchID string, loop *MatchLoop) {
	r.mu.Lock()
	r.matches[matchID] = loop
	rdb := r.rdb
	nodeID := r.nodeID
	r.mu.Unlock()

	if rdb != nil && nodeID != "" {
		err := rdb.Set(context.Background(), "match:node:"+matchID, nodeID, 0).Err()
		if err != nil {
			if r.ZapLogger != nil {
				r.ZapLogger.Error("Failed to register match in Redis", zap.String("match_id", matchID), zap.Error(err))
			}
		}
	}
}

// Unregister removes a match loop from the registry.
func (r *Router) Unregister(matchID string) {
	r.mu.Lock()
	delete(r.matches, matchID)
	rdb := r.rdb
	r.mu.Unlock()

	if rdb != nil {
		err := rdb.Del(context.Background(), "match:node:"+matchID).Err()
		if err != nil {
			if r.ZapLogger != nil {
				r.ZapLogger.Error("Failed to delete match from Redis", zap.String("match_id", matchID), zap.Error(err))
			}
		}
	}
}

// GetMatchLoop retrieves a match loop from the registry.
func (r *Router) GetMatchLoop(matchID string) (*MatchLoop, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	loop, exists := r.matches[matchID]
	return loop, exists
}

// ForwardInput routes client inputs to the targeted match loop, forwarding to peer nodes if needed.
func (r *Router) ForwardInput(ctx context.Context, matchID string, input MatchInput) error {
	r.mu.RLock()
	loop, exists := r.matches[matchID]
	nodeID := r.nodeID
	rdb := r.rdb
	forwarder := r.clusterForwarder
	r.mu.RUnlock()

	if exists {
		loop.SubmitInput(input)
		return nil
	}

	// Fallback to cluster lookup if configured
	if rdb != nil && forwarder != nil {
		targetNodeID, err := rdb.Get(ctx, "match:node:"+matchID).Result()
		if err == redis.Nil {
			return errors.New("match not found in cluster registry")
		} else if err != nil {
			return err
		}

		if targetNodeID != nodeID {
			return forwarder(ctx, targetNodeID, matchID, input)
		}
	}

	return errors.New("match not found on this node")
}
