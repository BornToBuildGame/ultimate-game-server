package match

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Router maps active match IDs to their local loop execution thread.
// It abstracts cross-node routing by providing local in-memory lookups and falling back to cluster forwarding.
type Router struct {
	mu                     sync.RWMutex
	matches                map[string]*MatchLoop
	rdb                    *redis.Client
	nodeID                 string
	clusterForwarder       func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error
	clusterSignalForwarder func(ctx context.Context, targetNodeID, matchID string, data string) error

	HookRegistry *runtime.HookRegistry
	Logger       runtime.Logger
	ZapLogger    *zap.Logger
	DB           *sql.DB
	NK           runtime.RuntimeModule
	Registry     SessionRegistry

	// LuaModulePath is the directory for Lua match modules (e.g. data/modules).
	LuaModulePath   string
	luaMatchSources map[string]string
}

// NewRouter creates a new match Router.
func NewRouter() *Router {
	return &Router{
		matches: make(map[string]*MatchLoop),
	}
}

// SetSessionRegistry configures the SessionRegistry reference.
func (r *Router) SetSessionRegistry(reg SessionRegistry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Registry = reg
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

// CreateAndRegisterMatch instantiates a Go or Lua match and registers its MatchLoop.
func (r *Router) CreateAndRegisterMatch(ctx context.Context, matchID string, module string, params map[string]interface{}) error {
	r.mu.RLock()
	hr := r.HookRegistry
	logger := r.Logger
	zapLogger := r.ZapLogger
	db := r.DB
	nk := r.NK
	registry := r.Registry
	matchCount := len(r.matches)
	r.mu.RUnlock()

	if hr == nil {
		return errors.New("HookRegistry is not set on Match Router")
	}
	if matchCount >= maxConcurrentMatches {
		return fmt.Errorf("max concurrent matches (%d) reached", maxConcurrentMatches)
	}

	factory, ok := hr.GetMatch(module)
	if !ok {
		return r.createAndRegisterLuaMatch(matchID, module, params)
	}

	goMatch, err := factory(ctx, logger, db, nk)
	if err != nil {
		return fmt.Errorf("failed to instantiate match %q: %w", module, err)
	}

	state, tickRate, label := goMatch.MatchInit(ctx, logger, db, nk, params)

	if tickRate < 1 || tickRate > 60 {
		return fmt.Errorf("invalid tick rate %d (must be 1-60)", tickRate)
	}
	if len(label) > maxLabelBytes {
		return fmt.Errorf("match label exceeds %d bytes", maxLabelBytes)
	}

	r.mu.Lock()
	if len(r.matches) >= maxConcurrentMatches {
		r.mu.Unlock()
		return fmt.Errorf("max concurrent matches (%d) reached", maxConcurrentMatches)
	}
	r.mu.Unlock()

	loop := NewMatchLoop(matchID, nil, tickRate, zapLogger, registry)
	loop.SetGoMatch(goMatch, state, logger, db, nk)
	loop.label = label
	if v := os.Getenv("UGE_MATCH_MAX_EMPTY_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			loop.maxEmptySec = n
		}
	}
	loop.onMetadataUpdate = func(matchID string, label string, playerCount int) {
		r.UpdateMetadata(matchID, label, playerCount)
	}
	loop.onEnd = func(matchID string, finalState MatchState) {
		r.Unregister(matchID)
	}

	r.Register(matchID, loop)
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
	if rdb != nil {
		r.clusterSignalForwarder = NewPubSubSignalForwarder(rdb, nodeID, r.ZapLogger)
	}
}

// SetClusterSignalForwarder overrides the cluster signal forwarder.
func (r *Router) SetClusterSignalForwarder(forwarder func(ctx context.Context, targetNodeID, matchID string, data string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusterSignalForwarder = forwarder
}

// Register adds a match loop to the routing registry.
func (r *Router) Register(matchID string, loop *MatchLoop) {
	r.mu.Lock()
	r.matches[matchID] = loop
	rdb := r.rdb
	nodeID := r.nodeID
	r.mu.Unlock()

	if rdb != nil && nodeID != "" {
		ctx := context.Background()
		pipe := rdb.Pipeline()
		pipe.Set(ctx, "match:node:"+matchID, nodeID, 0)
		pipe.SAdd(ctx, "match:ids", matchID)

		metadata := map[string]interface{}{
			"match_id":      matchID,
			"authoritative": "true",
			"label":         loop.label,
			"size":          len(loop.presences),
			"max_size":      100,
			"node":          nodeID,
		}
		pipe.HMSet(ctx, "match:metadata:"+matchID, metadata)

		_, err := pipe.Exec(ctx)
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
		ctx := context.Background()
		pipe := rdb.Pipeline()
		pipe.Del(ctx, "match:node:"+matchID)
		pipe.SRem(ctx, "match:ids", matchID)
		pipe.Del(ctx, "match:metadata:"+matchID)

		_, err := pipe.Exec(ctx)
		if err != nil {
			if r.ZapLogger != nil {
				r.ZapLogger.Error("Failed to delete match from Redis", zap.String("match_id", matchID), zap.Error(err))
			}
		}
	}
}

// UpdateMetadata updates match metadata in Redis globally.
func (r *Router) UpdateMetadata(matchID string, label string, playerCount int) {
	r.mu.RLock()
	rdb := r.rdb
	nodeID := r.nodeID
	r.mu.RUnlock()

	if rdb != nil && nodeID != "" {
		ctx := context.Background()
		rdb.HSet(ctx, "match:metadata:"+matchID, "label", label, "size", playerCount)
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

// ForwardSignal routes an admin signal to the targeted match loop, forwarding to peer nodes if needed.
func (r *Router) ForwardSignal(ctx context.Context, matchID string, data string) (string, error) {
	r.mu.RLock()
	loop, exists := r.matches[matchID]
	nodeID := r.nodeID
	rdb := r.rdb
	forwarder := r.clusterSignalForwarder
	r.mu.RUnlock()

	if exists {
		return loop.SubmitSignal(data)
	}

	if rdb != nil && forwarder != nil {
		targetNodeID, err := rdb.Get(ctx, "match:node:"+matchID).Result()
		if err == redis.Nil {
			return "", errors.New("match not found in cluster registry")
		} else if err != nil {
			return "", err
		}

		if targetNodeID != nodeID {
			if err := forwarder(ctx, targetNodeID, matchID, data); err != nil {
				return "", err
			}
			return "", nil
		}
	}

	return "", errors.New("match not found on this node")
}

// GetLocalMatches retrieves all active matches hosted on this node.
func (r *Router) GetLocalMatches() []*ActiveMatch {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var results []*ActiveMatch
	for id, loop := range r.matches {
		loop.mu.RLock()
		labelMap := make(map[string]interface{})
		_ = json.Unmarshal([]byte(loop.label), &labelMap)
		results = append(results, &ActiveMatch{
			MatchID:       id,
			Label:         labelMap,
			PlayerCount:   len(loop.presences),
			MaxSize:       100,
			Authoritative: true,
		})
		loop.mu.RUnlock()
	}
	return results
}

// GetLocalMatch retrieves details of a specific local match.
func (r *Router) GetLocalMatch(matchID string) (*ActiveMatch, []PresenceImpl, bool) {
	r.mu.RLock()
	loop, exists := r.matches[matchID]
	r.mu.RUnlock()
	if !exists {
		return nil, nil, false
	}

	loop.mu.RLock()
	defer loop.mu.RUnlock()
	labelMap := make(map[string]interface{})
	_ = json.Unmarshal([]byte(loop.label), &labelMap)

	var presences []PresenceImpl
	for _, p := range loop.presences {
		presences = append(presences, *p)
	}

	match := &ActiveMatch{
		MatchID:       matchID,
		Label:         labelMap,
		PlayerCount:   len(loop.presences),
		MaxSize:       100,
		Authoritative: true,
	}
	return match, presences, true
}

// ListMatches returns active match metadata (local or Redis registry).
func (r *Router) ListMatches(ctx context.Context, limit int, authoritative bool, label string, minSize, maxSize int) ([]*runtime.MatchInfo, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []*runtime.MatchInfo
	if r.rdb != nil {
		matchIDs, err := r.rdb.SMembers(ctx, "match:ids").Result()
		if err != nil {
			return nil, err
		}
		for _, matchID := range matchIDs {
			meta, err := r.rdb.HGetAll(ctx, "match:metadata:"+matchID).Result()
			if err != nil || len(meta) == 0 {
				continue
			}
			authVal, _ := strconv.ParseBool(meta["authoritative"])
			sizeVal, _ := strconv.Atoi(meta["size"])
			maxSizeVal, _ := strconv.Atoi(meta["max_size"])
			if authoritative && !authVal {
				continue
			}
			if label != "" && !containsLabel(meta["label"], label) {
				continue
			}
			if minSize > 0 && sizeVal < minSize {
				continue
			}
			if maxSize > 0 && sizeVal > maxSize {
				continue
			}
			out = append(out, &runtime.MatchInfo{
				MatchID: matchID, Authoritative: authVal, Label: meta["label"],
				Size: sizeVal, MaxSize: maxSizeVal, HandlerName: meta["handler"],
			})
			if len(out) >= limit {
				break
			}
		}
		return out, nil
	}
	for _, m := range r.GetLocalMatches() {
		if authoritative && !m.Authoritative {
			continue
		}
		labelBytes, _ := json.Marshal(m.Label)
		labelStr := string(labelBytes)
		if label != "" && !containsLabel(labelStr, label) {
			continue
		}
		if minSize > 0 && m.PlayerCount < minSize {
			continue
		}
		if maxSize > 0 && m.PlayerCount > maxSize {
			continue
		}
		out = append(out, &runtime.MatchInfo{
			MatchID: m.MatchID, Authoritative: m.Authoritative, Label: labelStr,
			Size: m.PlayerCount, MaxSize: m.MaxSize,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func containsLabel(haystack, needle string) bool {
	return needle == "" || strings.Contains(haystack, needle)
}

// GetMatch returns metadata for one match.
func (r *Router) GetMatch(ctx context.Context, matchID string) (*runtime.MatchInfo, error) {
	if r.rdb != nil {
		meta, err := r.rdb.HGetAll(ctx, "match:metadata:"+matchID).Result()
		if err != nil || len(meta) == 0 {
			if m, _, ok := r.GetLocalMatch(matchID); ok {
				lb, _ := json.Marshal(m.Label)
				return &runtime.MatchInfo{
					MatchID: m.MatchID, Authoritative: m.Authoritative, Label: string(lb),
					Size: m.PlayerCount, MaxSize: m.MaxSize,
				}, nil
			}
			return nil, errors.New("match not found")
		}
		authVal, _ := strconv.ParseBool(meta["authoritative"])
		sizeVal, _ := strconv.Atoi(meta["size"])
		maxSizeVal, _ := strconv.Atoi(meta["max_size"])
		return &runtime.MatchInfo{
			MatchID: matchID, Authoritative: authVal, Label: meta["label"],
			Size: sizeVal, MaxSize: maxSizeVal, HandlerName: meta["handler"],
		}, nil
	}
	m, _, ok := r.GetLocalMatch(matchID)
	if !ok {
		return nil, errors.New("match not found")
	}
	lb, _ := json.Marshal(m.Label)
	return &runtime.MatchInfo{
		MatchID: m.MatchID, Authoritative: m.Authoritative, Label: string(lb),
		Size: m.PlayerCount, MaxSize: m.MaxSize,
	}, nil
}

// MatchSignal delivers a signal to a match loop.
func (r *Router) MatchSignal(ctx context.Context, matchID, data string) (string, error) {
	return r.ForwardSignal(ctx, matchID, data)
}
