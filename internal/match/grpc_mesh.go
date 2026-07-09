package match

import (
	"context"
	"errors"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Router maps active match IDs to their local loop execution thread.
// It abstracts cross-node routing by providing local in-memory lookups and falling back to cluster forwarding.
type Router struct {
	mu               sync.RWMutex
	matches          map[string]*MatchLoop
	rdb              *redis.Client
	nodeID           string
	clusterForwarder func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error
}

// NewRouter creates a new match Router.
func NewRouter() *Router {
	return &Router{
		matches: make(map[string]*MatchLoop),
	}
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
	defer r.mu.Unlock()
	r.matches[matchID] = loop
}

// Unregister removes a match loop from the registry.
func (r *Router) Unregister(matchID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.matches, matchID)
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
