package match

import (
	"context"
	"errors"
	"sync"
)

// Router maps active match IDs to their local loop execution thread.
// It abstracts cross-node routing by providing local in-memory lookups for Single-Node mode.
type Router struct {
	mu      sync.RWMutex
	matches map[string]*MatchLoop
}

// NewRouter creates a new match Router.
func NewRouter() *Router {
	return &Router{
		matches: make(map[string]*MatchLoop),
	}
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

// ForwardInput routes client inputs to the targeted match loop.
func (r *Router) ForwardInput(ctx context.Context, matchID string, input MatchInput) error {
	r.mu.RLock()
	loop, exists := r.matches[matchID]
	r.mu.RUnlock()

	if !exists {
		return errors.New("match not found on this node")
	}

	loop.SubmitInput(input)
	return nil
}
