package match

import (
	"sync"
)

// ActiveMatch represents metadata for an ongoing match, searchable by labels.
type ActiveMatch struct {
	MatchID       string                 `json:"match_id"`
	Label         map[string]interface{} `json:"label"`
	PlayerCount   int                    `json:"player_count"`
	MaxSize       int                    `json:"max_size"`
	Authoritative bool                   `json:"authoritative"`
}

// Registry stores and queries metadata for all active matches.
type Registry struct {
	mu      sync.RWMutex
	matches map[string]*ActiveMatch
}

// NewRegistry creates a new match metadata registry.
func NewRegistry() *Registry {
	return &Registry{
		matches: make(map[string]*ActiveMatch),
	}
}

// Add registers a new active match.
func (r *Registry) Add(matchID string, label map[string]interface{}, playerCount, maxSize int, authoritative bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.matches[matchID] = &ActiveMatch{
		MatchID:       matchID,
		Label:         label,
		PlayerCount:   playerCount,
		MaxSize:       maxSize,
		Authoritative: authoritative,
	}
}

// Remove deletes a match metadata entry.
func (r *Registry) Remove(matchID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.matches, matchID)
}

// Search searches active matches matching a specific label key and value.
func (r *Registry) Search(key string, val interface{}) []*ActiveMatch {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []*ActiveMatch
	for _, m := range r.matches {
		if m.Label != nil {
			if v, exists := m.Label[key]; exists && v == val {
				results = append(results, m)
			}
		}
	}
	return results
}
