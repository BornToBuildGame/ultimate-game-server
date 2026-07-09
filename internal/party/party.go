package party

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrPartyNotFound      = errors.New("party not found")
	ErrNotLeader          = errors.New("operation allowed only for party leader")
	ErrPartyFull          = errors.New("party is full")
	ErrClosedNoInvitation = errors.New("party is closed and player has no invitation")
	ErrMemberNotFound     = errors.New("member not found")
	ErrInviteLimitExceeded = errors.New("invitation limits exceeded")
	ErrInvalidMaxSize     = errors.New("invalid max size (must be between 2 and 16)")
)

// PartyMember represents a player in an active party.
type PartyMember struct {
	UserID     string                 `json:"user_id"`
	Username   string                 `json:"username"`
	SessionID  string                 `json:"session_id"`
	JoinedAt   time.Time              `json:"joined_at"`
	Properties map[string]interface{} `json:"properties"`
}

// PartySession represents a transient, in-memory party group.
type PartySession struct {
	PartyID     string                  `json:"party_id"`
	LeaderID    string                  `json:"leader_id"`
	Open        bool                    `json:"open"`
	MaxSize     int                     `json:"max_size"`
	Members     map[string]*PartyMember `json:"members"`     // key: userID
	Invitations map[string]time.Time    `json:"invitations"` // key: userID -> invite time
	Metadata    map[string]interface{}  `json:"metadata"`
	Node        string                  `json:"node"`
}

// Registry manages thread-safe memory maps of active parties.
type Registry struct {
	mu      sync.RWMutex
	parties map[string]*PartySession
}

// NewRegistry creates a new Party Registry.
func NewRegistry() *Registry {
	return &Registry{
		parties: make(map[string]*PartySession),
	}
}

// CreateParty creates a new party and adds the leader as the first member.
func (r *Registry) CreateParty(leaderID, username, sessionID string, open bool, maxSize int) (*PartySession, error) {
	if maxSize < 2 || maxSize > 16 {
		return nil, ErrInvalidMaxSize
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	partyID := uuid.New().String()
	p := &PartySession{
		PartyID:     partyID,
		LeaderID:    leaderID,
		Open:        open,
		MaxSize:     maxSize,
		Members:     make(map[string]*PartyMember),
		Invitations: make(map[string]time.Time),
		Metadata:    make(map[string]interface{}),
	}

	p.Members[leaderID] = &PartyMember{
		UserID:     leaderID,
		Username:   username,
		SessionID:  sessionID,
		JoinedAt:   time.Now(),
		Properties: make(map[string]interface{}),
	}

	r.parties[partyID] = p
	return p, nil
}

// GetParty retrieves a party by ID.
func (r *Registry) GetParty(partyID string) (*PartySession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, exists := r.parties[partyID]
	if !exists {
		return nil, ErrPartyNotFound
	}
	return p, nil
}

// SendInvitation adds an invitation. Only the leader can invite.
func (r *Registry) SendInvitation(partyID, leaderID, targetUserID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists {
		return ErrPartyNotFound
	}

	if p.LeaderID != leaderID {
		return ErrNotLeader
	}

	if _, ok := p.Members[targetUserID]; ok {
		return nil // already in party
	}

	// Limit checks
	if len(p.Invitations) >= 20 {
		return ErrInviteLimitExceeded
	}

	p.Invitations[targetUserID] = time.Now()
	return nil
}

// JoinParty adds a user to the party, clearing any invitation.
func (r *Registry) JoinParty(partyID, userID, username, sessionID string) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists {
		return nil, ErrPartyNotFound
	}

	if len(p.Members) >= p.MaxSize {
		return nil, ErrPartyFull
	}

	if !p.Open {
		if _, invited := p.Invitations[userID]; !invited {
			return nil, ErrClosedNoInvitation
		}
	}

	// Add member
	p.Members[userID] = &PartyMember{
		UserID:     userID,
		Username:   username,
		SessionID:  sessionID,
		JoinedAt:   time.Now(),
		Properties: make(map[string]interface{}),
	}

	// Clear invite
	delete(p.Invitations, userID)

	return p, nil
}

// LeaveParty removes a user from the party and promotes a new leader if needed.
func (r *Registry) LeaveParty(partyID, userID string) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists {
		return nil, ErrPartyNotFound
	}

	if _, isMember := p.Members[userID]; !isMember {
		return nil, ErrMemberNotFound
	}

	delete(p.Members, userID)

	// If no members remain, delete the party
	if len(p.Members) == 0 {
		delete(r.parties, partyID)
		return nil, nil
	}

	// If the leader left, promote the longest-serving member
	if p.LeaderID == userID {
		var oldestMember *PartyMember
		for _, m := range p.Members {
			if oldestMember == nil || m.JoinedAt.Before(oldestMember.JoinedAt) {
				oldestMember = m
			}
		}
		if oldestMember != nil {
			p.LeaderID = oldestMember.UserID
		}
	}

	return p, nil
}

// UpdateMemberProperties updates custom variables for a party member.
func (r *Registry) UpdateMemberProperties(partyID, userID string, properties map[string]interface{}) (*PartySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.parties[partyID]
	if !exists {
		return nil, ErrPartyNotFound
	}

	m, ok := p.Members[userID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	for k, v := range properties {
		m.Properties[k] = v
	}

	return p, nil
}

// SweepInvitations removes invitations older than 5 minutes.
func (r *Registry) SweepInvitations() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for _, p := range r.parties {
		for target, tSent := range p.Invitations {
			if now.Sub(tSent) > 5*time.Minute {
				delete(p.Invitations, target)
			}
		}
	}
}
