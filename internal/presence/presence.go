package presence

import (
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// PresenceRecord represents a client's online state.
type PresenceRecord struct {
	UserID    string    `json:"user_id"`
	SessionID string    `json:"session_id"`
	Username  string    `json:"username"`
	Node      string    `json:"node"`
	Status    string    `json:"status"` // Custom JSON status string
	JoinedAt  time.Time `json:"joined_at"`
}

// PresenceTracker tracks active presences, subscriptions, and provides fast online bitmap checks.
type PresenceTracker struct {
	mu            sync.RWMutex
	presences     map[string][]PresenceRecord // key: user_id
	subscriptions map[string]map[string]bool  // key: watched user_id, value: set of subscriber session_ids
	sessionUser   map[string]string           // key: session_id, value: user_id

	// Roaring Bitmap index mapping
	onlineBitmap  *roaring.Bitmap
	userToIdx     map[string]uint32
	idxToUser     map[uint32]string
	nextUserIdx   uint32
}

// NewPresenceTracker creates a new PresenceTracker.
func NewPresenceTracker() *PresenceTracker {
	return &PresenceTracker{
		presences:     make(map[string][]PresenceRecord),
		subscriptions: make(map[string]map[string]bool),
		sessionUser:   make(map[string]string),
		onlineBitmap:  roaring.New(),
		userToIdx:     make(map[string]uint32),
		idxToUser:     make(map[uint32]string),
		nextUserIdx:   1,
	}
}

// SetPresence adds or updates a user's presence record.
// Returns the subscriber session IDs that should be notified.
func (pt *PresenceTracker) SetPresence(record PresenceRecord) []string {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	records := pt.presences[record.UserID]
	found := false
	for i, r := range records {
		if r.SessionID == record.SessionID {
			records[i] = record
			found = true
			break
		}
	}
	if !found {
		records = append(records, record)
		pt.presences[record.UserID] = records
	}

	pt.sessionUser[record.SessionID] = record.UserID

	// Map user to bitmap index if not exists, and set online bit
	idx, exists := pt.userToIdx[record.UserID]
	if !exists {
		idx = pt.nextUserIdx
		pt.nextUserIdx++
		pt.userToIdx[record.UserID] = idx
		pt.idxToUser[idx] = record.UserID
	}
	pt.onlineBitmap.Add(idx)

	// Collect subscribers
	var subscriberSessions []string
	if subs, exists := pt.subscriptions[record.UserID]; exists {
		for sessID := range subs {
			subscriberSessions = append(subscriberSessions, sessID)
		}
	}

	return subscriberSessions
}

// RemovePresence removes a presence record by session ID.
// Returns the user ID, whether the user is now fully offline, and the subscriber sessions to notify.
func (pt *PresenceTracker) RemovePresence(sessionID string) (string, bool, []string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	userID, exists := pt.sessionUser[sessionID]
	if !exists {
		return "", false, nil
	}

	delete(pt.sessionUser, sessionID)

	records := pt.presences[userID]
	var updatedRecords []PresenceRecord
	for _, r := range records {
		if r.SessionID != sessionID {
			updatedRecords = append(updatedRecords, r)
		}
	}

	isFullyOffline := len(updatedRecords) == 0
	if isFullyOffline {
		delete(pt.presences, userID)
		// Clear online bit
		if idx, exists := pt.userToIdx[userID]; exists {
			pt.onlineBitmap.Remove(idx)
		}
	} else {
		pt.presences[userID] = updatedRecords
	}

	// Collect subscribers
	var subscriberSessions []string
	if subs, exists := pt.subscriptions[userID]; exists {
		for sessID := range subs {
			subscriberSessions = append(subscriberSessions, sessID)
		}
	}

	return userID, isFullyOffline, subscriberSessions
}

// Follow registers a session to follow target users.
// Returns the PresenceRecords of targets who are currently online.
func (pt *PresenceTracker) Follow(sessionID string, targetUserIDs []string) []PresenceRecord {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var onlineRecords []PresenceRecord

	for _, targetID := range targetUserIDs {
		subs, exists := pt.subscriptions[targetID]
		if !exists {
			subs = make(map[string]bool)
			pt.subscriptions[targetID] = subs
		}
		subs[sessionID] = true

		if records, online := pt.presences[targetID]; online {
			onlineRecords = append(onlineRecords, records...)
		}
	}

	return onlineRecords
}

// Unfollow unregisters a session from following target users.
func (pt *PresenceTracker) Unfollow(sessionID string, targetUserIDs []string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for _, targetID := range targetUserIDs {
		if subs, exists := pt.subscriptions[targetID]; exists {
			delete(subs, sessionID)
			if len(subs) == 0 {
				delete(pt.subscriptions, targetID)
			}
		}
	}
}

// GetOnlineFriends computes the intersection of a user's friends list and online states using a Roaring Bitmap.
func (pt *PresenceTracker) GetOnlineFriends(friendUserIDs []string) []string {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	// 1. Create a bitmap representing the friends list indices
	friendsBitmap := roaring.New()
	for _, friendID := range friendUserIDs {
		if idx, exists := pt.userToIdx[friendID]; exists {
			friendsBitmap.Add(idx)
		}
	}

	// 2. Compute intersection with the online bitmap
	intersection := roaring.And(friendsBitmap, pt.onlineBitmap)

	// 3. Convert indices back to user IDs
	var onlineFriends []string
	iterator := intersection.Iterator()
	for iterator.HasNext() {
		idx := iterator.Next()
		if userID, exists := pt.idxToUser[idx]; exists {
			onlineFriends = append(onlineFriends, userID)
		}
	}

	return onlineFriends
}
