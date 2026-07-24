package presence

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// OnlineIndex provides fast friend∩online checks via roaring bitmaps (ADR-0019).
// It is not the presence product API — status lives on LocalTracker + StatusRegistry.
type OnlineIndex struct {
	mu           sync.RWMutex
	onlineBitmap *roaring.Bitmap
	userToIdx    map[string]uint32
	idxToUser    map[uint32]string
	nextUserIdx  uint32
	// session counts per user so multi-session online stays correct
	sessionCount map[string]int
}

// PresenceTracker is a backward-compatible alias for OnlineIndex (friends API).
type PresenceTracker = OnlineIndex

// NewOnlineIndex creates an empty online index.
func NewOnlineIndex() *OnlineIndex {
	return &OnlineIndex{
		onlineBitmap: roaring.New(),
		userToIdx:    make(map[string]uint32),
		idxToUser:    make(map[uint32]string),
		nextUserIdx:  1,
		sessionCount: make(map[string]int),
	}
}

// NewPresenceTracker creates an OnlineIndex (alias for friends wiring).
func NewPresenceTracker() *OnlineIndex {
	return NewOnlineIndex()
}

// SetOnline marks a user session as online in the index.
func (oi *OnlineIndex) SetOnline(userID string) {
	if userID == "" {
		return
	}
	oi.mu.Lock()
	defer oi.mu.Unlock()
	oi.sessionCount[userID]++
	idx, exists := oi.userToIdx[userID]
	if !exists {
		idx = oi.nextUserIdx
		oi.nextUserIdx++
		oi.userToIdx[userID] = idx
		oi.idxToUser[idx] = userID
	}
	oi.onlineBitmap.Add(idx)
}

// ClearOnline removes one session for a user; clears the bit when fully offline.
func (oi *OnlineIndex) ClearOnline(userID string) {
	if userID == "" {
		return
	}
	oi.mu.Lock()
	defer oi.mu.Unlock()
	n := oi.sessionCount[userID]
	if n <= 1 {
		delete(oi.sessionCount, userID)
		if idx, exists := oi.userToIdx[userID]; exists {
			oi.onlineBitmap.Remove(idx)
		}
		return
	}
	oi.sessionCount[userID] = n - 1
}

// IsOnline reports whether the user has at least one online status session.
func (oi *OnlineIndex) IsOnline(userID string) bool {
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	idx, ok := oi.userToIdx[userID]
	if !ok {
		return false
	}
	return oi.onlineBitmap.Contains(idx)
}

// GetOnlineFriends computes the intersection of a friends list and online states.
func (oi *OnlineIndex) GetOnlineFriends(friendUserIDs []string) []string {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	friendsBitmap := roaring.New()
	for _, friendID := range friendUserIDs {
		if idx, exists := oi.userToIdx[friendID]; exists {
			friendsBitmap.Add(idx)
		}
	}

	intersection := roaring.And(friendsBitmap, oi.onlineBitmap)

	var onlineFriends []string
	iterator := intersection.Iterator()
	for iterator.HasNext() {
		idx := iterator.Next()
		if userID, exists := oi.idxToUser[idx]; exists {
			onlineFriends = append(onlineFriends, userID)
		}
	}
	return onlineFriends
}
