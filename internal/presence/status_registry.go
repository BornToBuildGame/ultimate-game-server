package presence

import (
	"context"
	"encoding/json"
	"sync"
)

// StatusPresence is a compact status payload for envelopes.
type StatusPresence struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Username  string `json:"username"`
	Status    string `json:"status,omitempty"`
}

type statusEvent struct {
	userID string
	joins  []StatusPresence
	leaves []StatusPresence
}

// StatusRegistry manages follow edges and fans out status_presence_event (ADR-0019).
type StatusRegistry struct {
	mu sync.RWMutex

	bySession map[string]map[string]struct{} // sessionID -> followed userIDs
	byUser    map[string]map[string]struct{} // userID -> follower sessionIDs

	onlineMu    sync.RWMutex
	onlineCache map[string]map[string]struct{} // userID -> sessionIDs with status Tracked

	eventsCh chan *statusEvent
	router   MessageRouter
	online   *OnlineIndex

	ctx         context.Context
	ctxCancelFn context.CancelFunc
}

// NewStatusRegistry creates a StatusRegistry with an async event worker.
func NewStatusRegistry(router MessageRouter, online *OnlineIndex, queueSize int) *StatusRegistry {
	if queueSize <= 0 {
		queueSize = 1024
	}
	ctx, cancel := context.WithCancel(context.Background())
	sr := &StatusRegistry{
		bySession:   make(map[string]map[string]struct{}),
		byUser:      make(map[string]map[string]struct{}),
		onlineCache: make(map[string]map[string]struct{}),
		eventsCh:    make(chan *statusEvent, queueSize),
		router:      router,
		online:      online,
		ctx:         ctx,
		ctxCancelFn: cancel,
	}
	go sr.worker()
	return sr
}

// Stop cancels the event worker.
func (sr *StatusRegistry) Stop() {
	if sr.ctxCancelFn != nil {
		sr.ctxCancelFn()
	}
}

// Follow registers sessionID as a follower of the given user IDs.
// Returns current follow count for the session after the operation.
func (sr *StatusRegistry) Follow(sessionID string, userIDs []string) int {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	followed, ok := sr.bySession[sessionID]
	if !ok {
		followed = make(map[string]struct{})
		sr.bySession[sessionID] = followed
	}
	for _, uid := range userIDs {
		if uid == "" {
			continue
		}
		followed[uid] = struct{}{}
		subs, ok := sr.byUser[uid]
		if !ok {
			subs = make(map[string]struct{})
			sr.byUser[uid] = subs
		}
		subs[sessionID] = struct{}{}
	}
	return len(followed)
}

// FollowCount returns how many users a session currently follows.
func (sr *StatusRegistry) FollowCount(sessionID string) int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.bySession[sessionID])
}

// Unfollow removes follow edges for the given user IDs.
func (sr *StatusRegistry) Unfollow(sessionID string, userIDs []string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	followed := sr.bySession[sessionID]
	for _, uid := range userIDs {
		if followed != nil {
			delete(followed, uid)
		}
		if subs, ok := sr.byUser[uid]; ok {
			delete(subs, sessionID)
			if len(subs) == 0 {
				delete(sr.byUser, uid)
			}
		}
	}
	if followed != nil && len(followed) == 0 {
		delete(sr.bySession, sessionID)
	}
}

// UnfollowAll removes all follow edges for a session.
func (sr *StatusRegistry) UnfollowAll(sessionID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	followed, ok := sr.bySession[sessionID]
	if !ok {
		return
	}
	for uid := range followed {
		if subs, ok := sr.byUser[uid]; ok {
			delete(subs, sessionID)
			if len(subs) == 0 {
				delete(sr.byUser, uid)
			}
		}
	}
	delete(sr.bySession, sessionID)
}

// QueueJoin notifies followers of a status join/update.
func (sr *StatusRegistry) QueueJoin(userID string, p StatusPresence) {
	sr.Queue(userID, []StatusPresence{p}, nil)
}

// QueueLeave notifies followers of a status leave.
func (sr *StatusRegistry) QueueLeave(userID string, p StatusPresence) {
	sr.Queue(userID, nil, []StatusPresence{p})
}

// Queue enqueues a status event for async fan-out and online-cache update.
func (sr *StatusRegistry) Queue(userID string, joins, leaves []StatusPresence) {
	if sr == nil {
		return
	}
	ev := &statusEvent{userID: userID, joins: joins, leaves: leaves}
	select {
	case sr.eventsCh <- ev:
	case <-sr.ctx.Done():
	default:
		// Drop under extreme backpressure rather than block the socket path.
		go func() {
			select {
			case sr.eventsCh <- ev:
			case <-sr.ctx.Done():
			}
		}()
	}
}

func (sr *StatusRegistry) worker() {
	for {
		select {
		case <-sr.ctx.Done():
			return
		case e := <-sr.eventsCh:
			sr.applyOnline(e)
			sr.fanOut(e)
		}
	}
}

func (sr *StatusRegistry) applyOnline(e *statusEvent) {
	sr.onlineMu.Lock()
	existing, found := sr.onlineCache[e.userID]
	for _, leave := range e.leaves {
		if !found {
			continue
		}
		if _, ok := existing[leave.SessionID]; ok {
			delete(existing, leave.SessionID)
			if sr.online != nil {
				sr.online.ClearOnline(e.userID)
			}
		}
	}
	for _, join := range e.joins {
		if !found {
			existing = make(map[string]struct{}, 1)
			sr.onlineCache[e.userID] = existing
			found = true
		}
		_, already := existing[join.SessionID]
		existing[join.SessionID] = struct{}{}
		if !already && sr.online != nil {
			sr.online.SetOnline(e.userID)
		}
	}
	if found && len(existing) == 0 {
		delete(sr.onlineCache, e.userID)
	}
	sr.onlineMu.Unlock()
}

func (sr *StatusRegistry) fanOut(e *statusEvent) {
	sr.mu.RLock()
	ids, hasFollowers := sr.byUser[e.userID]
	if !hasFollowers || len(ids) == 0 {
		sr.mu.RUnlock()
		return
	}
	sessionIDs := make([]string, 0, len(ids))
	for sid := range ids {
		sessionIDs = append(sessionIDs, sid)
	}
	sr.mu.RUnlock()

	joins := e.joins
	leaves := e.leaves
	if joins == nil {
		joins = []StatusPresence{}
	}
	if leaves == nil {
		leaves = []StatusPresence{}
	}
	payload, err := json.Marshal(map[string]interface{}{
		"status_presence_event": map[string]interface{}{
			"joins":  joins,
			"leaves": leaves,
		},
	})
	if err != nil || sr.router == nil {
		return
	}
	sr.router.SendToSessionIDs(sessionIDs, payload)
}

// IsOnline reports whether the user has any status-tracked session.
func (sr *StatusRegistry) IsOnline(userID string) bool {
	sr.onlineMu.RLock()
	defer sr.onlineMu.RUnlock()
	return len(sr.onlineCache[userID]) > 0
}
