package socket

import (
	"encoding/json"
	"time"

	"ultimate-game-server/internal/presence"
)

func (gh *GatewayHandler) handleStatusFollow(s *Session, cid string, req *StatusFollowPayload) {
	pt := gh.PresenceTracker
	if pt == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "presence tracker not configured"})
		s.TrySend(res)
		return
	}
	records := pt.Follow(s.ID, req.UserIDs)
	presences := make([]map[string]interface{}, 0, len(records))
	for _, r := range records {
		presences = append(presences, map[string]interface{}{
			"user_id":    r.UserID,
			"session_id": r.SessionID,
			"username":   r.Username,
			"status":     r.Status,
		})
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"status": map[string]interface{}{
			"presences": presences,
		},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handleStatusUnfollow(s *Session, cid string, req *StatusUnfollowPayload) {
	pt := gh.PresenceTracker
	if pt == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "presence tracker not configured"})
		s.TrySend(res)
		return
	}
	pt.Unfollow(s.ID, req.UserIDs)
	res, _ := json.Marshal(map[string]interface{}{
		"cid":             cid,
		"status_unfollow": map[string]interface{}{"user_ids": req.UserIDs},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handleStatusUpdate(s *Session, cid string, req *StatusUpdatePayload) {
	pt := gh.PresenceTracker
	if pt == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "presence tracker not configured"})
		s.TrySend(res)
		return
	}

	if req.Status == "" {
		rec, had := pt.PeekPresence(s.ID)
		_, _, subs := pt.RemovePresence(s.ID)
		if had {
			gh.notifyStatusSubscribers(subs, nil, []map[string]interface{}{
				{
					"user_id":    rec.UserID,
					"session_id": rec.SessionID,
					"username":   rec.Username,
				},
			})
		}
		res, _ := json.Marshal(map[string]interface{}{
			"cid":           cid,
			"status_update": map[string]interface{}{"status": ""},
		})
		s.TrySend(res)
		return
	}

	subs := pt.SetPresence(presence.PresenceRecord{
		UserID:    s.UserID,
		SessionID: s.ID,
		Username:  s.Username,
		Status:    req.Status,
		JoinedAt:  time.Now(),
	})
	gh.notifyStatusSubscribers(subs, []map[string]interface{}{
		{
			"user_id":    s.UserID,
			"session_id": s.ID,
			"username":   s.Username,
			"status":     req.Status,
		},
	}, nil)

	res, _ := json.Marshal(map[string]interface{}{
		"cid":           cid,
		"status_update": map[string]interface{}{"status": req.Status},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handlePing(s *Session, cid string) {
	res, _ := json.Marshal(map[string]interface{}{
		"cid":  cid,
		"pong": map[string]interface{}{},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) notifyStatusSubscribers(subs []string, joins, leaves []map[string]interface{}) {
	if len(subs) == 0 {
		return
	}
	if joins == nil {
		joins = []map[string]interface{}{}
	}
	if leaves == nil {
		leaves = []map[string]interface{}{}
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"status_presence_event": map[string]interface{}{
			"joins":  joins,
			"leaves": leaves,
		},
	})
	for _, sid := range subs {
		gh.registry.SendToSession(sid, payload)
	}
}

// NotifyStatusJoin publishes a status_presence_event join to subscribers.
func (gh *GatewayHandler) NotifyStatusJoin(subs []string, userID, sessionID, username, status string) {
	gh.notifyStatusSubscribers(subs, []map[string]interface{}{
		{
			"user_id":    userID,
			"session_id": sessionID,
			"username":   username,
			"status":     status,
		},
	}, nil)
}

// NotifyStatusLeave publishes a status_presence_event leave to subscribers.
func (gh *GatewayHandler) NotifyStatusLeave(subs []string, userID, sessionID, username string) {
	gh.notifyStatusSubscribers(subs, nil, []map[string]interface{}{
		{
			"user_id":    userID,
			"session_id": sessionID,
			"username":   username,
		},
	})
}
