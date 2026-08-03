package socket

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BornToBuildGame/ultimate-game-server/internal/cluster"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"
)

const (
	defaultMaxStatusBytes     = 2048
	defaultMaxSubscriptions   = 1000
)

func (gh *GatewayHandler) maxStatusBytes() int {
	if gh.MaxStatusBytes > 0 {
		return gh.MaxStatusBytes
	}
	return defaultMaxStatusBytes
}

func (gh *GatewayHandler) maxSubscriptions() int {
	if gh.MaxSubscriptions > 0 {
		return gh.MaxSubscriptions
	}
	return defaultMaxSubscriptions
}

func (gh *GatewayHandler) handleStatusFollow(s *Session, cid string, req *StatusFollowPayload) {
	sr := gh.StatusRegistry
	tr := gh.StreamTracker
	if sr == nil || tr == nil {
		gh.sendStatusError(s, cid, "presence not configured")
		return
	}

	userIDs, err := gh.resolveFollowTargets(s, req)
	if err != nil {
		gh.sendStatusError(s, cid, err.Error())
		return
	}

	current := sr.FollowCount(s.ID)
	// Self is auto-followed; count toward cap only for new targets.
	newCount := 0
	for _, uid := range userIDs {
		if uid == s.UserID {
			continue
		}
		newCount++
	}
	if current+newCount > gh.maxSubscriptions() {
		gh.sendStatusError(s, cid, fmt.Sprintf("maximum status subscriptions is %d", gh.maxSubscriptions()))
		return
	}

	followIDs := make([]string, 0, len(userIDs))
	for _, uid := range userIDs {
		if uid == s.UserID {
			continue
		}
		followIDs = append(followIDs, uid)
	}
	sr.Follow(s.ID, followIDs)

	presences := make([]map[string]interface{}, 0)
	for _, uid := range followIDs {
		for _, p := range tr.ListByStream(presence.StatusStream(uid)) {
			presences = append(presences, map[string]interface{}{
				"user_id":    p.UserID,
				"session_id": p.SessionID,
				"username":   p.Meta.Username,
				"status":     p.Meta.Status,
			})
		}
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"status": map[string]interface{}{
			"presences": presences,
		},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) resolveFollowTargets(s *Session, req *StatusFollowPayload) ([]string, error) {
	if req == nil {
		return nil, nil
	}
	unique := make(map[string]struct{})
	var ids []string
	for _, uid := range req.UserIDs {
		if uid == "" || uid == s.UserID {
			continue
		}
		if _, ok := unique[uid]; ok {
			continue
		}
		unique[uid] = struct{}{}
		ids = append(ids, uid)
	}

	usernames := make([]string, 0, len(req.Usernames))
	for _, uname := range req.Usernames {
		if uname == "" || uname == s.Username {
			continue
		}
		usernames = append(usernames, uname)
	}

	if len(ids) == 0 && len(usernames) == 0 {
		return nil, nil
	}

	if gh.dbPool == nil {
		// Without DB, accept user IDs as-is (tests / local); reject username-only lookups.
		if len(usernames) > 0 {
			return nil, fmt.Errorf("username follow requires database")
		}
		return ids, nil
	}

	ctx := context.Background()
	resolved := make(map[string]struct{})

	if len(ids) > 0 && len(usernames) == 0 {
		rows, err := gh.dbPool.Query(ctx, `SELECT id::text FROM users WHERE id = ANY($1::UUID[])`, ids)
		if err != nil {
			return nil, fmt.Errorf("could not check users")
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, fmt.Errorf("could not check users")
			}
			if id == s.UserID {
				continue
			}
			resolved[id] = struct{}{}
		}
	} else {
		query := `SELECT id::text, username FROM users WHERE `
		params := make([]interface{}, 0, 2)
		if len(ids) > 0 {
			params = append(params, ids)
			query += fmt.Sprintf("id = ANY($%d::UUID[])", len(params))
		}
		if len(usernames) > 0 {
			if len(ids) > 0 {
				query += " OR "
			}
			params = append(params, usernames)
			query += fmt.Sprintf("username = ANY($%d::text[])", len(params))
		}
		rows, err := gh.dbPool.Query(ctx, query, params...)
		if err != nil {
			return nil, fmt.Errorf("could not check users")
		}
		defer rows.Close()
		for rows.Next() {
			var id, username string
			if err := rows.Scan(&id, &username); err != nil {
				return nil, fmt.Errorf("could not check users")
			}
			if id == s.UserID {
				continue
			}
			resolved[id] = struct{}{}
		}
	}

	out := make([]string, 0, len(resolved))
	for id := range resolved {
		out = append(out, id)
	}
	return out, nil
}

func (gh *GatewayHandler) handleStatusUnfollow(s *Session, cid string, req *StatusUnfollowPayload) {
	sr := gh.StatusRegistry
	if sr == nil {
		gh.sendStatusError(s, cid, "presence not configured")
		return
	}
	ids := make([]string, 0, len(req.UserIDs))
	for _, uid := range req.UserIDs {
		if uid == "" || uid == s.UserID {
			continue
		}
		ids = append(ids, uid)
	}
	sr.Unfollow(s.ID, ids)
	res, _ := json.Marshal(map[string]interface{}{
		"cid":             cid,
		"status_unfollow": map[string]interface{}{"user_ids": req.UserIDs},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handleStatusUpdate(s *Session, cid string, req *StatusUpdatePayload) {
	sr := gh.StatusRegistry
	tr := gh.StreamTracker
	if sr == nil || tr == nil {
		gh.sendStatusError(s, cid, "presence not configured")
		return
	}

	stream := presence.StatusStream(s.UserID)

	// JSON null → Untrack (appear offline). Empty string → online with blank status.
	if req.Status == nil {
		p, had := tr.Untrack(s.ID, stream)
	if had {
			username := p.Meta.Username
			if username == "" {
				username = s.Username
			}
			sp := presence.StatusPresence{
				UserID:    s.UserID,
				SessionID: s.ID,
				Username:  username,
			}
			sr.QueueLeave(s.UserID, sp)
			gh.publishPresenceMesh("leave", s.UserID, sp)
		}
		res, _ := json.Marshal(map[string]interface{}{
			"cid":           cid,
			"status_update": map[string]interface{}{"status": nil},
		})
		s.TrySend(res)
		return
	}

	status := *req.Status
	if len(status) > gh.maxStatusBytes() {
		gh.sendStatusError(s, cid, fmt.Sprintf("Status must be %d characters or less", gh.maxStatusBytes()))
		return
	}

	meta := presence.PresenceMeta{Username: s.Username, Status: status}
	if _, ok := tr.GetPresence(s.ID, stream); ok {
		_ = tr.Update(s.ID, stream, s.UserID, meta)
	} else {
		tr.Track(s.ID, stream, s.UserID, meta)
	}
	sr.QueueJoin(s.UserID, presence.StatusPresence{
		UserID:    s.UserID,
		SessionID: s.ID,
		Username:  s.Username,
		Status:    status,
	})
	gh.publishPresenceMesh("update", s.UserID, presence.StatusPresence{
		UserID:    s.UserID,
		SessionID: s.ID,
		Username:  s.Username,
		Status:    status,
	})

	res, _ := json.Marshal(map[string]interface{}{
		"cid":           cid,
		"status_update": map[string]interface{}{"status": status},
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

func (gh *GatewayHandler) sendStatusError(s *Session, cid, msg string) {
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"error": map[string]interface{}{
			"message": msg,
		},
	})
	s.TrySend(res)
}

// TrackStatusOnConnect auto-follows self and optionally Tracks status stream (?status=true).
func (gh *GatewayHandler) TrackStatusOnConnect(s *Session) {
	if gh.StatusRegistry == nil || s == nil {
		return
	}
	gh.StatusRegistry.Follow(s.ID, []string{s.UserID})
	if !s.TrackStatus || gh.StreamTracker == nil {
		return
	}
	stream := presence.StatusStream(s.UserID)
	gh.StreamTracker.Track(s.ID, stream, s.UserID, presence.PresenceMeta{Username: s.Username, Status: ""})
	gh.StatusRegistry.QueueJoin(s.UserID, presence.StatusPresence{
		UserID:    s.UserID,
		SessionID: s.ID,
		Username:  s.Username,
		Status:    "",
	})
	gh.publishPresenceMesh("join", s.UserID, presence.StatusPresence{
		UserID:    s.UserID,
		SessionID: s.ID,
		Username:  s.Username,
		Status:    "",
	})
}

// UntrackStatusOnDisconnect removes status presence and follow edges after grace expiry.
func (gh *GatewayHandler) UntrackStatusOnDisconnect(sessionID, userID, username string) {
	if gh.StreamTracker != nil {
		if userID != "" {
			if p, had := gh.StreamTracker.Untrack(sessionID, presence.StatusStream(userID)); had {
				uname := p.Meta.Username
				if uname == "" {
					uname = username
				}
				if gh.StatusRegistry != nil {
					sp := presence.StatusPresence{
						UserID:    userID,
						SessionID: sessionID,
						Username:  uname,
					}
					gh.StatusRegistry.QueueLeave(userID, sp)
					gh.publishPresenceMesh("leave", userID, sp)
				}
			}
		}
		removals := gh.StreamTracker.UntrackAllDetailed(sessionID)
		runtime.EmitStreamPresenceRemovals(gh.MessageRouter, gh.StreamTracker, removals)
	}
	if gh.StatusRegistry != nil {
		gh.StatusRegistry.UnfollowAll(sessionID)
	}
}

func (gh *GatewayHandler) publishPresenceMesh(kind, userID string, p presence.StatusPresence) {
	mesh := gh.clusterMesh()
	if mesh == nil {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"user_id": userID,
		"presence": map[string]interface{}{
			"user_id":    p.UserID,
			"session_id": p.SessionID,
			"username":   p.Username,
			"status":     p.Status,
		},
	})
	_ = mesh.PublishPresence(context.Background(), cluster.PresenceEventMessage{
		Kind:    kind,
		Payload: payload,
	})
}

// DeliverClusterPresence applies a remote status join/leave/update to local followers.
func (gh *GatewayHandler) DeliverClusterPresence(kind string, payload []byte) {
	if gh.StatusRegistry == nil {
		return
	}
	var body struct {
		UserID   string `json:"user_id"`
		Presence struct {
			UserID    string `json:"user_id"`
			SessionID string `json:"session_id"`
			Username  string `json:"username"`
			Status    string `json:"status"`
		} `json:"presence"`
	}
	if err := json.Unmarshal(payload, &body); err != nil || body.UserID == "" {
		return
	}
	sp := presence.StatusPresence{
		UserID:    body.Presence.UserID,
		SessionID: body.Presence.SessionID,
		Username:  body.Presence.Username,
		Status:    body.Presence.Status,
	}
	if kind == "leave" {
		gh.StatusRegistry.QueueLeave(body.UserID, sp)
		return
	}
	gh.StatusRegistry.QueueJoin(body.UserID, sp)
}

// StatusFollow adds follow edges for a session (runtime nk.status_follow).
func (gh *GatewayHandler) StatusFollow(sessionID string, userIDs []string) error {
	if gh.StatusRegistry == nil {
		return fmt.Errorf("presence not configured")
	}
	if gh.StatusRegistry.FollowCount(sessionID)+len(userIDs) > gh.maxSubscriptions() {
		return fmt.Errorf("maximum status subscriptions is %d", gh.maxSubscriptions())
	}
	gh.StatusRegistry.Follow(sessionID, userIDs)
	return nil
}

// StatusUnfollow removes follow edges for a session (runtime nk.status_unfollow).
func (gh *GatewayHandler) StatusUnfollow(sessionID string, userIDs []string) error {
	if gh.StatusRegistry == nil {
		return fmt.Errorf("presence not configured")
	}
	gh.StatusRegistry.Unfollow(sessionID, userIDs)
	return nil
}
