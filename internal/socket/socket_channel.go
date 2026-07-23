package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"ultimate-game-server/internal/chat"

	"github.com/google/uuid"
)

const (
	channelTypeRoom  = 1
	channelTypeDM    = 2
	channelTypeGroup = 3
)

func buildChannelID(userID, target string, typ int) (string, error) {
	if target == "" {
		return "", fmt.Errorf("channel target required")
	}
	switch typ {
	case channelTypeRoom, 0:
		return fmt.Sprintf("1...%s", target), nil
	case channelTypeDM:
		a, b := userID, target
		if a > b {
			a, b = b, a
		}
		return fmt.Sprintf("2.%s.%s.", a, b), nil
	case channelTypeGroup:
		return fmt.Sprintf("3.%s..", target), nil
	default:
		return "", fmt.Errorf("invalid channel type")
	}
}

func (gh *GatewayHandler) handleChannelJoin(s *Session, cid string, req *ChannelJoinPayload) {
	channelID, err := buildChannelID(s.UserID, req.Target, req.Type)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}

	gh.mu.Lock()
	room, ok := gh.channels[channelID]
	if !ok {
		room = make(map[string]*Session)
		gh.channels[channelID] = room
	}
	room[s.ID] = s
	presences := make([]map[string]interface{}, 0, len(room))
	others := make([]*Session, 0, len(room))
	for _, sess := range room {
		presences = append(presences, map[string]interface{}{
			"user_id":    sess.UserID,
			"username":   sess.Username,
			"session_id": sess.ID,
		})
		if sess.ID != s.ID {
			others = append(others, sess)
		}
	}
	gh.mu.Unlock()

	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"channel": map[string]interface{}{
			"id":        channelID,
			"presences": presences,
			"self": map[string]interface{}{
				"user_id":    s.UserID,
				"username":   s.Username,
				"session_id": s.ID,
			},
		},
	})
	s.TrySend(res)

	if len(others) > 0 {
		notif, _ := json.Marshal(map[string]interface{}{
			"channel_presence_event": map[string]interface{}{
				"channel_id": channelID,
				"joins": []map[string]interface{}{
					{
						"user_id":    s.UserID,
						"username":   s.Username,
						"session_id": s.ID,
					},
				},
				"leaves": []interface{}{},
			},
		})
		for _, sess := range others {
			sess.TrySend(notif)
		}
	}
}

func (gh *GatewayHandler) handleChannelLeave(s *Session, cid string, req *ChannelLeavePayload) {
	gh.removeFromChannel(s, req.ChannelID)
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"channel_leave": map[string]interface{}{
			"channel_id": req.ChannelID,
		},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handleChannelMessageSend(s *Session, cid string, req *ChannelMessageSendPayload) {
	trimmed := strings.TrimSpace(string(req.Content))
	if trimmed == "" || trimmed[0] != '{' {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "content must be a JSON object"})
		s.TrySend(res)
		return
	}

	var contentObj map[string]interface{}
	if err := json.Unmarshal(req.Content, &contentObj); err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "content must be a JSON object"})
		s.TrySend(res)
		return
	}

	gh.mu.RLock()
	room, ok := gh.channels[req.ChannelID]
	recipients := make([]*Session, 0)
	member := false
	if ok {
		recipients = make([]*Session, 0, len(room))
		for _, sess := range room {
			recipients = append(recipients, sess)
			if sess.ID == s.ID {
				member = true
			}
		}
	}
	dbPool := gh.dbPool
	gh.mu.RUnlock()

	if !ok || !member {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "not in channel"})
		s.TrySend(res)
		return
	}

	msgID := uuid.New().String()
	contentStr := string(req.Content)

	if dbPool != nil {
		streamMode, subject, descriptor, label := parseChannelID(req.ChannelID)
		_ = chat.SaveMessage(context.Background(), dbPool, &chat.Message{
			ID:               msgID,
			Code:             0,
			SenderID:         s.UserID,
			Username:         s.Username,
			StreamMode:       streamMode,
			StreamSubject:    subject,
			StreamDescriptor: descriptor,
			StreamLabel:      label,
			Content:          contentStr,
		})
	}

	msgPayload, _ := json.Marshal(map[string]interface{}{
		"channel_message": map[string]interface{}{
			"channel_id": req.ChannelID,
			"message_id": msgID,
			"code":       0,
			"sender_id":  s.UserID,
			"username":   s.Username,
			"content":    contentObj,
		},
	})
	for _, sess := range recipients {
		sess.TrySend(msgPayload)
	}

	ack, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"channel_message_ack": map[string]interface{}{
			"channel_id": req.ChannelID,
			"message_id": msgID,
			"code":       0,
		},
	})
	s.TrySend(ack)
}

func parseChannelID(channelID string) (mode int16, subject, descriptor, label string) {
	parts := strings.SplitN(channelID, ".", 4)
	if len(parts) < 4 {
		return chat.StreamModeRoom, "", "", channelID
	}
	switch parts[0] {
	case "1":
		mode = chat.StreamModeRoom
	case "2":
		mode = chat.StreamModeDirect
	case "3":
		mode = chat.StreamModeGroup
	default:
		mode = chat.StreamModeRoom
	}
	return mode, parts[1], parts[2], parts[3]
}

func (gh *GatewayHandler) removeFromChannel(s *Session, channelID string) {
	gh.mu.Lock()
	room, ok := gh.channels[channelID]
	if !ok {
		gh.mu.Unlock()
		return
	}
	delete(room, s.ID)
	others := make([]*Session, 0, len(room))
	for _, sess := range room {
		others = append(others, sess)
	}
	if len(room) == 0 {
		delete(gh.channels, channelID)
	}
	gh.mu.Unlock()

	if len(others) == 0 {
		return
	}
	notif, _ := json.Marshal(map[string]interface{}{
		"channel_presence_event": map[string]interface{}{
			"channel_id": channelID,
			"joins":      []interface{}{},
			"leaves": []map[string]interface{}{
				{
					"user_id":    s.UserID,
					"username":   s.Username,
					"session_id": s.ID,
				},
			},
		},
	})
	for _, sess := range others {
		sess.TrySend(notif)
	}
}

func (gh *GatewayHandler) leaveAllChannels(s *Session) {
	gh.mu.Lock()
	var channelIDs []string
	for id, room := range gh.channels {
		if _, ok := room[s.ID]; ok {
			channelIDs = append(channelIDs, id)
		}
	}
	gh.mu.Unlock()
	for _, id := range channelIDs {
		gh.removeFromChannel(s, id)
	}
}
