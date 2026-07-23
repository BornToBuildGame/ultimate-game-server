package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ultimate-game-server/internal/chat"
	"ultimate-game-server/internal/notification"
	"ultimate-game-server/internal/social"

	"github.com/google/uuid"
)

type channelMemberMeta struct {
	Persistence bool
	Hidden      bool
}

func boolOrDefault(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

func (gh *GatewayHandler) sendChannelError(s *Session, cid, msg string) {
	res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": msg})
	s.TrySend(res)
}

func (gh *GatewayHandler) handleChannelJoin(s *Session, cid string, req *ChannelJoinPayload) {
	ctx := context.Background()
	channelID, stream, err := chat.BuildChannelId(ctx, gh.dbPool, s.UserID, req.Target, req.Type)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}

	meta := channelMemberMeta{
		Persistence: boolOrDefault(req.Persistence, true),
		Hidden:      boolOrDefault(req.Hidden, false),
	}

	if gh.StreamTracker != nil {
		gh.StreamTracker.Track(s.ID, chat.StreamKey(stream))
	}

	gh.mu.Lock()
	if gh.channelMeta == nil {
		gh.channelMeta = make(map[string]map[string]channelMemberMeta)
	}
	room, ok := gh.channels[channelID]
	isNew := true
	if !ok {
		room = make(map[string]*Session)
		gh.channels[channelID] = room
	} else if _, exists := room[s.ID]; exists {
		isNew = false
	}
	room[s.ID] = s
	if gh.channelMeta[channelID] == nil {
		gh.channelMeta[channelID] = make(map[string]channelMemberMeta)
	}
	gh.channelMeta[channelID][s.ID] = meta

	presences := make([]map[string]interface{}, 0, len(room))
	others := make([]*Session, 0, len(room))
	otherUserPresent := false
	for sid, sess := range room {
		m := gh.channelMeta[channelID][sid]
		if m.Hidden {
			continue
		}
		if isNew && sess.ID == s.ID {
			continue
		}
		presences = append(presences, map[string]interface{}{
			"user_id":     sess.UserID,
			"username":    sess.Username,
			"session_id":  sess.ID,
			"persistence": m.Persistence,
		})
		if sess.ID != s.ID {
			others = append(others, sess)
			if stream.Mode == chat.StreamModeDM && sess.UserID != s.UserID {
				otherUserPresent = true
			}
		}
	}
	gh.mu.Unlock()

	channel := map[string]interface{}{
		"id":        channelID,
		"presences": presences,
		"self": map[string]interface{}{
			"user_id":     s.UserID,
			"username":    s.Username,
			"session_id":  s.ID,
			"persistence": meta.Persistence,
		},
	}
	switch stream.Mode {
	case chat.StreamModeChannel:
		channel["room_name"] = stream.Label
	case chat.StreamModeGroup:
		channel["group_id"] = stream.Subject
	case chat.StreamModeDM:
		channel["user_id_one"] = stream.Subject
		channel["user_id_two"] = stream.Subcontext
	}

	res, _ := json.Marshal(map[string]interface{}{"cid": cid, "channel": channel})
	s.TrySend(res)

	if isNew && !meta.Hidden && len(others) > 0 {
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

	if isNew && stream.Mode == chat.StreamModeDM && !otherUserPresent && gh.dbPool != nil {
		otherUserID := stream.Subject
		if otherUserID == s.UserID {
			otherUserID = stream.Subcontext
		}
		content, _ := json.Marshal(map[string]string{"username": s.Username})
		_ = notification.CreateNotification(ctx, gh.dbPool, &notification.Notification{
			UserID:     otherUserID,
			Subject:    fmt.Sprintf("%s wants to chat", s.Username),
			Content:    string(content),
			Code:       notification.CodeDmRequest,
			SenderID:   s.UserID,
			Persistent: true,
		})
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

func (gh *GatewayHandler) channelMembership(s *Session, channelID string) (member bool, persist bool, recipients []*Session, stream chat.Stream, err error) {
	stream, err = chat.ChannelIdToStream(channelID)
	if err != nil {
		return false, false, nil, stream, err
	}
	gh.mu.RLock()
	defer gh.mu.RUnlock()
	room, ok := gh.channels[channelID]
	if !ok {
		return false, false, nil, stream, nil
	}
	recipients = make([]*Session, 0, len(room))
	for sid, sess := range room {
		recipients = append(recipients, sess)
		if sess.ID == s.ID {
			member = true
			if meta, mok := gh.channelMeta[channelID][sid]; mok {
				persist = meta.Persistence
			} else {
				persist = true
			}
		}
	}
	return member, persist, recipients, stream, nil
}

func (gh *GatewayHandler) broadcastChannelMessage(recipients []*Session, msg *chat.ChannelMessage) {
	payload := map[string]interface{}{
		"channel_message": map[string]interface{}{
			"channel_id":  msg.ChannelID,
			"message_id":  msg.MessageID,
			"code":        msg.Code,
			"sender_id":   msg.SenderID,
			"username":    msg.Username,
			"content":     json.RawMessage(msg.Content),
			"create_time": msg.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time": msg.UpdateTime.UTC().Format(time.RFC3339Nano),
			"persistent":  msg.Persistent,
		},
	}
	cm := payload["channel_message"].(map[string]interface{})
	if msg.RoomName != "" {
		cm["room_name"] = msg.RoomName
	}
	if msg.GroupID != "" {
		cm["group_id"] = msg.GroupID
	}
	if msg.UserIDOne != "" {
		cm["user_id_one"] = msg.UserIDOne
		cm["user_id_two"] = msg.UserIDTwo
	}
	bytes, _ := json.Marshal(payload)
	for _, sess := range recipients {
		sess.TrySend(bytes)
	}
}

func (gh *GatewayHandler) sendChannelAck(s *Session, cid string, ack *chat.ChannelMessageAck) {
	payload := map[string]interface{}{
		"cid": cid,
		"channel_message_ack": map[string]interface{}{
			"channel_id":  ack.ChannelID,
			"message_id":  ack.MessageID,
			"code":        ack.Code,
			"username":    ack.Username,
			"create_time": ack.CreateTime.UTC().Format(time.RFC3339Nano),
			"update_time": ack.UpdateTime.UTC().Format(time.RFC3339Nano),
			"persistent":  ack.Persistent,
		},
	}
	a := payload["channel_message_ack"].(map[string]interface{})
	if ack.RoomName != "" {
		a["room_name"] = ack.RoomName
	}
	if ack.GroupID != "" {
		a["group_id"] = ack.GroupID
	}
	if ack.UserIDOne != "" {
		a["user_id_one"] = ack.UserIDOne
		a["user_id_two"] = ack.UserIDTwo
	}
	bytes, _ := json.Marshal(payload)
	s.TrySend(bytes)
}

func validateJSONObjectContent(raw json.RawMessage) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed[0] != '{' {
		return "", fmt.Errorf("content must be a JSON object")
	}
	var contentObj map[string]interface{}
	if err := json.Unmarshal(raw, &contentObj); err != nil {
		return "", fmt.Errorf("content must be a JSON object")
	}
	return string(raw), nil
}

func (gh *GatewayHandler) ensureGroupMemberOnSend(stream chat.Stream, userID string) error {
	if stream.Mode != chat.StreamModeGroup || gh.dbPool == nil {
		return nil
	}
	ok, err := social.IsGroupMember(context.Background(), gh.dbPool, userID, stream.Subject)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("not a group member")
	}
	return nil
}

func (gh *GatewayHandler) handleChannelMessageSend(s *Session, cid string, req *ChannelMessageSendPayload) {
	contentStr, err := validateJSONObjectContent(req.Content)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}

	member, persist, recipients, stream, err := gh.channelMembership(s, req.ChannelID)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	if !member {
		gh.sendChannelError(s, cid, "not in channel")
		return
	}
	if err := gh.ensureGroupMemberOnSend(stream, s.UserID); err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}

	ack, msg, err := chat.ChannelMessageSend(context.Background(), gh.dbPool, stream, req.ChannelID, contentStr, s.UserID, s.Username, persist)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	gh.broadcastChannelMessage(recipients, msg)
	gh.sendChannelAck(s, cid, ack)
}

func (gh *GatewayHandler) handleChannelMessageUpdate(s *Session, cid string, req *ChannelMessageUpdatePayload) {
	contentStr, err := validateJSONObjectContent(req.Content)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	if req.MessageID == "" {
		gh.sendChannelError(s, cid, "message_id required")
		return
	}

	member, persist, recipients, stream, err := gh.channelMembership(s, req.ChannelID)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	if !member {
		gh.sendChannelError(s, cid, "not in channel")
		return
	}
	if err := gh.ensureGroupMemberOnSend(stream, s.UserID); err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}

	ack, msg, err := chat.ChannelMessageUpdate(context.Background(), gh.dbPool, stream, req.ChannelID, req.MessageID, contentStr, s.UserID, s.Username, persist)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	gh.broadcastChannelMessage(recipients, msg)
	gh.sendChannelAck(s, cid, ack)
}

func (gh *GatewayHandler) handleChannelMessageRemove(s *Session, cid string, req *ChannelMessageRemovePayload) {
	if req.MessageID == "" {
		gh.sendChannelError(s, cid, "message_id required")
		return
	}

	member, persist, recipients, stream, err := gh.channelMembership(s, req.ChannelID)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	if !member {
		gh.sendChannelError(s, cid, "not in channel")
		return
	}
	if err := gh.ensureGroupMemberOnSend(stream, s.UserID); err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}

	ack, msg, err := chat.ChannelMessageRemove(context.Background(), gh.dbPool, stream, req.ChannelID, req.MessageID, s.UserID, s.Username, persist)
	if err != nil {
		gh.sendChannelError(s, cid, err.Error())
		return
	}
	gh.broadcastChannelMessage(recipients, msg)
	gh.sendChannelAck(s, cid, ack)
}

func (gh *GatewayHandler) removeFromChannel(s *Session, channelID string) {
	stream, streamErr := chat.ChannelIdToStream(channelID)

	gh.mu.Lock()
	room, ok := gh.channels[channelID]
	if !ok {
		gh.mu.Unlock()
		return
	}
	meta := channelMemberMeta{}
	if m, mok := gh.channelMeta[channelID][s.ID]; mok {
		meta = m
	}
	delete(room, s.ID)
	if gh.channelMeta[channelID] != nil {
		delete(gh.channelMeta[channelID], s.ID)
		if len(gh.channelMeta[channelID]) == 0 {
			delete(gh.channelMeta, channelID)
		}
	}
	others := make([]*Session, 0, len(room))
	for _, sess := range room {
		others = append(others, sess)
	}
	if len(room) == 0 {
		delete(gh.channels, channelID)
	}
	gh.mu.Unlock()

	if streamErr == nil && gh.StreamTracker != nil {
		gh.StreamTracker.Untrack(s.ID, chat.StreamKey(stream))
	}

	if meta.Hidden || len(others) == 0 {
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

// BroadcastChannelMessage implements chat.MessageRouter for local WS fan-out.
func (gh *GatewayHandler) BroadcastChannelMessage(channelID string, msg *chat.ChannelMessage) {
	gh.mu.RLock()
	room := gh.channels[channelID]
	recipients := make([]*Session, 0, len(room))
	for _, sess := range room {
		recipients = append(recipients, sess)
	}
	gh.mu.RUnlock()
	gh.broadcastChannelMessage(recipients, msg)
}

// BroadcastGroupChannelMessage implements social.ChannelBroadcaster.
func (gh *GatewayHandler) BroadcastGroupChannelMessage(ctx context.Context, groupID string, code int16, content, senderID, username string) {
	stream := chat.Stream{Mode: chat.StreamModeGroup, Subject: groupID}
	channelID, err := chat.StreamToChannelId(stream)
	if err != nil {
		return
	}
	if content == "" {
		content = "{}"
	}
	ts := time.Now().UTC()
	msgID := uuid.New().String()
	msg := &chat.ChannelMessage{
		ChannelID:  channelID,
		MessageID:  msgID,
		Code:       code,
		SenderID:   senderID,
		Username:   username,
		Content:    content,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: true,
		GroupID:    groupID,
	}
	if gh.dbPool != nil {
		_ = chat.SaveMessage(ctx, gh.dbPool, &chat.Message{
			ID:               msgID,
			Code:             code,
			SenderID:         senderID,
			Username:         username,
			StreamMode:       chat.StreamModeGroup,
			StreamSubject:    groupID,
			StreamDescriptor: uuid.Nil.String(),
			StreamLabel:      "",
			Content:          content,
			CreateTime:       ts,
			UpdateTime:       ts,
		})
	}
	gh.mu.RLock()
	room := gh.channels[channelID]
	recipients := make([]*Session, 0, len(room))
	for _, sess := range room {
		recipients = append(recipients, sess)
	}
	gh.mu.RUnlock()
	gh.broadcastChannelMessage(recipients, msg)
}
