package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"ultimate-game-server/internal/presence"
	"ultimate-game-server/internal/social"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Join types for channel_join.type (reference-aligned).
const (
	ChannelJoinUnspecified = 0
	ChannelJoinRoom        = 1
	ChannelJoinDM          = 2
	ChannelJoinGroup       = 3
)

// Stream modes (DB + channel ID prefix) — aligned with presence / ADR-0009 / ADR-0013.
const (
	StreamModeChannel = presence.StreamModeChannel // 2 room
	StreamModeGroup   = presence.StreamModeGroup   // 3 group
	StreamModeDM      = presence.StreamModeDM      // 4 DM
)

// Message type codes.
const (
	MessageCodeChat         int16 = 0
	MessageCodeChatUpdate   int16 = 1
	MessageCodeChatRemove   int16 = 2
	MessageCodeGroupJoin    int16 = 3
	MessageCodeGroupAdd     int16 = 4
	MessageCodeGroupLeave   int16 = 5
	MessageCodeGroupKick    int16 = 6
	MessageCodeGroupPromote int16 = 7
	MessageCodeGroupBan     int16 = 8
	MessageCodeGroupDemote  int16 = 9
)

// NotificationCodeDmRequest is sent when a user newly joins a DM and the other party is absent.
const NotificationCodeDmRequest int16 = -1

var (
	ErrChannelIDInvalid     = errors.New("invalid channel id")
	ErrInvalidChannelTarget = errors.New("invalid channel target")
	ErrInvalidChannelType   = errors.New("invalid channel type")
	ErrChannelMessageNotFound = errors.New("channel message not found")
	ErrChannelCursorInvalid = errors.New("channel cursor invalid")
	ErrChannelGroupNotFound = errors.New("channel group not found")
)

var controlCharsRegex = regexp.MustCompilePOSIX("[[:cntrl:]]+")

// Stream is the presence/stream identity for a channel.
type Stream struct {
	Mode       int16
	Subject    string // UUID string or empty → uuid.Nil in DB
	Subcontext string
	Label      string
}

// Message represents a chat message stored in the database.
type Message struct {
	ID               string    `json:"id"`
	Code             int16     `json:"code"`
	SenderID         string    `json:"sender_id"`
	Username         string    `json:"username"`
	StreamMode       int16     `json:"stream_mode"`
	StreamSubject    string    `json:"stream_subject"`
	StreamDescriptor string    `json:"stream_descriptor"`
	StreamLabel      string    `json:"stream_label"`
	Content          string    `json:"content"`
	CreateTime       time.Time `json:"create_time"`
	UpdateTime       time.Time `json:"update_time"`
}

// MessageRouter broadcasts channel messages to local WebSocket subscribers.
type MessageRouter interface {
	BroadcastChannelMessage(channelID string, msg *ChannelMessage)
}

// DefaultRouter is set by the socket gateway at startup.
var DefaultRouter MessageRouter

type noopRouter struct{}

func (noopRouter) BroadcastChannelMessage(string, *ChannelMessage) {}

func routerOrDefault() MessageRouter {
	if DefaultRouter != nil {
		return DefaultRouter
	}
	return noopRouter{}
}

// ChannelMessage is the wire-facing message payload.
type ChannelMessage struct {
	ChannelID  string    `json:"channel_id"`
	MessageID  string    `json:"message_id"`
	Code       int16     `json:"code"`
	SenderID   string    `json:"sender_id"`
	Username   string    `json:"username"`
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
	Persistent bool      `json:"persistent"`
	RoomName   string    `json:"room_name,omitempty"`
	GroupID    string    `json:"group_id,omitempty"`
	UserIDOne  string    `json:"user_id_one,omitempty"`
	UserIDTwo  string    `json:"user_id_two,omitempty"`
}

// ChannelMessageAck is the send/update/remove confirmation to the originator.
type ChannelMessageAck struct {
	ChannelID  string    `json:"channel_id"`
	MessageID  string    `json:"message_id"`
	Code       int16     `json:"code"`
	Username   string    `json:"username"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
	Persistent bool      `json:"persistent"`
	RoomName   string    `json:"room_name,omitempty"`
	GroupID    string    `json:"group_id,omitempty"`
	UserIDOne  string    `json:"user_id_one,omitempty"`
	UserIDTwo  string    `json:"user_id_two,omitempty"`
}

// ChannelMessageList is a paginated history response.
type ChannelMessageList struct {
	Messages        []ChannelMessage `json:"messages"`
	NextCursor      string           `json:"next_cursor,omitempty"`
	PrevCursor      string           `json:"prev_cursor,omitempty"`
	CacheableCursor string           `json:"cacheable_cursor,omitempty"`
}

type channelMessageListCursor struct {
	StreamMode       uint8
	StreamSubject    string
	StreamSubcontext string
	StreamLabel      string
	CreateTime       int64
	Id               string
	Forward          bool
	IsNext           bool
}

func uuidOrNil(s string) string {
	if s == "" {
		return uuid.Nil.String()
	}
	return s
}

func subjectString(s string) string {
	if s == "" || s == uuid.Nil.String() {
		return ""
	}
	return s
}

// StreamToChannelId encodes a stream as a four-part channel ID.
func StreamToChannelId(stream Stream) (string, error) {
	if stream.Mode != StreamModeChannel && stream.Mode != StreamModeGroup && stream.Mode != StreamModeDM {
		return "", ErrChannelIDInvalid
	}
	return fmt.Sprintf("%d.%s.%s.%s", stream.Mode, subjectString(stream.Subject), subjectString(stream.Subcontext), stream.Label), nil
}

// ChannelIdToStream parses a four-part channel ID.
func ChannelIdToStream(channelID string) (Stream, error) {
	if channelID == "" {
		return Stream{}, ErrChannelIDInvalid
	}
	components := strings.SplitN(channelID, ".", 4)
	if len(components) != 4 {
		return Stream{}, ErrChannelIDInvalid
	}

	stream := Stream{Mode: StreamModeChannel}
	switch components[0] {
	case "2":
		if components[1] != "" || components[2] != "" {
			return Stream{}, ErrChannelIDInvalid
		}
		if l := len(components[3]); l < 1 || l > 64 {
			return Stream{}, ErrChannelIDInvalid
		}
		stream.Label = components[3]
	case "3":
		if components[2] != "" || components[3] != "" {
			return Stream{}, ErrChannelIDInvalid
		}
		if components[1] != "" {
			if _, err := uuid.Parse(components[1]); err != nil {
				return Stream{}, ErrChannelIDInvalid
			}
			stream.Subject = components[1]
		}
		stream.Mode = StreamModeGroup
	case "4":
		if components[3] != "" {
			return Stream{}, ErrChannelIDInvalid
		}
		if components[1] != "" {
			if _, err := uuid.Parse(components[1]); err != nil {
				return Stream{}, ErrChannelIDInvalid
			}
			stream.Subject = components[1]
		}
		if components[2] != "" {
			if _, err := uuid.Parse(components[2]); err != nil {
				return Stream{}, ErrChannelIDInvalid
			}
			stream.Subcontext = components[2]
		}
		stream.Mode = StreamModeDM
	default:
		return Stream{}, ErrChannelIDInvalid
	}
	return stream, nil
}

// StreamKey converts a chat Stream to a presence StreamKey.
func StreamKey(stream Stream) presence.StreamKey {
	return presence.StreamKey{
		Mode:       stream.Mode,
		Subject:    stream.Subject,
		Subcontext: stream.Subcontext,
		Label:      stream.Label,
	}
}

// BuildChannelId builds a channel ID and stream from join type + target.
// userID may be empty (uuid.Nil semantics) for authoritative runtime calls that skip ACL.
func BuildChannelId(ctx context.Context, pool *pgxpool.Pool, userID, target string, joinType int) (string, Stream, error) {
	if target == "" {
		return "", Stream{}, ErrInvalidChannelTarget
	}

	stream := Stream{Mode: StreamModeChannel}

	switch joinType {
	case ChannelJoinUnspecified, ChannelJoinRoom:
		if len(target) < 1 || len(target) > 64 {
			return "", Stream{}, fmt.Errorf("channel name must be 1-64 chars: %w", ErrInvalidChannelTarget)
		}
		if controlCharsRegex.MatchString(target) {
			return "", Stream{}, fmt.Errorf("channel name must not contain control chars: %w", ErrInvalidChannelTarget)
		}
		if !utf8.ValidString(target) {
			return "", Stream{}, fmt.Errorf("channel name must only contain valid UTF-8: %w", ErrInvalidChannelTarget)
		}
		stream.Label = target
	case ChannelJoinDM:
		uid, err := uuid.Parse(target)
		if err != nil || uid == uuid.Nil {
			return "", Stream{}, fmt.Errorf("invalid user ID in direct message join: %w", ErrInvalidChannelTarget)
		}
		if userID != "" && pool != nil {
			allowed, err := social.UserExistsAndDoesNotBlock(ctx, pool, target, userID)
			if err != nil {
				return "", Stream{}, fmt.Errorf("failed to look up user ID: %w", err)
			}
			if !allowed {
				return "", Stream{}, fmt.Errorf("user ID not found: %w", ErrInvalidChannelTarget)
			}
		}
		callerStr := userID
		if callerStr == "" {
			callerStr = uuid.Nil.String()
		}
		targetStr := uid.String()
		if targetStr > callerStr {
			stream.Subject = callerStr
			stream.Subcontext = targetStr
		} else {
			stream.Subject = targetStr
			stream.Subcontext = callerStr
		}
		stream.Mode = StreamModeDM
	case ChannelJoinGroup:
		gid, err := uuid.Parse(target)
		if err != nil {
			return "", Stream{}, fmt.Errorf("invalid group ID in group channel join: %w", ErrInvalidChannelTarget)
		}
		if userID != "" && pool != nil {
			ok, err := social.IsGroupMember(ctx, pool, userID, target)
			if err != nil {
				return "", Stream{}, err
			}
			if !ok {
				return "", Stream{}, ErrChannelGroupNotFound
			}
		}
		stream.Subject = gid.String()
		stream.Mode = StreamModeGroup
	default:
		return "", Stream{}, ErrInvalidChannelType
	}

	channelID, err := StreamToChannelId(stream)
	if err != nil {
		return "", Stream{}, err
	}
	return channelID, stream, nil
}

func applyTypeFields(msg *ChannelMessage, ack *ChannelMessageAck, stream Stream) {
	switch stream.Mode {
	case StreamModeChannel:
		msg.RoomName = stream.Label
		if ack != nil {
			ack.RoomName = stream.Label
		}
	case StreamModeGroup:
		msg.GroupID = stream.Subject
		if ack != nil {
			ack.GroupID = stream.Subject
		}
	case StreamModeDM:
		msg.UserIDOne = stream.Subject
		msg.UserIDTwo = stream.Subcontext
		if ack != nil {
			ack.UserIDOne = stream.Subject
			ack.UserIDTwo = stream.Subcontext
		}
	}
}

// ChannelMessageSend persists (optional) and returns an ack payload for the sender.
func ChannelMessageSend(ctx context.Context, pool *pgxpool.Pool, stream Stream, channelID, content, senderID, senderUsername string, persist bool) (*ChannelMessageAck, *ChannelMessage, error) {
	ts := time.Now().UTC()
	msgID := uuid.New().String()
	msg := &ChannelMessage{
		ChannelID:  channelID,
		MessageID:  msgID,
		Code:       MessageCodeChat,
		SenderID:   senderID,
		Username:   senderUsername,
		Content:    content,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	ack := &ChannelMessageAck{
		ChannelID:  channelID,
		MessageID:  msgID,
		Code:       MessageCodeChat,
		Username:   senderUsername,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	applyTypeFields(msg, ack, stream)

	if persist && pool != nil {
		if err := SaveMessage(ctx, pool, &Message{
			ID:               msgID,
			Code:             MessageCodeChat,
			SenderID:         senderID,
			Username:         senderUsername,
			StreamMode:       stream.Mode,
			StreamSubject:    uuidOrNil(stream.Subject),
			StreamDescriptor: uuidOrNil(stream.Subcontext),
			StreamLabel:      stream.Label,
			Content:          content,
			CreateTime:       ts,
			UpdateTime:       ts,
		}); err != nil {
			return nil, nil, err
		}
	}
	return ack, msg, nil
}

// ChannelMessageUpdate updates a message (sender-only when persist).
func ChannelMessageUpdate(ctx context.Context, pool *pgxpool.Pool, stream Stream, channelID, messageID, content, senderID, senderUsername string, persist bool) (*ChannelMessageAck, *ChannelMessage, error) {
	ts := time.Now().UTC()
	msg := &ChannelMessage{
		ChannelID:  channelID,
		MessageID:  messageID,
		Code:       MessageCodeChatUpdate,
		SenderID:   senderID,
		Username:   senderUsername,
		Content:    content,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	ack := &ChannelMessageAck{
		ChannelID:  channelID,
		MessageID:  messageID,
		Code:       MessageCodeChatUpdate,
		Username:   senderUsername,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	applyTypeFields(msg, ack, stream)

	if persist && pool != nil {
		var createTime time.Time
		err := pool.QueryRow(ctx,
			`UPDATE message SET update_time = $5, username = $4, content = $3, code = $6 WHERE id = $1 AND sender_id = $2 RETURNING create_time`,
			messageID, senderID, content, senderUsername, ts, MessageCodeChatUpdate,
		).Scan(&createTime)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrChannelMessageNotFound
		}
		if err != nil {
			return nil, nil, err
		}
		msg.CreateTime = createTime
		ack.CreateTime = createTime
	}
	return ack, msg, nil
}

// ChannelMessageRemove deletes a message (sender-only when persist) and returns remove broadcast payload.
func ChannelMessageRemove(ctx context.Context, pool *pgxpool.Pool, stream Stream, channelID, messageID, senderID, senderUsername string, persist bool) (*ChannelMessageAck, *ChannelMessage, error) {
	ts := time.Now().UTC()
	msg := &ChannelMessage{
		ChannelID:  channelID,
		MessageID:  messageID,
		Code:       MessageCodeChatRemove,
		SenderID:   senderID,
		Username:   senderUsername,
		Content:    "{}",
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	ack := &ChannelMessageAck{
		ChannelID:  channelID,
		MessageID:  messageID,
		Code:       MessageCodeChatRemove,
		Username:   senderUsername,
		CreateTime: ts,
		UpdateTime: ts,
		Persistent: persist,
	}
	applyTypeFields(msg, ack, stream)

	if persist && pool != nil {
		var createTime, updateTime time.Time
		err := pool.QueryRow(ctx,
			`DELETE FROM message WHERE id = $1 AND sender_id = $2 RETURNING create_time, update_time`,
			messageID, senderID,
		).Scan(&createTime, &updateTime)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrChannelMessageNotFound
		}
		if err != nil {
			return nil, nil, err
		}
		msg.CreateTime = createTime
		msg.UpdateTime = updateTime
		ack.CreateTime = createTime
		ack.UpdateTime = updateTime
	}
	return ack, msg, nil
}

// SaveMessage persists a chat message to PostgreSQL.
func SaveMessage(ctx context.Context, pool *pgxpool.Pool, msg *Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreateTime.IsZero() {
		msg.CreateTime = time.Now().UTC()
	}
	if msg.UpdateTime.IsZero() {
		msg.UpdateTime = msg.CreateTime
	}
	contentJSON := msg.Content
	if contentJSON == "" {
		contentJSON = "{}"
	}
	_, err := pool.Exec(ctx, `
INSERT INTO message (id, code, sender_id, username, stream_mode, stream_subject, stream_descriptor, stream_label, content, create_time, update_time)
VALUES ($1, $2, $3, $4, $5, $6::UUID, $7::UUID, $8, $9, $10, $11)`,
		msg.ID, msg.Code, msg.SenderID, msg.Username, msg.StreamMode,
		uuidOrNil(msg.StreamSubject), uuidOrNil(msg.StreamDescriptor), msg.StreamLabel,
		contentJSON, msg.CreateTime, msg.UpdateTime)
	return err
}

func encodeCursor(c *channelMessageListCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeCursor(cursor string) (*channelMessageListCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	cb, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		cb, err = base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return nil, ErrChannelCursorInvalid
		}
	}
	out := &channelMessageListCursor{}
	if err := gob.NewDecoder(bytes.NewReader(cb)).Decode(out); err != nil {
		return nil, ErrChannelCursorInvalid
	}
	return out, nil
}

// ChannelMessagesList lists history. caller empty skips authorization (runtime/server-side).
func ChannelMessagesList(ctx context.Context, pool *pgxpool.Pool, caller string, stream Stream, channelID string, limit int, forward bool, cursor string) (*ChannelMessageList, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	incoming, err := decodeCursor(cursor)
	if err != nil {
		return nil, err
	}
	if incoming != nil {
		if forward != incoming.Forward ||
			uint8(stream.Mode) != incoming.StreamMode ||
			uuidOrNil(stream.Subject) != incoming.StreamSubject ||
			uuidOrNil(stream.Subcontext) != incoming.StreamSubcontext ||
			stream.Label != incoming.StreamLabel {
			return nil, ErrChannelCursorInvalid
		}
	}

	if caller != "" {
		switch stream.Mode {
		case StreamModeGroup:
			ok, err := social.IsGroupMember(ctx, pool, caller, stream.Subject)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, ErrChannelGroupNotFound
			}
		case StreamModeDM:
			if stream.Subject != caller && stream.Subcontext != caller {
				return nil, ErrChannelIDInvalid
			}
		}
	}

	subject := uuidOrNil(stream.Subject)
	descriptor := uuidOrNil(stream.Subcontext)

	query := `SELECT id, code, sender_id, username, content, create_time, update_time FROM message
WHERE stream_mode = $1 AND stream_subject = $2::UUID AND stream_descriptor = $3::UUID AND stream_label = $4`
	params := []interface{}{stream.Mode, subject, descriptor, stream.Label, limit + 1}

	if incoming == nil {
		if forward {
			query += " ORDER BY create_time ASC, id ASC"
		} else {
			query += " ORDER BY create_time DESC, id DESC"
		}
	} else {
		if (forward && incoming.IsNext) || (!forward && !incoming.IsNext) {
			query += " AND (stream_mode, stream_subject, stream_descriptor, stream_label, create_time, id) > ($1, $2::UUID, $3::UUID, $4, $6, $7) ORDER BY create_time ASC, id ASC"
		} else {
			query += " AND (stream_mode, stream_subject, stream_descriptor, stream_label, create_time, id) < ($1, $2::UUID, $3::UUID, $4, $6, $7) ORDER BY create_time DESC, id DESC"
		}
		params = append(params, time.Unix(0, incoming.CreateTime).UTC(), incoming.Id)
	}
	query += " LIMIT $5"

	rows, err := pool.Query(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := make([]ChannelMessage, 0, limit)
	var nextCursor, prevCursor *channelMessageListCursor
	var lastCreate time.Time
	var lastID string

	for rows.Next() {
		var m ChannelMessage
		var contentJSON []byte
		var createTime, updateTime time.Time
		if err := rows.Scan(&m.MessageID, &m.Code, &m.SenderID, &m.Username, &contentJSON, &createTime, &updateTime); err != nil {
			return nil, err
		}
		if len(messages) >= limit {
			nextCursor = &channelMessageListCursor{
				StreamMode:       uint8(stream.Mode),
				StreamSubject:    subject,
				StreamSubcontext: descriptor,
				StreamLabel:      stream.Label,
				CreateTime:       lastCreate.UnixNano(),
				Id:               lastID,
				Forward:          forward,
				IsNext:           true,
			}
			break
		}
		m.ChannelID = channelID
		m.Content = string(contentJSON)
		m.CreateTime = createTime
		m.UpdateTime = updateTime
		m.Persistent = true
		applyTypeFields(&m, nil, stream)
		messages = append(messages, m)
		lastCreate = createTime
		lastID = m.MessageID

		if incoming != nil && prevCursor == nil {
			prevCursor = &channelMessageListCursor{
				StreamMode:       uint8(stream.Mode),
				StreamSubject:    subject,
				StreamSubcontext: descriptor,
				StreamLabel:      stream.Label,
				CreateTime:       createTime.UnixNano(),
				Id:               m.MessageID,
				Forward:          forward,
				IsNext:           false,
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if incoming != nil && !incoming.IsNext {
		nextCursor, prevCursor = prevCursor, nextCursor
		if nextCursor != nil {
			nextCursor.IsNext = !nextCursor.IsNext
		}
		if prevCursor != nil {
			prevCursor.IsNext = !prevCursor.IsNext
		}
		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
			messages[i], messages[j] = messages[j], messages[i]
		}
	}

	var cacheable *channelMessageListCursor
	if l := len(messages); l > 0 {
		cacheable = &channelMessageListCursor{
			StreamMode:       uint8(stream.Mode),
			StreamSubject:    subject,
			StreamSubcontext: descriptor,
			StreamLabel:      stream.Label,
			CreateTime:       messages[l-1].CreateTime.UnixNano(),
			Id:               messages[l-1].MessageID,
			Forward:          true,
			IsNext:           true,
		}
	}

	nextStr, err := encodeCursor(nextCursor)
	if err != nil {
		return nil, err
	}
	prevStr, err := encodeCursor(prevCursor)
	if err != nil {
		return nil, err
	}
	cacheStr, err := encodeCursor(cacheable)
	if err != nil {
		return nil, err
	}

	return &ChannelMessageList{
		Messages:        messages,
		NextCursor:      nextStr,
		PrevCursor:      prevStr,
		CacheableCursor: cacheStr,
	}, nil
}

// ListMessages is a simple helper retained for tests; prefer ChannelMessagesList.
func ListMessages(ctx context.Context, pool *pgxpool.Pool, streamMode int16, streamSubject, streamDescriptor, streamLabel string, limit int) ([]Message, error) {
	stream := Stream{
		Mode:       streamMode,
		Subject:    streamSubject,
		Subcontext: streamDescriptor,
		Label:      streamLabel,
	}
	channelID, err := StreamToChannelId(stream)
	if err != nil {
		// Allow listing with legacy/test modes by skipping ID encode.
		channelID = ""
	}
	list, err := ChannelMessagesList(ctx, pool, "", stream, channelID, limit, true, "")
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(list.Messages))
	for _, m := range list.Messages {
		out = append(out, Message{
			ID:               m.MessageID,
			Code:             m.Code,
			SenderID:         m.SenderID,
			Username:         m.Username,
			StreamMode:       streamMode,
			StreamSubject:    uuidOrNil(streamSubject),
			StreamDescriptor: uuidOrNil(streamDescriptor),
			StreamLabel:      streamLabel,
			Content:          m.Content,
			CreateTime:       m.CreateTime,
			UpdateTime:       m.UpdateTime,
		})
	}
	return out, nil
}
