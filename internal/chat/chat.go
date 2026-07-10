package chat

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamMode defines the type of chat stream.
const (
	StreamModeDirect = 0
	StreamModeGroup  = 1
	StreamModeRoom   = 2
)

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
	Content          string    `json:"content"` // JSON string representation of content
	CreateTime       time.Time `json:"create_time"`
	UpdateTime       time.Time `json:"update_time"`
}

// SaveMessage persists a chat message to PostgreSQL.
func SaveMessage(ctx context.Context, pool *pgxpool.Pool, msg *Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreateTime.IsZero() {
		msg.CreateTime = time.Now()
	}
	if msg.UpdateTime.IsZero() {
		msg.UpdateTime = msg.CreateTime
	}

	query := `INSERT INTO message (id, code, sender_id, username, stream_mode, stream_subject, stream_descriptor, stream_label, content, create_time, update_time) 
	          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	contentJson := msg.Content
	if contentJson == "" {
		contentJson = "{}"
	}

	_, err := pool.Exec(ctx, query,
		msg.ID,
		msg.Code,
		msg.SenderID,
		msg.Username,
		msg.StreamMode,
		msg.StreamSubject,
		msg.StreamDescriptor,
		msg.StreamLabel,
		contentJson,
		msg.CreateTime,
		msg.UpdateTime,
	)

	return err
}

// ListMessages retrieves historical messages from a chat stream.
func ListMessages(
	ctx context.Context,
	pool *pgxpool.Pool,
	streamMode int16,
	streamSubject, streamDescriptor, streamLabel string,
	limit int,
) ([]Message, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `SELECT id, code, sender_id, username, stream_mode, stream_subject, stream_descriptor, stream_label, content, create_time, update_time 
	          FROM message
	          WHERE stream_mode = $1 AND stream_subject = $2 AND stream_descriptor = $3 AND stream_label = $4
	          ORDER BY create_time DESC
	          LIMIT $5`

	rows, err := pool.Query(ctx, query, streamMode, streamSubject, streamDescriptor, streamLabel, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		var contentJson []byte
		err := rows.Scan(
			&m.ID,
			&m.Code,
			&m.SenderID,
			&m.Username,
			&m.StreamMode,
			&m.StreamSubject,
			&m.StreamDescriptor,
			&m.StreamLabel,
			&contentJson,
			&m.CreateTime,
			&m.UpdateTime,
		)
		if err != nil {
			return nil, err
		}
		m.Content = string(contentJson)
		messages = append(messages, m)
	}

	// Reverse to ascending chronological order
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	return messages, nil
}

// StreamRegistry manages real-time chat subscriptions and routing.
type StreamRegistry struct {
	mu      sync.RWMutex
	streams map[string]map[string]func(payload []byte) // streamKey -> sessionID -> sendCallback
}

// NewStreamRegistry creates a new StreamRegistry.
func NewStreamRegistry() *StreamRegistry {
	return &StreamRegistry{
		streams: make(map[string]map[string]func(payload []byte)),
	}
}

// Subscribe adds a session callback to a stream.
func (sr *StreamRegistry) Subscribe(streamKey, sessionID string, sendFn func(payload []byte)) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.streams == nil {
		sr.streams = make(map[string]map[string]func(payload []byte))
	}

	sessionMap, exists := sr.streams[streamKey]
	if !exists {
		sessionMap = make(map[string]func(payload []byte))
		sr.streams[streamKey] = sessionMap
	}
	sessionMap[sessionID] = sendFn
}

// Unsubscribe removes a session from a stream.
func (sr *StreamRegistry) Unsubscribe(streamKey, sessionID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.streams == nil {
		return
	}

	if sessionMap, exists := sr.streams[streamKey]; exists {
		delete(sessionMap, sessionID)
		if len(sessionMap) == 0 {
			delete(sr.streams, streamKey)
		}
	}
}

// Broadcast sends a message to all active subscribers on the stream.
func (sr *StreamRegistry) Broadcast(streamKey string, payload []byte) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	if sr.streams == nil {
		return
	}

	if sessionMap, exists := sr.streams[streamKey]; exists {
		for _, sendFn := range sessionMap {
			sendFn(payload)
		}
	}
}
