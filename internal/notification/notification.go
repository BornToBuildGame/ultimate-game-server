package notification

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Notification represents a notification stored in the database.
type Notification struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	Subject    string    `json:"subject"`
	Content    string    `json:"content"` // JSON string representation of content
	Code       int16     `json:"code"`
	SenderID   string    `json:"sender_id"`
	CreateTime time.Time `json:"create_time"`
}

// CreateNotification inserts a notification record and simulates external push triggers.
func CreateNotification(ctx context.Context, pool *pgxpool.Pool, notif *Notification) error {
	if notif.ID == "" {
		notif.ID = uuid.New().String()
	}
	if notif.CreateTime.IsZero() {
		notif.CreateTime = time.Now()
	}

	contentJson := notif.Content
	if contentJson == "" {
		contentJson = "{}"
	}

	query := `INSERT INTO notification (id, user_id, subject, content, code, sender_id, create_time) 
	          VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := pool.Exec(ctx, query,
		notif.ID,
		notif.UserID,
		notif.Subject,
		contentJson,
		notif.Code,
		notif.SenderID,
		notif.CreateTime,
	)
	if err != nil {
		return err
	}

	// Simulate external push triggers (APNS / FCM)
	go triggerExternalPush(notif)

	return nil
}

// ListNotifications retrieves recent notifications for a user.
func ListNotifications(ctx context.Context, pool *pgxpool.Pool, userID string, limit int) ([]Notification, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `SELECT id, user_id, subject, content, code, sender_id, create_time 
	          FROM notification
	          WHERE user_id = $1
	          ORDER BY create_time DESC
	          LIMIT $2`

	rows, err := pool.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Notification
	for rows.Next() {
		var n Notification
		var contentJson []byte
		err := rows.Scan(
			&n.ID,
			&n.UserID,
			&n.Subject,
			&contentJson,
			&n.Code,
			&n.SenderID,
			&n.CreateTime,
		)
		if err != nil {
			return nil, err
		}
		n.Content = string(contentJson)
		list = append(list, n)
	}

	return list, nil
}

// Global flag to count external push mock executions during testing
var ExternalPushCount int64

func triggerExternalPush(notif *Notification) {
	// In production, this resolves device push tokens and sends payload to APNS/FCM.
	// For testing and local validation, we just increment a mock counter.
	atomic.AddInt64(&ExternalPushCount, 1)
}
