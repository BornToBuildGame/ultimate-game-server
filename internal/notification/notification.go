package notification

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reserved system notification codes (reference-aligned).
const (
	CodeDmRequest        int16 = -1
	CodeFriendRequest    int16 = -2
	CodeFriendAccept     int16 = -3
	CodeGroupAdd         int16 = -4
	CodeGroupJoinRequest int16 = -5
	CodeFriendJoinGame   int16 = -6
	CodeSingleSocket     int16 = -7
	CodeUserBanned       int16 = -8
	CodeFriendRemove     int16 = -9
	CodeFriendImport     int16 = -1001 // UGE extension (custom negative band)
)

var (
	ErrNotificationCursorInvalid = errors.New("notification cursor invalid")
	ErrNotificationCodeInvalid   = errors.New("notification code invalid")
)

// Notification is an in-app notification (DB row and/or wire payload).
type Notification struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	Subject    string    `json:"subject"`
	Content    string    `json:"content"`
	Code       int16     `json:"code"`
	SenderID   string    `json:"sender_id"`
	CreateTime time.Time `json:"create_time"`
	Persistent bool      `json:"persistent"`
}

// List is a paginated list response.
type List struct {
	Notifications   []*Notification `json:"notifications"`
	CacheableCursor string           `json:"cacheable_cursor,omitempty"`
}

type notificationCacheableCursor struct {
	NotificationID string
	CreateTime     int64
}

// Deliverer pushes live notifications to online sessions.
type Deliverer interface {
	SendNotifications(userID string, notifs []*Notification)
	// SendNotificationsToAll delivers to every connected notifications-stream session (non-persistent send-all).
	SendNotificationsToAll(notifs []*Notification)
}

// DefaultDeliverer is set by the API/socket layer at startup.
var DefaultDeliverer Deliverer = noopDeliverer{}

type noopDeliverer struct{}

func (noopDeliverer) SendNotifications(string, []*Notification)      {}
func (noopDeliverer) SendNotificationsToAll([]*Notification)         {}

func delivererOrDefault(d Deliverer) Deliverer {
	if d != nil {
		return d
	}
	if DefaultDeliverer != nil {
		return DefaultDeliverer
	}
	return noopDeliverer{}
}

// ValidateRuntimeCode checks runtime/game send codes (>0 or [-2000,-1000]).
func ValidateRuntimeCode(code int16) error {
	if code > 0 {
		return nil
	}
	if code >= -2000 && code <= -1000 {
		return nil
	}
	return fmt.Errorf("%w: %d", ErrNotificationCodeInvalid, code)
}

// CreateNotification inserts a persistent notification (legacy helper). Prefer NotificationSend.
func CreateNotification(ctx context.Context, pool *pgxpool.Pool, notif *Notification) error {
	if notif == nil {
		return errors.New("nil notification")
	}
	notif.Persistent = true
	return NotificationSend(ctx, pool, nil, map[string][]*Notification{
		notif.UserID: {notif},
	})
}

// NotificationSend persists persistent notifications and delivers all to online recipients.
func NotificationSend(ctx context.Context, pool *pgxpool.Pool, deliver Deliverer, notifications map[string][]*Notification) error {
	if len(notifications) == 0 {
		return nil
	}
	persistent := make(map[string][]*Notification)
	for userID, ns := range notifications {
		for _, n := range ns {
			if n == nil {
				continue
			}
			if n.ID == "" {
				n.ID = uuid.New().String()
			}
			if n.CreateTime.IsZero() {
				n.CreateTime = time.Now().UTC()
			}
			if n.UserID == "" {
				n.UserID = userID
			}
			if n.Content == "" {
				n.Content = "{}"
			}
			if n.SenderID == "" {
				n.SenderID = uuid.Nil.String()
			}
			if n.Persistent {
				persistent[userID] = append(persistent[userID], n)
			}
		}
	}
	if len(persistent) > 0 && pool != nil {
		if err := NotificationSave(ctx, pool, persistent); err != nil {
			return err
		}
	}
	d := delivererOrDefault(deliver)
	for userID, ns := range notifications {
		if len(ns) == 0 {
			continue
		}
		d.SendNotifications(userID, ns)
	}
	return nil
}

// NotificationSave batch-inserts notifications.
func NotificationSave(ctx context.Context, pool *pgxpool.Pool, notifications map[string][]*Notification) error {
	if pool == nil || len(notifications) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO notification (id, user_id, subject, content, code, sender_id, create_time) VALUES `)
	args := make([]interface{}, 0, 64)
	argN := 1
	first := true
	for userID, ns := range notifications {
		for _, n := range ns {
			if !first {
				sb.WriteString(",")
			}
			first = false
			sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", argN, argN+1, argN+2, argN+3, argN+4, argN+5, argN+6))
			argN += 7
			uid := n.UserID
			if uid == "" {
				uid = userID
			}
			content := n.Content
			if content == "" {
				content = "{}"
			}
			sender := n.SenderID
			if sender == "" {
				sender = uuid.Nil.String()
			}
			args = append(args, n.ID, uid, n.Subject, content, n.Code, sender, n.CreateTime)
		}
	}
	if first {
		return nil
	}
	_, err := pool.Exec(ctx, sb.String(), args...)
	return err
}

func encodeCursor(c *notificationCacheableCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeCursor(cursor string) (*notificationCacheableCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	cb, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		cb, err = base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			cb, err = base64.StdEncoding.DecodeString(cursor)
			if err != nil {
				return nil, ErrNotificationCursorInvalid
			}
		}
	}
	out := &notificationCacheableCursor{}
	if err := gob.NewDecoder(bytes.NewReader(cb)).Decode(out); err != nil {
		return nil, ErrNotificationCursorInvalid
	}
	return out, nil
}

// NotificationList lists persistent notifications ASC with cacheable_cursor.
func NotificationList(ctx context.Context, pool *pgxpool.Pool, userID string, limit int, cursor string) (*List, error) {
	if limit <= 0 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	incoming, err := decodeCursor(cursor)
	if err != nil {
		return nil, err
	}

	query := `SELECT id, user_id, subject, content, code, sender_id, create_time FROM notification WHERE user_id = $1`
	args := []interface{}{userID, limit}
	if incoming != nil {
		query += ` AND (create_time, id) > ($3::TIMESTAMPTZ, $4::UUID)`
		args = append(args, time.Unix(0, incoming.CreateTime).UTC(), incoming.NotificationID)
	}
	query += ` ORDER BY create_time ASC, id ASC LIMIT $2`

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &List{Notifications: make([]*Notification, 0, limit)}
	for rows.Next() {
		n := &Notification{Persistent: true}
		var contentJSON []byte
		if err := rows.Scan(&n.ID, &n.UserID, &n.Subject, &contentJSON, &n.Code, &n.SenderID, &n.CreateTime); err != nil {
			return nil, err
		}
		n.Content = string(contentJSON)
		out.Notifications = append(out.Notifications, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if l := len(out.Notifications); l > 0 {
		last := out.Notifications[l-1]
		out.CacheableCursor, err = encodeCursor(&notificationCacheableCursor{
			NotificationID: last.ID,
			CreateTime:     last.CreateTime.UnixNano(),
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListNotifications is a convenience wrapper (limit default 100 for runtime).
func ListNotifications(ctx context.Context, pool *pgxpool.Pool, userID string, limit int) ([]*Notification, error) {
	if limit <= 0 {
		limit = 100
	}
	list, err := NotificationList(ctx, pool, userID, limit, "")
	if err != nil {
		return nil, err
	}
	return list.Notifications, nil
}

// NotificationDelete deletes notifications for a user by id.
func NotificationDelete(ctx context.Context, pool *pgxpool.Pool, userID string, ids []string) error {
	if pool == nil || len(ids) == 0 {
		return nil
	}
	_, err := pool.Exec(ctx, `DELETE FROM notification WHERE user_id = $1 AND id = ANY($2::UUID[])`, userID, ids)
	return err
}

// NotificationsGetId returns notifications by id for a user (empty userID = any user / admin).
func NotificationsGetId(ctx context.Context, pool *pgxpool.Pool, userID string, ids ...string) ([]*Notification, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	scanAll := func(query string, args ...interface{}) ([]*Notification, error) {
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []*Notification
		for rows.Next() {
			n := &Notification{Persistent: true}
			var contentJSON []byte
			if err := rows.Scan(&n.ID, &n.UserID, &n.Subject, &contentJSON, &n.Code, &n.SenderID, &n.CreateTime); err != nil {
				return nil, err
			}
			n.Content = string(contentJSON)
			out = append(out, n)
		}
		return out, rows.Err()
	}
	if userID != "" {
		return scanAll(`SELECT id, user_id, subject, content, code, sender_id, create_time FROM notification WHERE user_id = $1 AND id = ANY($2::UUID[])`, userID, ids)
	}
	return scanAll(`SELECT id, user_id, subject, content, code, sender_id, create_time FROM notification WHERE id = ANY($1::UUID[])`, ids)
}

// NotificationsDeleteId deletes by ids; empty userID deletes globally by id.
func NotificationsDeleteId(ctx context.Context, pool *pgxpool.Pool, userID string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if userID != "" {
		return NotificationDelete(ctx, pool, userID, ids)
	}
	_, err := pool.Exec(ctx, `DELETE FROM notification WHERE id = ANY($1::UUID[])`, ids)
	return err
}

// NotificationUpdate is a partial update request.
type NotificationUpdate struct {
	ID       string
	Subject  *string
	Content  *string
	SenderID *string
}

// NotificationsUpdate applies partial updates.
func NotificationsUpdate(ctx context.Context, pool *pgxpool.Pool, updates ...NotificationUpdate) error {
	for _, u := range updates {
		if u.ID == "" {
			continue
		}
		_, err := pool.Exec(ctx, `
UPDATE notification SET
  subject = COALESCE($2, subject),
  content = COALESCE($3::jsonb, content),
  sender_id = COALESCE($4::uuid, sender_id)
WHERE id = $1`,
			u.ID, u.Subject, u.Content, u.SenderID)
		if err != nil {
			return err
		}
	}
	return nil
}

// NotificationSendAll sends one notification to all users (persistent) or all connected (non-persistent).
func NotificationSendAll(ctx context.Context, pool *pgxpool.Pool, deliver Deliverer, n *Notification) error {
	if n == nil {
		return errors.New("nil notification")
	}
	if n.ID == "" {
		n.ID = uuid.New().String()
	}
	if n.CreateTime.IsZero() {
		n.CreateTime = time.Now().UTC()
	}
	if n.Content == "" {
		n.Content = "{}"
	}
	if n.SenderID == "" {
		n.SenderID = uuid.Nil.String()
	}
	d := delivererOrDefault(deliver)
	if !n.Persistent {
		d.SendNotificationsToAll([]*Notification{n})
		return nil
	}
	if pool == nil {
		return errors.New("db required for persistent send-all")
	}
	const page = 1000
	var lastID string
	for {
		query := `SELECT id FROM users ORDER BY id ASC LIMIT $1`
		args := []interface{}{page}
		if lastID != "" {
			query = `SELECT id FROM users WHERE id > $1::UUID ORDER BY id ASC LIMIT $2`
			args = []interface{}{lastID, page}
		}
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		batch := make(map[string][]*Notification)
		count := 0
		for rows.Next() {
			var uid string
			if err := rows.Scan(&uid); err != nil {
				rows.Close()
				return err
			}
			lastID = uid
			copyN := *n
			copyN.ID = uuid.New().String()
			copyN.UserID = uid
			batch[uid] = []*Notification{&copyN}
			count++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if err := NotificationSend(ctx, pool, d, batch); err != nil {
			return err
		}
		if count < page {
			break
		}
	}
	return nil
}
