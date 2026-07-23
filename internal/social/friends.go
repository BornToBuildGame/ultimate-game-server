package social

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FriendState represents the states of a social edge.
const (
	StateFriend         = 0
	StateInviteSent     = 1
	StateInviteReceived = 2
	StateBlocked        = 3
)

// Notification codes for friend events (reference-engine aligned).
const (
	NotificationCodeFriendRequest = -2
	NotificationCodeFriendAccept  = -3
	NotificationCodeFriendImport  = -6 // UGE extension (Facebook/Steam import)
	NotificationCodeFriendRemove  = -9
)

// Default limits (PRD-07).
const (
	DefaultMaxFriends = 1000
	DefaultMaxPending = 100
	DefaultListLimit  = 100
	MaxListLimit      = 1000
)

var (
	ErrCannotAddSelf       = errors.New("cannot add yourself as friend")
	ErrCannotBlockSelf     = errors.New("cannot block yourself")
	ErrFriendLimitReached  = errors.New("friend count limit reached")
	ErrPendingLimitReached = errors.New("pending friend request limit reached")
	ErrInvalidCursor       = errors.New("invalid friends cursor")
)

// FriendNotifier dispatches friend-related notifications.
type FriendNotifier interface {
	Notify(ctx context.Context, userID, subject, content string, code int16, senderID string) error
}

// NoopNotifier is a no-op FriendNotifier used in tests.
type NoopNotifier struct{}

func (NoopNotifier) Notify(context.Context, string, string, string, int16, string) error {
	return nil
}

// DefaultNotifier is used when callers pass nil.
var DefaultNotifier FriendNotifier = NoopNotifier{}

// FriendUser holds profile fields returned in friend lists.
type FriendUser struct {
	ID          string
	Username    string
	DisplayName string
	AvatarURL   string
	LangTag     string
	Location    string
	Timezone    string
	Metadata    string
	CreateTime  time.Time
	UpdateTime  time.Time
	EdgeCount   int32
}

// Friend represents a friend record with edge metadata.
type Friend struct {
	User       FriendUser
	State      int
	UpdateTime time.Time
	Metadata   string
	Position   int64
}

// FriendOfFriend is a friends-of-friends discovery result.
type FriendOfFriend struct {
	Referrer string
	User     FriendUser
}

type edgeListCursor struct {
	State    int64
	Position int64
}

type friendsOfFriendsCursor struct {
	SourceID      string
	DestinationID string
}

// Config holds friend limit configuration.
type Config struct {
	MaxFriends int
	MaxPending int
}

// DefaultConfig returns PRD defaults.
func DefaultConfig() Config {
	return Config{MaxFriends: DefaultMaxFriends, MaxPending: DefaultMaxPending}
}

func notifierOrDefault(n FriendNotifier) FriendNotifier {
	if n == nil {
		return DefaultNotifier
	}
	return n
}

func encodeCursor(v any) (string, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeEdgeCursor(cursor string) (*edgeListCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	b, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var c edgeListCursor
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		return nil, ErrInvalidCursor
	}
	return &c, nil
}

func decodeFoFCursor(cursor string) (*friendsOfFriendsCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	b, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var c friendsOfFriendsCursor
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		return nil, ErrInvalidCursor
	}
	if c.SourceID == "" || c.DestinationID == "" {
		return nil, ErrInvalidCursor
	}
	return &c, nil
}

func countEdges(ctx context.Context, tx pgx.Tx, userID string, state int) (int, error) {
	var n int
	err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM user_edge WHERE source_id = $1 AND state = $2`, userID, state).Scan(&n)
	return n, err
}

func getUsername(ctx context.Context, pool *pgxpool.Pool, userID string) string {
	var username string
	_ = pool.QueryRow(ctx, `SELECT username FROM users WHERE id = $1`, userID).Scan(&username)
	return username
}

// AddFriend sends a friend invite or accepts an incoming invite.
func AddFriend(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string) error {
	return AddFriendWithOpts(ctx, pool, sourceID, destinationID, "{}", DefaultConfig(), nil)
}

// AddFriendWithOpts is AddFriend with metadata, limits, and notifier.
func AddFriendWithOpts(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID, metadata string, cfg Config, notifier FriendNotifier) error {
	if sourceID == destinationID {
		return ErrCannotAddSelf
	}
	if metadata == "" {
		metadata = "{}"
	}
	if cfg.MaxFriends <= 0 {
		cfg.MaxFriends = DefaultMaxFriends
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = DefaultMaxPending
	}
	notifier = notifierOrDefault(notifier)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Caller already blocked target — skip silently (must delete block first).
	var blockState int
	err = tx.QueryRow(ctx, `SELECT state FROM user_edge WHERE source_id = $1 AND destination_id = $2 AND state = $3`,
		sourceID, destinationID, StateBlocked).Scan(&blockState)
	if err == nil {
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	accepted, err := addFriendTx(ctx, tx, sourceID, destinationID, metadata, cfg)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tx.Commit(ctx)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	username := getUsername(ctx, pool, sourceID)
	contentBytes, _ := json.Marshal(map[string]string{"username": username})
	content := string(contentBytes)
	if accepted {
		_ = notifier.Notify(ctx, destinationID,
			fmt.Sprintf("%s accepted your friend request", username),
			content, int16(NotificationCodeFriendAccept), sourceID)
	} else {
		_ = notifier.Notify(ctx, destinationID,
			fmt.Sprintf("%s wants to add you as a friend", username),
			content, int16(NotificationCodeFriendRequest), sourceID)
	}
	return nil
}

func addFriendTx(ctx context.Context, tx pgx.Tx, userID, friendID, metadata string, cfg Config) (bool, error) {
	res, err := tx.Exec(ctx, `
UPDATE user_edge SET state = 0, update_time = now(),
	metadata = CASE
		WHEN source_id = $2 AND destination_id = $1 THEN metadata || $3::JSONB
		ELSE metadata
	END
WHERE (source_id = $1 AND destination_id = $2 AND state = 1)
OR (source_id = $2 AND destination_id = $1 AND state = 2)
`, friendID, userID, metadata)
	if err != nil {
		return false, err
	}
	if res.RowsAffected() == 2 {
		return true, nil
	}

	friendCount, err := countEdges(ctx, tx, userID, StateFriend)
	if err != nil {
		return false, err
	}
	if friendCount >= cfg.MaxFriends {
		return false, ErrFriendLimitReached
	}
	pendingCount, err := countEdges(ctx, tx, userID, StateInviteSent)
	if err != nil {
		return false, err
	}
	if pendingCount >= cfg.MaxPending {
		return false, ErrPendingLimitReached
	}

	position := time.Now().UTC().UnixNano()
	_, err = tx.Exec(ctx, `
INSERT INTO user_edge (source_id, destination_id, state, position, update_time, metadata)
SELECT source_id, destination_id, state, position, update_time, metadata
FROM (VALUES
  ($1::UUID, $2::UUID, 1, $3::BIGINT, now(), $4::JSONB),
  ($2::UUID, $1::UUID, 2, $3::BIGINT, now(), '{}'::JSONB)
) AS ue(source_id, destination_id, state, position, update_time, metadata)
WHERE
	EXISTS (SELECT id FROM users WHERE id = $2::UUID)
	AND NOT EXISTS (
		SELECT 1 FROM user_edge
		WHERE source_id = $2::UUID AND destination_id = $1::UUID AND state = 3
	)
ON CONFLICT (source_id, destination_id) DO NOTHING
`, userID, friendID, position, metadata)
	if err != nil {
		return false, err
	}

	res, err = tx.Exec(ctx, `
UPDATE users SET edge_count = edge_count + 1, update_time = now()
WHERE (id = $1::UUID OR id = $2::UUID)
AND EXISTS (
	SELECT 1 FROM user_edge
	WHERE (source_id = $1::UUID AND destination_id = $2::UUID AND position = $3::BIGINT)
	OR (source_id = $2::UUID AND destination_id = $1::UUID AND position = $3::BIGINT)
)`, userID, friendID, position)
	if err != nil {
		return false, err
	}
	if res.RowsAffected() != 2 {
		return false, pgx.ErrNoRows
	}
	return false, nil
}

// BlockUser blocks interaction with another user.
func BlockUser(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string) error {
	if sourceID == destinationID {
		return ErrCannotBlockSelf
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Exec(ctx, `UPDATE user_edge SET state = $1, update_time = now() WHERE source_id = $2 AND destination_id = $3`,
		StateBlocked, sourceID, destinationID)
	if err != nil {
		return err
	}

	position := time.Now().UTC().UnixNano()
	if res.RowsAffected() == 0 {
		res, err = tx.Exec(ctx, `
INSERT INTO user_edge (source_id, destination_id, state, position, update_time)
SELECT source_id, destination_id, state, position, update_time
FROM (VALUES ($1::UUID, $2::UUID, 3, $3::BIGINT, now()))
AS ue(source_id, destination_id, state, position, update_time)
WHERE EXISTS (SELECT id FROM users WHERE id = $2::UUID)`,
			sourceID, destinationID, position)
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return tx.Commit(ctx)
		}
		if _, err = tx.Exec(ctx, `UPDATE users SET edge_count = edge_count + 1, update_time = now() WHERE id = $1`, sourceID); err != nil {
			return err
		}
	}

	res, err = tx.Exec(ctx, `DELETE FROM user_edge WHERE source_id = $1 AND destination_id = $2 AND state <> $3`,
		destinationID, sourceID, StateBlocked)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 1 {
		if _, err = tx.Exec(ctx, `UPDATE users SET edge_count = edge_count - 1, update_time = now() WHERE id = $1 AND edge_count > 0`, destinationID); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// UnblockUser removes a block edge (same as delete when only block exists).
func UnblockUser(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string) error {
	return DeleteFriend(ctx, pool, sourceID, destinationID, nil)
}

// GetFriends returns all active mutual friends of a user.
func GetFriends(ctx context.Context, pool *pgxpool.Pool, userID string) ([]Friend, error) {
	list, _, err := ListFriends(ctx, pool, userID, StateFriend, MaxListLimit, "")
	return list, err
}

// DeleteFriend deletes friend relationships, rejects invites, or unblocks.
func DeleteFriend(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string, notifier FriendNotifier) error {
	notifier = notifierOrDefault(notifier)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	notify, err := deleteFriendTx(ctx, tx, sourceID, destinationID)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if notify {
		username := getUsername(ctx, pool, sourceID)
		contentBytes, _ := json.Marshal(map[string]string{"username": username})
		_ = notifier.Notify(ctx, destinationID,
			fmt.Sprintf("%s removed you as a friend", username),
			string(contentBytes), int16(NotificationCodeFriendRemove), sourceID)
	}
	return nil
}

func deleteFriendTx(ctx context.Context, tx pgx.Tx, userID, friendID string) (bool, error) {
	res, err := tx.Exec(ctx,
		`DELETE FROM user_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1 AND state <> $3)`,
		userID, friendID, StateBlocked)
	if err != nil {
		return false, err
	}
	rows := res.RowsAffected()
	switch rows {
	case 0:
		return false, nil
	case 1:
		_, err = tx.Exec(ctx, `UPDATE users SET edge_count = edge_count - 1, update_time = now() WHERE id = $1::UUID AND edge_count > 0`, userID)
		return false, err
	case 2:
		_, err = tx.Exec(ctx, `UPDATE users SET edge_count = edge_count - 1, update_time = now() WHERE id IN ($1, $2) AND edge_count > 0`, userID, friendID)
		return true, err
	default:
		return false, errors.New("unexpected number of edges were deleted")
	}
}

// ListFriends returns friend records filterable by state, with DB keyset pagination.
func ListFriends(ctx context.Context, pool *pgxpool.Pool, userID string, filterState int, limit int, cursor string) ([]Friend, string, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	incoming, err := decodeEdgeCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	query := `
SELECT u.id, u.username, COALESCE(u.display_name, ''), COALESCE(u.avatar_url, ''),
       u.lang_tag, COALESCE(u.location, ''), COALESCE(u.timezone, ''),
       COALESCE(u.metadata::text, '{}'), u.create_time, u.update_time, u.edge_count,
       e.state, e.update_time, COALESCE(e.metadata::text, '{}'), e.position
FROM user_edge e
JOIN users u ON e.destination_id = u.id
WHERE e.source_id = $1 AND e.state = $2`
	args := []any{userID, filterState}
	argN := 3

	if incoming != nil {
		query += fmt.Sprintf(` AND (e.state, e.position) < ($%d, $%d)`, argN, argN+1)
		args = append(args, incoming.State, incoming.Position)
		argN += 2
	}
	query += fmt.Sprintf(` ORDER BY e.state ASC, e.position DESC LIMIT $%d`, argN)
	args = append(args, limit+1)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var friends []Friend
	for rows.Next() {
		var f Friend
		if err := rows.Scan(
			&f.User.ID, &f.User.Username, &f.User.DisplayName, &f.User.AvatarURL,
			&f.User.LangTag, &f.User.Location, &f.User.Timezone,
			&f.User.Metadata, &f.User.CreateTime, &f.User.UpdateTime, &f.User.EdgeCount,
			&f.State, &f.UpdateTime, &f.Metadata, &f.Position,
		); err != nil {
			return nil, "", err
		}
		friends = append(friends, f)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(friends) > limit {
		last := friends[limit-1]
		nextCursor, err = encodeCursor(edgeListCursor{State: int64(last.State), Position: last.Position})
		if err != nil {
			return nil, "", err
		}
		friends = friends[:limit]
	}
	return friends, nextCursor, nil
}

// ListFriendsOfFriends returns friends-of-friends discovery results.
func ListFriendsOfFriends(ctx context.Context, pool *pgxpool.Pool, userID string, limit int, cursor string) ([]FriendOfFriend, string, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	incoming, err := decodeFoFCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	friendRows, err := pool.Query(ctx, `
SELECT destination_id FROM user_edge
WHERE source_id = $1 AND state = $2
ORDER BY destination_id`, userID, StateFriend)
	if err != nil {
		return nil, "", err
	}
	defer friendRows.Close()

	var friends []string
	for friendRows.Next() {
		var id string
		if err := friendRows.Scan(&id); err != nil {
			return nil, "", err
		}
		friends = append(friends, id)
	}
	friendRows.Close()

	if len(friends) == 0 {
		return []FriendOfFriend{}, "", nil
	}

	type fof struct {
		Referrer string
		UserID   string
	}
	var results []fof
	userIDs := make([]string, 0)
	outgoingCursor := ""

friendLoop:
	for _, f := range friends {
		if incoming != nil && f != incoming.SourceID {
			continue
		}
		q := `
SELECT source_id, destination_id
FROM user_edge
WHERE source_id = $1
AND destination_id != $2
AND destination_id != ALL($3::UUID[])
AND state = 0`
		params := []any{f, userID, friends, limit + 1}
		if incoming != nil {
			q += ` AND (source_id, destination_id) >= ($5, $6)`
			params = append(params, incoming.SourceID, incoming.DestinationID)
		}
		q += ` ORDER BY source_id, destination_id LIMIT $4`

		rows, err := pool.Query(ctx, q, params...)
		if err != nil {
			return nil, "", err
		}
		for rows.Next() {
			var sourceID, destID string
			if err := rows.Scan(&sourceID, &destID); err != nil {
				rows.Close()
				return nil, "", err
			}
			if len(results) >= limit {
				rows.Close()
				outgoingCursor, _ = encodeCursor(friendsOfFriendsCursor{SourceID: sourceID, DestinationID: destID})
				break friendLoop
			}
			results = append(results, fof{Referrer: sourceID, UserID: destID})
			userIDs = append(userIDs, destID)
		}
		rows.Close()
		incoming = nil
	}

	if len(userIDs) == 0 {
		return []FriendOfFriend{}, "", nil
	}

	users, err := loadUsersByIDs(ctx, pool, userIDs)
	if err != nil {
		return nil, "", err
	}

	out := make([]FriendOfFriend, 0, len(results))
	for _, r := range results {
		u, ok := users[r.UserID]
		if !ok {
			continue
		}
		out = append(out, FriendOfFriend{Referrer: r.Referrer, User: u})
	}
	return out, outgoingCursor, nil
}

func loadUsersByIDs(ctx context.Context, pool *pgxpool.Pool, ids []string) (map[string]FriendUser, error) {
	rows, err := pool.Query(ctx, `
SELECT id, username, COALESCE(display_name, ''), COALESCE(avatar_url, ''),
       lang_tag, COALESCE(location, ''), COALESCE(timezone, ''),
       COALESCE(metadata::text, '{}'), create_time, update_time, edge_count
FROM users WHERE id = ANY($1::UUID[])`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]FriendUser, len(ids))
	for rows.Next() {
		var u FriendUser
		if err := rows.Scan(&u.ID, &u.Username, &u.DisplayName, &u.AvatarURL,
			&u.LangTag, &u.Location, &u.Timezone, &u.Metadata,
			&u.CreateTime, &u.UpdateTime, &u.EdgeCount); err != nil {
			return nil, err
		}
		out[u.ID] = u
	}
	return out, rows.Err()
}

// UsersGetFriendStatus returns edge states between userID and the given friend IDs.
func UsersGetFriendStatus(ctx context.Context, pool *pgxpool.Pool, userID string, friendIDs []string) (map[string]int, error) {
	if len(friendIDs) == 0 {
		return map[string]int{}, nil
	}
	rows, err := pool.Query(ctx, `
SELECT destination_id, state FROM user_edge
WHERE source_id = $1 AND destination_id = ANY($2::UUID[])`, userID, friendIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int, len(friendIDs))
	for rows.Next() {
		var id string
		var state int
		if err := rows.Scan(&id, &state); err != nil {
			return nil, err
		}
		out[id] = state
	}
	return out, rows.Err()
}

// FriendMetadataUpdate updates edge metadata for source→destination.
func FriendMetadataUpdate(ctx context.Context, pool *pgxpool.Pool, userID, friendID string, metadata map[string]any) error {
	meta := "{}"
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		meta = string(b)
	}
	_, err := pool.Exec(ctx, `UPDATE user_edge SET metadata = $3::JSONB, update_time = now() WHERE source_id = $1 AND destination_id = $2`,
		userID, friendID, meta)
	return err
}

// ResolveUserIDs resolves usernames to IDs and merges with provided IDs.
func ResolveUserIDs(ctx context.Context, pool *pgxpool.Pool, ids, usernames []string) ([]string, error) {
	resolved := make([]string, 0, len(ids)+len(usernames))
	seen := make(map[string]struct{})
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		resolved = append(resolved, id)
	}
	for _, username := range usernames {
		var uid string
		err := pool.QueryRow(ctx, `SELECT id FROM users WHERE username = $1`, username).Scan(&uid)
		if err != nil {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		resolved = append(resolved, uid)
	}
	return resolved, nil
}

// ParseLegacyPositionCursor parses a legacy integer position cursor.
func ParseLegacyPositionCursor(cursor string) int64 {
	if cursor == "" {
		return 0
	}
	n, _ := strconv.ParseInt(cursor, 10, 64)
	return n
}
