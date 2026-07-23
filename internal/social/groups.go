package social

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GroupState represents whether a group is open or closed.
const (
	GroupStateOpen   = 0
	GroupStateClosed = 1
)

// GroupMemberState represents the membership roles.
const (
	RoleSuperAdmin  = 0
	RoleAdmin       = 1
	RoleMember      = 2
	RoleJoinRequest = 3
	RoleBanned      = 4
)

// Group notification codes (reference-engine aligned).
const (
	NotificationCodeGroupAdd         = -4
	NotificationCodeGroupJoinRequest = -5
)

var epochDisable = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// Group represents a guild/clan record.
type Group struct {
	ID          string
	CreatorID   string
	Name        string
	Description string
	AvatarURL   string
	LangTag     string
	Metadata    string
	State       int
	EdgeCount   int
	MaxCount    int
	CreateTime  time.Time
	UpdateTime  time.Time
	DisableTime time.Time
}

// CreateGroupParams configures group creation.
type CreateGroupParams struct {
	Name        string
	Description string
	AvatarURL   string
	LangTag     string
	Metadata    string
	Open        bool
	MaxCount    int
}

// CreateGroup creates a new group and designates the creator as SuperAdmin.
// Legacy helper: open=true, max_count=100.
func CreateGroup(ctx context.Context, pool *pgxpool.Pool, creatorID, name, description, avatarURL, langTag string) (*Group, error) {
	return CreateGroupWithParams(ctx, pool, creatorID, CreateGroupParams{
		Name: name, Description: description, AvatarURL: avatarURL, LangTag: langTag,
		Open: true, MaxCount: 100, Metadata: "{}",
	})
}

// CreateGroupWithParams creates a group with full options.
func CreateGroupWithParams(ctx context.Context, pool *pgxpool.Pool, creatorID string, p CreateGroupParams) (*Group, error) {
	if p.Name == "" {
		return nil, errors.New("group name required")
	}
	if p.LangTag == "" {
		p.LangTag = "en"
	}
	if p.MaxCount <= 0 {
		p.MaxCount = 100
	}
	if p.Metadata == "" {
		p.Metadata = "{}"
	}
	if !json.Valid([]byte(p.Metadata)) {
		return nil, errors.New("invalid metadata json")
	}
	state := GroupStateClosed
	if p.Open {
		state = GroupStateOpen
	}

	groupID := uuid.New().String()
	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	groupQuery := `INSERT INTO groups (id, creator_id, name, description, avatar_url, lang_tag, metadata, state, edge_count, max_count)
	               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, 1, $9)
	               RETURNING id, creator_id, name, description, avatar_url, lang_tag, metadata::text, state, edge_count, max_count, create_time, update_time, disable_time`
	g := &Group{}
	err = tx.QueryRow(ctx, groupQuery, groupID, creatorID, p.Name, p.Description, p.AvatarURL, p.LangTag, p.Metadata, state, p.MaxCount).
		Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.Metadata, &g.State, &g.EdgeCount, &g.MaxCount, &g.CreateTime, &g.UpdateTime, &g.DisableTime)
	if err != nil {
		return nil, err
	}

	edgeQuery := `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)`
	if _, err = tx.Exec(ctx, edgeQuery, groupID, position, creatorID, RoleSuperAdmin); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, edgeQuery, creatorID, position, groupID, RoleSuperAdmin); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

// GroupsGetID returns groups by IDs (active only).
func GroupsGetID(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]*Group, error) {
	if len(ids) == 0 {
		return []*Group{}, nil
	}
	rows, err := pool.Query(ctx, `SELECT id, creator_id, name, description, avatar_url, lang_tag, metadata::text, state, edge_count, max_count, create_time, update_time, disable_time
		FROM groups WHERE id = ANY($1) AND disable_time = $2`, ids, epochDisable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Group
	for rows.Next() {
		g := &Group{}
		if err := rows.Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.Metadata, &g.State, &g.EdgeCount, &g.MaxCount, &g.CreateTime, &g.UpdateTime, &g.DisableTime); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

// JoinGroup joins an open group or queues a join request for a closed group.
func JoinGroup(ctx context.Context, pool *pgxpool.Pool, userID, groupID string, notifier FriendNotifier) error {
	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var state, edgeCount, maxCount int
	var name string
	err = tx.QueryRow(ctx, `SELECT state, edge_count, max_count, name FROM groups WHERE id = $1 AND disable_time = $2 FOR UPDATE`,
		groupID, epochDisable).Scan(&state, &edgeCount, &maxCount, &name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("group not found")
		}
		return err
	}

	var role int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&role)
	if err == nil {
		if role <= RoleMember {
			return errors.New("already member of this group")
		}
		if role == RoleBanned {
			return errors.New("banned from this group")
		}
		if role == RoleJoinRequest {
			return errors.New("join request already pending")
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	edgeQuery := `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)
	              ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`

	if state == GroupStateOpen {
		if edgeCount >= maxCount {
			return errors.New("group is full")
		}
		if _, err = tx.Exec(ctx, edgeQuery, groupID, position, userID, RoleMember); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, edgeQuery, userID, position, groupID, RoleMember); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE groups SET edge_count = edge_count + 1, update_time = now() WHERE id = $1`, groupID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	// Closed: join request (no edge_count bump)
	if _, err = tx.Exec(ctx, edgeQuery, groupID, position, userID, RoleJoinRequest); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, edgeQuery, userID, position, groupID, RoleJoinRequest); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	if notifier != nil {
		var username string
		_ = pool.QueryRow(ctx, `SELECT username FROM users WHERE id = $1`, userID).Scan(&username)
		contentBytes, _ := json.Marshal(map[string]string{"group_id": groupID, "username": username})
		rows, qerr := pool.Query(ctx, `SELECT destination_id FROM group_edge WHERE source_id = $1 AND state <= $2`, groupID, RoleAdmin)
		if qerr == nil {
			defer rows.Close()
			for rows.Next() {
				var adminID string
				if rows.Scan(&adminID) == nil {
					_ = notifier.Notify(ctx, adminID, "Group join request", string(contentBytes), int16(NotificationCodeGroupJoinRequest), userID)
				}
			}
		}
	}
	return nil
}

// GetUserRole retrieves the role of a user in a group.
func GetUserRole(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) (int, error) {
	var role int
	err := pool.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return -1, errors.New("not a member")
		}
		return -1, err
	}
	return role, nil
}

// IsGroupMember reports whether user has membership state <= RoleMember.
func IsGroupMember(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) (bool, error) {
	var role int
	err := pool.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return role <= RoleMember, nil
}

// AddGroupUsers adds users as members or accepts join requests (admin+). Empty callerID = authoritative.
func AddGroupUsers(ctx context.Context, pool *pgxpool.Pool, callerID, groupID string, userIDs []string, notifier FriendNotifier) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var edgeCount, maxCount int
	var name string
	err = tx.QueryRow(ctx, `SELECT edge_count, max_count, name FROM groups WHERE id = $1 AND disable_time = $2 FOR UPDATE`,
		groupID, epochDisable).Scan(&edgeCount, &maxCount, &name)
	if err != nil {
		return errors.New("group not found")
	}

	if callerID != "" {
		var callerRole int
		err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, callerID).Scan(&callerRole)
		if err != nil || callerRole > RoleAdmin {
			return errors.New("insufficient permissions")
		}
	}

	edgeQuery := `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)
	              ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`
	added := 0
	for _, uid := range userIDs {
		if uid == "" {
			continue
		}
		position := time.Now().UnixNano() / int64(time.Millisecond)
		var existing int
		err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, uid).Scan(&existing)
		if err == nil {
			if existing <= RoleMember {
				continue
			}
			if existing == RoleBanned {
				continue
			}
			// join request or other → promote to member
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if edgeCount+added >= maxCount {
			return errors.New("group is full")
		}
		if _, err = tx.Exec(ctx, edgeQuery, groupID, position, uid, RoleMember); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, edgeQuery, uid, position, groupID, RoleMember); err != nil {
			return err
		}
		added++
	}
	if added > 0 {
		if _, err = tx.Exec(ctx, `UPDATE groups SET edge_count = edge_count + $1, update_time = now() WHERE id = $2`, added, groupID); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if notifier != nil && added > 0 {
		contentBytes, _ := json.Marshal(map[string]string{"group_id": groupID, "name": name})
		sender := callerID
		for _, uid := range userIDs {
			_ = notifier.Notify(ctx, uid, "You've been added to a group", string(contentBytes), int16(NotificationCodeGroupAdd), sender)
		}
	}
	return nil
}

// KickMember removes a user from a group if kicker has proper authority.
// Also used to reject join requests (state 3) without changing edge_count.
func KickMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	if kickerID == userID {
		return errors.New("cannot kick yourself")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kickerRole int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member of the group")
	}
	if kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to kick")
	}

	var targetRole int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member of the group")
	}
	if kickerRole >= targetRole && targetRole <= RoleMember {
		return errors.New("cannot kick equal or higher ranking members")
	}
	// Admin can kick join requests (state 3)
	if targetRole == RoleJoinRequest && kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to kick")
	}

	_, err = tx.Exec(ctx, `DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)`, groupID, userID)
	if err != nil {
		return err
	}
	if targetRole <= RoleMember {
		if _, err = tx.Exec(ctx, `UPDATE groups SET edge_count = GREATEST(edge_count - 1, 0), update_time = now() WHERE id = $1`, groupID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// BanGroupUsers bans users: remove membership edges; insert unidirectional group→user state 4.
func BanGroupUsers(ctx context.Context, pool *pgxpool.Pool, callerID, groupID string, userIDs []string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if callerID != "" {
		var callerRole int
		err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, callerID).Scan(&callerRole)
		if err != nil || callerRole > RoleAdmin {
			return errors.New("insufficient permissions to ban")
		}
	}

	for _, uid := range userIDs {
		if uid == "" || uid == callerID {
			continue
		}
		var targetRole int
		err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, uid).Scan(&targetRole)
		wasMember := err == nil && targetRole <= RoleMember
		if err == nil && callerID != "" {
			var callerRole int
			_ = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, callerID).Scan(&callerRole)
			if targetRole <= callerRole && targetRole <= RoleMember {
				continue
			}
		}
		_, _ = tx.Exec(ctx, `DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)`, groupID, uid)
		if wasMember {
			_, _ = tx.Exec(ctx, `UPDATE groups SET edge_count = GREATEST(edge_count - 1, 0), update_time = now() WHERE id = $1`, groupID)
		}
		pos := time.Now().UnixNano() / int64(time.Millisecond)
		_, err = tx.Exec(ctx, `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)
			ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`,
			groupID, pos, uid, RoleBanned)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// UpdateGroup updates group metadata; callerID empty = authoritative.
func UpdateGroup(ctx context.Context, pool *pgxpool.Pool, callerID, id, name, description, avatarURL, langTag string, open bool, metadata string) error {
	if callerID != "" {
		role, err := GetUserRole(ctx, pool, callerID, id)
		if err != nil || role > RoleAdmin {
			return errors.New("insufficient permissions to update group")
		}
	}
	if metadata == "" {
		metadata = "{}"
	}
	if !json.Valid([]byte(metadata)) {
		return errors.New("invalid metadata json")
	}
	state := GroupStateOpen
	if !open {
		state = GroupStateClosed
	}
	_, err := pool.Exec(ctx, `UPDATE groups SET name = $1, description = $2, avatar_url = $3, lang_tag = $4, state = $5, metadata = $6::jsonb, update_time = now()
		WHERE id = $7 AND disable_time = $8`, name, description, avatarURL, langTag, state, metadata, id, epochDisable)
	return err
}

// UpdateGroupMaxCount sets max_count (runtime/console).
func UpdateGroupMaxCount(ctx context.Context, pool *pgxpool.Pool, id string, maxCount int) error {
	if maxCount < 1 {
		return errors.New("invalid max_count")
	}
	_, err := pool.Exec(ctx, `UPDATE groups SET max_count = $1, update_time = now() WHERE id = $2 AND disable_time = $3`, maxCount, id, epochDisable)
	return err
}

// DeleteGroup soft-disables a group (superadmin). Empty callerID = authoritative hard cleanup of edges + disable.
func DeleteGroup(ctx context.Context, pool *pgxpool.Pool, callerID, id string) error {
	if callerID != "" {
		role, err := GetUserRole(ctx, pool, callerID, id)
		if err != nil || role != RoleSuperAdmin {
			return errors.New("only SuperAdmin can delete group")
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `DELETE FROM group_edge WHERE source_id = $1 OR destination_id = $1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE groups SET disable_time = now(), update_time = now(), edge_count = 0 WHERE id = $1`, id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListGroups searches active groups with filters and pagination.
func ListGroups(ctx context.Context, pool *pgxpool.Pool, name, langTag string, open *bool, members int, limit int, cursor string) ([]*Group, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	query := `SELECT id, creator_id, name, description, avatar_url, lang_tag, metadata::text, state, edge_count, max_count, create_time, update_time, disable_time
		FROM groups WHERE disable_time = $1`
	args := []interface{}{epochDisable}
	argIdx := 2

	if name != "" {
		query += fmt.Sprintf(" AND name ILIKE $%d", argIdx)
		args = append(args, "%"+name+"%")
		argIdx++
	}
	if langTag != "" {
		query += fmt.Sprintf(" AND lang_tag = $%d", argIdx)
		args = append(args, langTag)
		argIdx++
	}
	if open != nil {
		stateVal := GroupStateOpen
		if !*open {
			stateVal = GroupStateClosed
		}
		query += fmt.Sprintf(" AND state = $%d", argIdx)
		args = append(args, stateVal)
		argIdx++
	}
	if members > 0 {
		query += fmt.Sprintf(" AND edge_count <= $%d", argIdx)
		args = append(args, members)
		argIdx++
	}

	query += " ORDER BY id ASC"
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var list []*Group
	for rows.Next() {
		g := &Group{}
		if err = rows.Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.Metadata, &g.State, &g.EdgeCount, &g.MaxCount, &g.CreateTime, &g.UpdateTime, &g.DisableTime); err != nil {
			return nil, "", err
		}
		list = append(list, g)
	}

	startIdx := 0
	if cursor != "" {
		for i, g := range list {
			if g.ID == cursor {
				startIdx = i + 1
				break
			}
		}
	}
	if startIdx >= len(list) {
		return []*Group{}, "", nil
	}
	endIdx := startIdx + limit
	if endIdx > len(list) {
		endIdx = len(list)
	}
	nextCursor := ""
	if endIdx < len(list) {
		nextCursor = list[endIdx-1].ID
	}
	return list[startIdx:endIdx], nextCursor, nil
}

// GroupsGetRandom returns up to count random active groups.
func GroupsGetRandom(ctx context.Context, pool *pgxpool.Pool, count int) ([]*Group, error) {
	if count <= 0 {
		count = 1
	}
	if count > 100 {
		count = 100
	}
	rows, err := pool.Query(ctx, `SELECT id, creator_id, name, description, avatar_url, lang_tag, metadata::text, state, edge_count, max_count, create_time, update_time, disable_time
		FROM groups WHERE disable_time = $1 ORDER BY random() LIMIT $2`, epochDisable, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Group
	for rows.Next() {
		g := &Group{}
		if err := rows.Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.Metadata, &g.State, &g.EdgeCount, &g.MaxCount, &g.CreateTime, &g.UpdateTime, &g.DisableTime); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

// LeaveGroup removes a member from a group.
func LeaveGroup(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var role int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&role)
	if err != nil {
		return errors.New("user is not a member of the group")
	}
	if role == RoleBanned {
		return nil
	}
	if role == RoleJoinRequest {
		_, err = tx.Exec(ctx, `DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)`, groupID, userID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if role == RoleSuperAdmin {
		var superCount int
		_ = tx.QueryRow(ctx, `SELECT COUNT(*) FROM group_edge WHERE source_id = $1 AND state = $2`, groupID, RoleSuperAdmin).Scan(&superCount)
		if superCount <= 1 {
			var otherCount int
			_ = tx.QueryRow(ctx, `SELECT COUNT(*) FROM group_edge WHERE source_id = $1 AND state <= $2 AND destination_id <> $3`, groupID, RoleMember, userID).Scan(&otherCount)
			if otherCount > 0 {
				return errors.New("cannot leave as last superadmin while others remain")
			}
			// last member: soft-delete group
			_, _ = tx.Exec(ctx, `DELETE FROM group_edge WHERE source_id = $1 OR destination_id = $1`, groupID)
			_, err = tx.Exec(ctx, `UPDATE groups SET disable_time = now(), edge_count = 0, update_time = now() WHERE id = $1`, groupID)
			return tx.Commit(ctx)
		}
	}

	_, err = tx.Exec(ctx, `DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)`, groupID, userID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE groups SET edge_count = GREATEST(edge_count - 1, 0) WHERE id = $1`, groupID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PromoteMember raises a user's role (member→admin→superadmin) one step.
func PromoteMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kickerRole, targetRole int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member")
	}
	if kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to promote")
	}
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member")
	}
	if targetRole > RoleMember || targetRole <= RoleSuperAdmin {
		return errors.New("cannot promote target")
	}
	if targetRole <= kickerRole {
		return errors.New("cannot promote equal or higher ranking members")
	}
	newRole := targetRole - 1
	_, err = tx.Exec(ctx, `UPDATE group_edge SET state = $1 WHERE (source_id = $2 AND destination_id = $3) OR (source_id = $3 AND destination_id = $2)`, newRole, groupID, userID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DemoteMember lowers a user's role one step toward member.
func DemoteMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kickerRole, targetRole int
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member")
	}
	if kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to demote")
	}
	err = tx.QueryRow(ctx, `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`, groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member")
	}
	if targetRole >= RoleMember || targetRole < RoleSuperAdmin {
		return errors.New("cannot demote target")
	}
	if kickerRole >= targetRole {
		return errors.New("cannot demote equal or higher ranking members")
	}
	if targetRole == RoleSuperAdmin {
		var superCount int
		_ = tx.QueryRow(ctx, `SELECT COUNT(*) FROM group_edge WHERE source_id = $1 AND state = $2`, groupID, RoleSuperAdmin).Scan(&superCount)
		if superCount <= 1 {
			return errors.New("cannot demote last superadmin")
		}
	}
	newRole := targetRole + 1
	_, err = tx.Exec(ctx, `UPDATE group_edge SET state = $1 WHERE (source_id = $2 AND destination_id = $3) OR (source_id = $3 AND destination_id = $2)`, newRole, groupID, userID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type GroupMember struct {
	UserID   string
	Username string
	Role     int
}

// ListGroupMembers lists users belonging to a group (members only by default; includeRequests adds state 3).
func ListGroupMembers(ctx context.Context, pool *pgxpool.Pool, groupID string, limit int, cursor string) ([]GroupMember, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	query := `SELECT u.id, u.username, e.state, e.position
	          FROM group_edge e
	          JOIN users u ON e.destination_id = u.id
	          WHERE e.source_id = $1 AND e.state <= $2
	          ORDER BY e.position DESC`

	rows, err := pool.Query(ctx, query, groupID, RoleMember)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var members []GroupMember
	var positions []int64
	for rows.Next() {
		var m GroupMember
		var pos int64
		if err := rows.Scan(&m.UserID, &m.Username, &m.Role, &pos); err != nil {
			return nil, "", err
		}
		members = append(members, m)
		positions = append(positions, pos)
	}

	startIdx := 0
	if cursor != "" {
		if parsed, err := strconv.ParseInt(cursor, 10, 64); err == nil {
			for i, pos := range positions {
				if pos < parsed {
					startIdx = i
					break
				}
			}
		}
	}
	if startIdx >= len(members) {
		return []GroupMember{}, "", nil
	}
	endIdx := startIdx + limit
	if endIdx > len(members) {
		endIdx = len(members)
	}
	nextCursor := ""
	if endIdx < len(members) {
		nextCursor = strconv.FormatInt(positions[endIdx], 10)
	}
	return members[startIdx:endIdx], nextCursor, nil
}

type UserGroupRelation struct {
	Group *Group
	Role  int
}

// ListUserGroups lists all groups a user belongs to.
func ListUserGroups(ctx context.Context, pool *pgxpool.Pool, userID string, limit int, cursor string) ([]UserGroupRelation, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	query := `SELECT g.id, g.creator_id, g.name, g.description, g.avatar_url, g.lang_tag, g.metadata::text, g.state, g.edge_count, g.max_count, g.create_time, g.update_time, g.disable_time, e.state, e.position
	          FROM group_edge e
	          JOIN groups g ON e.destination_id = g.id
	          WHERE e.source_id = $1 AND e.state <= $2 AND g.disable_time = $3
	          ORDER BY e.position DESC`

	rows, err := pool.Query(ctx, query, userID, RoleMember, epochDisable)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var rels []UserGroupRelation
	var positions []int64
	for rows.Next() {
		var r UserGroupRelation
		r.Group = &Group{}
		var pos int64
		err = rows.Scan(
			&r.Group.ID, &r.Group.CreatorID, &r.Group.Name, &r.Group.Description, &r.Group.AvatarURL, &r.Group.LangTag, &r.Group.Metadata, &r.Group.State, &r.Group.EdgeCount, &r.Group.MaxCount, &r.Group.CreateTime, &r.Group.UpdateTime, &r.Group.DisableTime,
			&r.Role, &pos,
		)
		if err != nil {
			return nil, "", err
		}
		rels = append(rels, r)
		positions = append(positions, pos)
	}

	startIdx := 0
	if cursor != "" {
		if parsed, err := strconv.ParseInt(cursor, 10, 64); err == nil {
			for i, pos := range positions {
				if pos < parsed {
					startIdx = i
					break
				}
			}
		}
	}
	if startIdx >= len(rels) {
		return []UserGroupRelation{}, "", nil
	}
	endIdx := startIdx + limit
	if endIdx > len(rels) {
		endIdx = len(rels)
	}
	nextCursor := ""
	if endIdx < len(rels) {
		nextCursor = strconv.FormatInt(positions[endIdx], 10)
	}
	return rels[startIdx:endIdx], nextCursor, nil
}
