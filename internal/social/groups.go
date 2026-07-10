package social

import (
	"context"
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

// Group represents a guild/clan record.
type Group struct {
	ID          string
	CreatorID   string
	Name        string
	Description string
	AvatarURL   string
	LangTag     string
	State       int
	EdgeCount   int
	MaxCount    int
}

// CreateGroup creates a new group and designates the creator as SuperAdmin.
func CreateGroup(ctx context.Context, pool *pgxpool.Pool, creatorID, name, description, avatarURL, langTag string) (*Group, error) {
	groupID := uuid.New().String()
	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Insert groups record
	groupQuery := `INSERT INTO groups (id, creator_id, name, description, avatar_url, lang_tag, state, edge_count, max_count) 
	               VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 100) RETURNING id, creator_id, name, description, avatar_url, lang_tag, state, edge_count, max_count`
	
	g := &Group{}
	err = tx.QueryRow(ctx, groupQuery, groupID, creatorID, name, description, avatarURL, langTag, GroupStateOpen).
		Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.State, &g.EdgeCount, &g.MaxCount)
	if err != nil {
		return nil, err
	}

	// Insert bidirectional edges (Group -> Creator and Creator -> Group)
	edgeQuery := `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)`
	_, err = tx.Exec(ctx, edgeQuery, groupID, position, creatorID, RoleSuperAdmin)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, edgeQuery, creatorID, position, groupID, RoleSuperAdmin)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return g, nil
}

// JoinGroup joins an open group, adding membership edges and updating the count.
func JoinGroup(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) error {
	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Lock and fetch group state/size
	var state, edgeCount, maxCount int
	groupQuery := `SELECT state, edge_count, max_count FROM groups WHERE id = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, groupQuery, groupID).Scan(&state, &edgeCount, &maxCount)
	if err != nil {
		return err
	}

	if state == GroupStateClosed {
		return errors.New("group is closed")
	}
	if edgeCount >= maxCount {
		return errors.New("group is full")
	}

	// 2. Check if already member
	var role int
	checkQuery := `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`
	err = tx.QueryRow(ctx, checkQuery, groupID, userID).Scan(&role)
	if err == nil {
		if role <= RoleMember {
			return errors.New("already member of this group")
		}
		if role == RoleBanned {
			return errors.New("banned from this group")
		}
	}

	// 3. Insert membership edges
	edgeQuery := `INSERT INTO group_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)
	              ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`
	_, err = tx.Exec(ctx, edgeQuery, groupID, position, userID, RoleMember)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, edgeQuery, userID, position, groupID, RoleMember)
	if err != nil {
		return err
	}

	// 4. Update edge count
	updateQuery := `UPDATE groups SET edge_count = edge_count + 1, update_time = now() WHERE id = $1`
	_, err = tx.Exec(ctx, updateQuery, groupID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// GetUserRole retrieves the role of a user in a group.
func GetUserRole(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) (int, error) {
	var role int
	query := `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`
	err := pool.QueryRow(ctx, query, groupID, userID).Scan(&role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return -1, errors.New("not a member")
		}
		return -1, err
	}
	return role, nil
}

// KickMember removes a user from a group if kicker has proper authority.
func KickMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	if kickerID == userID {
		return errors.New("cannot kick yourself")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Fetch kicker role
	var kickerRole int
	queryRole := `SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2`
	err = tx.QueryRow(ctx, queryRole, groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member of the group")
	}

	// Kicker must be superadmin or admin
	if kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to kick")
	}

	// Fetch target role
	var targetRole int
	err = tx.QueryRow(ctx, queryRole, groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member of the group")
	}

	// Kicker role must be strictly higher than target role
	if kickerRole >= targetRole {
		return errors.New("cannot kick equal or higher ranking members")
	}

	// Delete bidirectional edges
	deleteQuery := `DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)`
	_, err = tx.Exec(ctx, deleteQuery, groupID, userID)
	if err != nil {
		return err
	}

	// Update edge count
	updateQuery := `UPDATE groups SET edge_count = edge_count - 1, update_time = now() WHERE id = $1`
	_, err = tx.Exec(ctx, updateQuery, groupID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// UpdateGroup updates group metadata and details.
func UpdateGroup(ctx context.Context, pool *pgxpool.Pool, id, name, description, avatarURL, langTag string, open bool, metadata string) error {
	state := GroupStateOpen
	if !open {
		state = GroupStateClosed
	}
	query := `UPDATE groups SET name = $1, description = $2, avatar_url = $3, lang_tag = $4, state = $5, metadata = $6, update_time = now() WHERE id = $7`
	_, err := pool.Exec(ctx, query, name, description, avatarURL, langTag, state, metadata, id)
	return err
}

// DeleteGroup deletes a group and all its edges.
func DeleteGroup(ctx context.Context, pool *pgxpool.Pool, kickerID, id string) error {
	// Kicker must be SuperAdmin
	var role int
	err := pool.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", id, kickerID).Scan(&role)
	if err != nil {
		return errors.New("kicker is not a member of the group")
	}
	if role != RoleSuperAdmin {
		return errors.New("only SuperAdmin can delete group")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DELETE FROM group_edge WHERE source_id = $1 OR destination_id = $1", id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "DELETE FROM groups WHERE id = $1", id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ListGroups searches groups with filters and pagination.
func ListGroups(ctx context.Context, pool *pgxpool.Pool, name, langTag string, open *bool, limit int, cursor string) ([]*Group, string, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	query := `SELECT id, creator_id, name, description, avatar_url, lang_tag, state, edge_count, max_count FROM groups WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

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

	query += " ORDER BY id ASC"

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var list []*Group
	for rows.Next() {
		g := &Group{}
		err = rows.Scan(&g.ID, &g.CreatorID, &g.Name, &g.Description, &g.AvatarURL, &g.LangTag, &g.State, &g.EdgeCount, &g.MaxCount)
		if err != nil {
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

// LeaveGroup removes a member from a group.
func LeaveGroup(ctx context.Context, pool *pgxpool.Pool, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Fetch role
	var role int
	err = tx.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", groupID, userID).Scan(&role)
	if err != nil {
		return errors.New("user is not a member of the group")
	}

	if role == RoleSuperAdmin {
		var nextAdmin string
		errAdmin := tx.QueryRow(ctx, "SELECT destination_id FROM group_edge WHERE source_id = $1 AND destination_id <> $2 AND state = $3 LIMIT 1", groupID, userID, RoleAdmin).Scan(&nextAdmin)
		if errAdmin != nil {
			errAdmin = tx.QueryRow(ctx, "SELECT destination_id FROM group_edge WHERE source_id = $1 AND destination_id <> $2 AND state = $3 LIMIT 1", groupID, userID, RoleMember).Scan(&nextAdmin)
		}
		if errAdmin == nil {
			_, err = tx.Exec(ctx, "UPDATE group_edge SET state = $1 WHERE (source_id = $2 AND destination_id = $3) OR (source_id = $3 AND destination_id = $2)", RoleSuperAdmin, groupID, nextAdmin)
			if err != nil {
				return err
			}
		}
	}

	_, err = tx.Exec(ctx, "DELETE FROM group_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1)", groupID, userID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "UPDATE groups SET edge_count = edge_count - 1 WHERE id = $1", groupID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// PromoteMember raises a user's role in the group.
func PromoteMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kickerRole, targetRole int
	err = tx.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member")
	}
	if kickerRole > RoleAdmin {
		return errors.New("insufficient permissions to promote")
	}

	err = tx.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member")
	}

	if targetRole <= RoleAdmin {
		return errors.New("target is already admin or superadmin")
	}

	_, err = tx.Exec(ctx, "UPDATE group_edge SET state = $1 WHERE (source_id = $2 AND destination_id = $3) OR (source_id = $3 AND destination_id = $2)", RoleAdmin, groupID, userID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// DemoteMember lowers a user's role in the group.
func DemoteMember(ctx context.Context, pool *pgxpool.Pool, kickerID, userID, groupID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kickerRole, targetRole int
	err = tx.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", groupID, kickerID).Scan(&kickerRole)
	if err != nil {
		return errors.New("kicker is not a member")
	}
	if kickerRole != RoleSuperAdmin {
		return errors.New("only SuperAdmin can demote admins")
	}

	err = tx.QueryRow(ctx, "SELECT state FROM group_edge WHERE source_id = $1 AND destination_id = $2", groupID, userID).Scan(&targetRole)
	if err != nil {
		return errors.New("target is not a member")
	}

	if targetRole != RoleAdmin {
		return errors.New("target is not an admin")
	}

	_, err = tx.Exec(ctx, "UPDATE group_edge SET state = $1 WHERE (source_id = $2 AND destination_id = $3) OR (source_id = $3 AND destination_id = $2)", RoleMember, groupID, userID)
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

// ListGroupMembers lists all users belonging to a group.
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

	query := `SELECT g.id, g.creator_id, g.name, g.description, g.avatar_url, g.lang_tag, g.state, g.edge_count, g.max_count, e.state, e.position
	          FROM group_edge e
	          JOIN groups g ON e.destination_id = g.id
	          WHERE e.source_id = $1 AND e.state <= $2
	          ORDER BY e.position DESC`

	rows, err := pool.Query(ctx, query, userID, RoleMember)
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
			&r.Group.ID, &r.Group.CreatorID, &r.Group.Name, &r.Group.Description, &r.Group.AvatarURL, &r.Group.LangTag, &r.Group.State, &r.Group.EdgeCount, &r.Group.MaxCount,
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

