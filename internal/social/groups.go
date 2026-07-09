package social

import (
	"context"
	"errors"
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
