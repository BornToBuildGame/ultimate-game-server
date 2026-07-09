package social

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FriendState represents the states of a social edge.
const (
	StateFriend         = 0
	StateInviteSent     = 1
	StateInviteReceived = 2
	StateBlocked        = 3
)

// Friend represents a friend record.
type Friend struct {
	UserID   string
	Username string
	State    int
}

// AddFriend sends a friend invite or accepts an incoming invite.
func AddFriend(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string) error {
	if sourceID == destinationID {
		return errors.New("cannot add yourself as friend")
	}

	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Check if an edge already exists
	var existingState int
	queryCheck := `SELECT state FROM user_edge WHERE source_id = $1 AND destination_id = $2`
	err = tx.QueryRow(ctx, queryCheck, sourceID, destinationID).Scan(&existingState)

	if err == nil {
		if existingState == StateFriend {
			return errors.New("already friends")
		}
		if existingState == StateInviteSent {
			return errors.New("friend invite already sent")
		}
		if existingState == StateBlocked {
			return errors.New("user is blocked")
		}
	}

	// Check if B already sent an invite to A
	var reverseState int
	err = tx.QueryRow(ctx, queryCheck, destinationID, sourceID).Scan(&reverseState)

	if err == nil && reverseState == StateInviteSent {
		// Mutual invite: Promote both to friends!
		updateQuery := `UPDATE user_edge SET state = $1, position = $2, update_time = now() WHERE (source_id = $3 AND destination_id = $4) OR (source_id = $4 AND destination_id = $3)`
		_, err = tx.Exec(ctx, updateQuery, StateFriend, position, sourceID, destinationID)
		if err != nil {
			return err
		}
	} else {
		// Normal flow: Insert InviteSent for A and InviteReceived for B
		insertQuery := `INSERT INTO user_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4) 
		                ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`
		_, err = tx.Exec(ctx, insertQuery, sourceID, position, destinationID, StateInviteSent)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, insertQuery, destinationID, position, sourceID, StateInviteReceived)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// BlockUser blocks interaction with another user.
func BlockUser(ctx context.Context, pool *pgxpool.Pool, sourceID, destinationID string) error {
	if sourceID == destinationID {
		return errors.New("cannot block yourself")
	}

	position := time.Now().UnixNano() / int64(time.Millisecond)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Delete reciprocal friend relations if they exist
	deleteQuery := `DELETE FROM user_edge WHERE (source_id = $1 AND destination_id = $2) OR (source_id = $2 AND destination_id = $1 AND state <> $3)`
	_, err = tx.Exec(ctx, deleteQuery, sourceID, destinationID, StateBlocked)
	if err != nil {
		return err
	}

	// 2. Insert Blocked state source_id -> destination_id
	insertBlock := `INSERT INTO user_edge (source_id, position, destination_id, state) VALUES ($1, $2, $3, $4)
	                ON CONFLICT (source_id, destination_id) DO UPDATE SET state = $4, position = $2, update_time = now()`
	_, err = tx.Exec(ctx, insertBlock, sourceID, position, destinationID, StateBlocked)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// GetFriends returns all active mutual friends of a user.
func GetFriends(ctx context.Context, pool *pgxpool.Pool, userID string) ([]Friend, error) {
	query := `SELECT u.id, u.username, e.state 
	          FROM user_edge e
	          JOIN users u ON e.destination_id = u.id
	          WHERE e.source_id = $1 AND e.state = $2
	          ORDER BY e.position DESC`

	rows, err := pool.Query(ctx, query, userID, StateFriend)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var friends []Friend
	for rows.Next() {
		var f Friend
		if err := rows.Scan(&f.UserID, &f.Username, &f.State); err != nil {
			return nil, err
		}
		friends = append(friends, f)
	}

	return friends, nil
}
