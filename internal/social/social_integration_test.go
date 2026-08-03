//go:build integration

package social

import (
	"context"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestSocial_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	}()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get container DSN: %v", err)
	}

	logger := zap.NewNop()
	dbCfg := database.Config{
		DSN:             dsn,
		MaxOpenConns:    5,
		MaxRetries:      5,
		RetryDelay:      500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Run migrations
	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Insert test users
	userAID := uuid.New().String()
	userBID := uuid.New().String()

	insertUser := `INSERT INTO users (id, username, email, password, display_name) VALUES ($1, $2, $3, $4, $5)`
	_, err = pool.Exec(ctx, insertUser, userAID, "user_a", "a@test.com", []byte("hash"), "User A")
	if err != nil {
		t.Fatalf("failed to insert user A: %v", err)
	}
	_, err = pool.Exec(ctx, insertUser, userBID, "user_b", "b@test.com", []byte("hash"), "User B")
	if err != nil {
		t.Fatalf("failed to insert user B: %v", err)
	}

	// ==========================================
	// Test Friends System
	// ==========================================
	// 3. User A adds User B (Invite Sent)
	err = AddFriend(ctx, pool, userAID, userBID)
	if err != nil {
		t.Fatalf("failed to add friend: %v", err)
	}

	// Check that A's friends list is empty (since it's only invite_sent, not mutual)
	friendsA, err := GetFriends(ctx, pool, userAID)
	if err != nil {
		t.Fatalf("failed to get friends: %v", err)
	}
	if len(friendsA) != 0 {
		t.Errorf("expected 0 friends, got: %d", len(friendsA))
	}

	// 4. User B adds User A (Invite Received -> Mutual acceptance)
	err = AddFriend(ctx, pool, userBID, userAID)
	if err != nil {
		t.Fatalf("failed to accept mutual friend: %v", err)
	}

	// Check friends list again: both should see each other
	friendsA2, err := GetFriends(ctx, pool, userAID)
	if err != nil {
		t.Fatalf("failed to get friends A: %v", err)
	}
	if len(friendsA2) != 1 || friendsA2[0].User.ID != userBID {
		t.Errorf("expected User B to be in A's friends, got: %v", friendsA2)
	}

	friendsB, err := GetFriends(ctx, pool, userBID)
	if err != nil {
		t.Fatalf("failed to get friends B: %v", err)
	}
	if len(friendsB) != 1 || friendsB[0].User.ID != userAID {
		t.Errorf("expected User A to be in B's friends, got: %v", friendsB)
	}

	// 5. User A blocks User B
	err = BlockUser(ctx, pool, userAID, userBID)
	if err != nil {
		t.Fatalf("failed to block user: %v", err)
	}

	// Verify friends lists are empty again
	friendsA3, _ := GetFriends(ctx, pool, userAID)
	if len(friendsA3) != 0 {
		t.Errorf("expected A to have 0 friends post-block, got: %d", len(friendsA3))
	}
	friendsB2, _ := GetFriends(ctx, pool, userBID)
	if len(friendsB2) != 0 {
		t.Errorf("expected B to have 0 friends post-block, got: %d", len(friendsB2))
	}

	// 5b. Unblock and re-friend; verify FoF discovery
	if err := UnblockUser(ctx, pool, userAID, userBID); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	userCID := uuid.New().String()
	_, err = pool.Exec(ctx, insertUser, userCID, "user_c", "c@test.com", []byte("hash"), "User C")
	if err != nil {
		t.Fatalf("insert user C: %v", err)
	}
	_ = AddFriend(ctx, pool, userAID, userBID)
	_ = AddFriend(ctx, pool, userBID, userAID)
	_ = AddFriend(ctx, pool, userBID, userCID)
	_ = AddFriend(ctx, pool, userCID, userBID)

	fof, _, err := ListFriendsOfFriends(ctx, pool, userAID, 10, "")
	if err != nil {
		t.Fatalf("fof: %v", err)
	}
	foundC := false
	for _, f := range fof {
		if f.User.ID == userCID {
			foundC = true
		}
		if f.User.ID == userAID || f.User.ID == userBID {
			t.Errorf("FoF must exclude self and direct friends, got %s", f.User.ID)
		}
	}
	if !foundC {
		t.Errorf("expected FoF to include user C, got %+v", fof)
	}

	var edgeCount int
	_ = pool.QueryRow(ctx, `SELECT edge_count FROM users WHERE id = $1`, userAID).Scan(&edgeCount)
	if edgeCount < 1 {
		t.Errorf("expected edge_count >= 1 for user A, got %d", edgeCount)
	}

	// ==========================================
	// Test Groups System
	// ==========================================
	// 6. User A creates Group
	group, err := CreateGroup(ctx, pool, userAID, "Gamer Clan", "Cool clan", "http://icon.png", "en")
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}
	if group.EdgeCount != 1 {
		t.Errorf("expected initial edge count 1, got: %d", group.EdgeCount)
	}

	// Check creator role is SuperAdmin (0)
	roleA, err := GetUserRole(ctx, pool, userAID, group.ID)
	if err != nil {
		t.Fatalf("failed to get user role: %v", err)
	}
	if roleA != RoleSuperAdmin {
		t.Errorf("expected creator role to be SuperAdmin, got: %d", roleA)
	}

	// 7. User B joins Group
	err = JoinGroup(ctx, pool, userBID, group.ID, nil)
	if err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	roleB, err := GetUserRole(ctx, pool, userBID, group.ID)
	if err != nil {
		t.Fatalf("failed to get user role B: %v", err)
	}
	if roleB != RoleMember {
		t.Errorf("expected user role to be Member, got: %d", roleB)
	}

	// Verify group membership count updated
	var finalCount int
	err = pool.QueryRow(ctx, "SELECT edge_count FROM groups WHERE id = $1", group.ID).Scan(&finalCount)
	if err != nil {
		t.Fatalf("failed to fetch group size: %v", err)
	}
	if finalCount != 2 {
		t.Errorf("expected group size 2, got: %d", finalCount)
	}

	// 8. User B tries to Kick User A (SuperAdmin) -> should fail due to permissions
	err = KickMember(ctx, pool, userBID, userAID, group.ID)
	if err == nil {
		t.Error("expected kick by Member of SuperAdmin to fail, but it succeeded")
	}

	// 9. User A kicks User B -> should succeed
	err = KickMember(ctx, pool, userAID, userBID, group.ID)
	if err != nil {
		t.Fatalf("failed to kick member B by A: %v", err)
	}

	// Verify B is no longer a member
	_, err = GetUserRole(ctx, pool, userBID, group.ID)
	if err == nil {
		t.Error("expected user role lookup to fail after kick, but it succeeded")
	}

	// Verify group membership count decremented back to 1
	err = pool.QueryRow(ctx, "SELECT edge_count FROM groups WHERE id = $1", group.ID).Scan(&finalCount)
	if err != nil {
		t.Fatalf("failed to fetch group size: %v", err)
	}
	if finalCount != 1 {
		t.Errorf("expected group size 1 post-kick, got: %d", finalCount)
	}

	// 10. Closed join-request → AddGroupUsers accept
	closed, err := CreateGroupWithParams(ctx, pool, userAID, CreateGroupParams{
		Name: "Closed Guild", Open: false, MaxCount: 10, Metadata: "{}",
	})
	if err != nil {
		t.Fatalf("create closed: %v", err)
	}
	if err = JoinGroup(ctx, pool, userBID, closed.ID, nil); err != nil {
		t.Fatalf("closed join request: %v", err)
	}
	roleB, err = GetUserRole(ctx, pool, userBID, closed.ID)
	if err != nil || roleB != RoleJoinRequest {
		t.Fatalf("expected join request role, got %d err=%v", roleB, err)
	}
	var closedCount int
	_ = pool.QueryRow(ctx, `SELECT edge_count FROM groups WHERE id = $1`, closed.ID).Scan(&closedCount)
	if closedCount != 1 {
		t.Fatalf("join request must not bump edge_count, got %d", closedCount)
	}
	if err = AddGroupUsers(ctx, pool, userAID, closed.ID, []string{userBID}, nil); err != nil {
		t.Fatalf("accept: %v", err)
	}
	roleB, _ = GetUserRole(ctx, pool, userBID, closed.ID)
	if roleB != RoleMember {
		t.Fatalf("expected member after accept, got %d", roleB)
	}

	// 11. Ban blocks rejoin
	if err = BanGroupUsers(ctx, pool, userAID, closed.ID, []string{userBID}); err != nil {
		t.Fatalf("ban: %v", err)
	}
	if err = JoinGroup(ctx, pool, userBID, closed.ID, nil); err == nil {
		t.Fatal("banned user should not rejoin")
	}

	// 12. Soft-delete excludes from list
	if err = DeleteGroup(ctx, pool, userAID, closed.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	list, _, err := ListGroups(ctx, pool, "Closed", "", nil, 0, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range list {
		if g.ID == closed.ID {
			t.Fatal("soft-deleted group should not appear in list")
		}
	}
}
