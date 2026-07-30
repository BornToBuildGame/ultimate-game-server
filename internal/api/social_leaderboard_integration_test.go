//go:build integration && ignore

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/database"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestSocialLeaderboard_Integration(t *testing.T) {
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

	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Start API Server
	serverCfg := Config{
		HTTPAddr:        "127.0.0.1:18350",
		GRPCAddr:        "127.0.0.1:18349",
		JWTSecret:       []byte("super_secret_signing_key_at_least_32_bytes_long_1234567"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    100,
		RateLimitRefill: 10,
	}

	srv, err := NewServer(logger, serverCfg, pool)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	err = srv.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(shutdownCtx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Helper to authenticate mock users
	authenticateUser := func(username, token string) (string, string) {
		payload := []byte(fmt.Sprintf(`{"custom_id":"%s"}`, token))
		req, _ := http.NewRequest("POST", "http://127.0.0.1:18350/v2/account/authenticate/custom", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to authenticate %s: %v", username, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("authentication failed with status: %d", resp.StatusCode)
		}
		var authResp struct {
			AccessToken string `json:"access_token"`
			UserID      string `json:"user_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&authResp)
		return authResp.UserID, authResp.AccessToken
	}

	userAID, tokenA := authenticateUser("usera", "mock_token_a")
	userBID, tokenB := authenticateUser("userb", "mock_token_b")
	userCID, _ := authenticateUser("userc", "mock_token_c")

	// 3. Connect via gRPC
	conn, err := grpc.NewClient("127.0.0.1:18349", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	lbClient := apipb.NewLeaderboardServiceClient(conn)
	tmClient := apipb.NewTournamentServiceClient(conn)
	frClient := apipb.NewFriendsServiceClient(conn)
	gpClient := apipb.NewGroupServiceClient(conn)

	// Contexts with Auth Metadata
	ctxA := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+tokenA))
	ctxB := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+tokenB))

	// ==========================================
	// A. Leaderboard Test
	// ==========================================
	t.Run("Leaderboard Flow", func(t *testing.T) {
		// 1. Create Leaderboard
		_, err := lbClient.CreateLeaderboard(ctx, &apipb.CreateLeaderboardRequest{
			Id:            "test_lb",
			SortOrder:     "descending",
			Operator:      "best",
			Authoritative: false,
		})
		if err != nil {
			t.Fatalf("failed to create leaderboard: %v", err)
		}

		// 2. Write record for User A
		recA, err := lbClient.WriteLeaderboardRecord(ctxA, &apipb.WriteLeaderboardRecordRequest{
			LeaderboardId: "test_lb",
			Score:         100,
			Subscore:      10,
		})
		if err != nil {
			t.Fatalf("failed to write record A: %v", err)
		}
		if recA.Score != 100 || recA.Subscore != 10 {
			t.Errorf("unexpected score/subscore for A: %+v", recA)
		}

		// 3. Write record for User B (higher score)
		_, err = lbClient.WriteLeaderboardRecord(ctxB, &apipb.WriteLeaderboardRecordRequest{
			LeaderboardId: "test_lb",
			Score:         200,
			Subscore:      20,
		})
		if err != nil {
			t.Fatalf("failed to write record B: %v", err)
		}

		// 4. List records
		list, err := lbClient.ListLeaderboardRecords(ctx, &apipb.ListLeaderboardRecordsRequest{
			LeaderboardId: "test_lb",
			Limit:         10,
		})
		if err != nil {
			t.Fatalf("failed to list records: %v", err)
		}
		if len(list.Records) != 2 {
			t.Errorf("expected 2 records, got: %d", len(list.Records))
		}
		if list.Records[0].OwnerId != userBID || list.Records[1].OwnerId != userAID {
			t.Errorf("records not in descending order: %+v", list.Records)
		}

		// 5. Delete record for User A
		_, err = lbClient.DeleteLeaderboardRecord(ctxA, &apipb.DeleteLeaderboardRecordRequest{
			LeaderboardId: "test_lb",
		})
		if err != nil {
			t.Fatalf("failed to delete record A: %v", err)
		}

		// 6. Delete Leaderboard
		_, err = lbClient.DeleteLeaderboard(ctx, &apipb.DeleteLeaderboardRequest{
			Id: "test_lb",
		})
		if err != nil {
			t.Fatalf("failed to delete leaderboard: %v", err)
		}
	})

	// ==========================================
	// B. Tournament Test
	// ==========================================
	t.Run("Tournament Flow", func(t *testing.T) {
		// 1. Create Tournament
		_, err := tmClient.CreateTournament(ctx, &apipb.CreateTournamentRequest{
			Id:            "test_tm",
			SortOrder:     "descending",
			Operator:      "best",
			Authoritative: false,
			JoinRequired:  true,
			Duration:      3600,
		})
		if err != nil {
			t.Fatalf("failed to create tournament: %v", err)
		}

		// 2. Join Tournament
		_, err = tmClient.JoinTournament(ctxA, &apipb.JoinTournamentRequest{
			Id: "test_tm",
		})
		if err != nil {
			t.Fatalf("failed to join tournament A: %v", err)
		}

		// 3. Write tournament record
		_, err = tmClient.WriteTournamentRecord(ctxA, &apipb.WriteTournamentRecordRequest{
			TournamentId: "test_tm",
			Score:        50,
		})
		if err != nil {
			t.Fatalf("failed to write tournament record: %v", err)
		}

		// 4. List Tournaments
		tms, err := tmClient.ListTournaments(ctx, &apipb.ListTournamentsRequest{
			CategoryStart: 0,
			CategoryEnd:   10,
			Limit:         10,
		})
		if err != nil {
			t.Fatalf("failed to list tournaments: %v", err)
		}
		if len(tms.Tournaments) != 1 {
			t.Errorf("expected 1 tournament, got: %d", len(tms.Tournaments))
		}

		// 5. Delete Tournament
		_, err = tmClient.DeleteTournament(ctx, &apipb.DeleteTournamentRequest{
			Id: "test_tm",
		})
		if err != nil {
			t.Fatalf("failed to delete tournament: %v", err)
		}
	})

	// ==========================================
	// C. Friends Test
	// ==========================================
	t.Run("Friends Flow", func(t *testing.T) {
		// 1. A adds B
		_, err := frClient.AddFriends(ctxA, &apipb.AddFriendsRequest{
			Ids: []string{userBID},
		})
		if err != nil {
			t.Fatalf("failed to add friend B: %v", err)
		}

		// List friends for A - should be in invite sent state (state=1)
		flist, err := frClient.ListFriends(ctxA, &apipb.ListFriendsRequest{
			State: 1, // Sent Request
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("failed to list friends: %v", err)
		}
		if len(flist.Friends) != 1 || flist.Friends[0].User.Id != userBID {
			t.Errorf("unexpected friends list for A: %+v", flist)
		}

		// 2. B mutual adds A (promotes to StateFriend=0)
		_, err = frClient.AddFriends(ctxB, &apipb.AddFriendsRequest{
			Ids: []string{userAID},
		})
		if err != nil {
			t.Fatalf("failed B mutual add: %v", err)
		}

		// A lists friends with State=0 (friend)
		flist, err = frClient.ListFriends(ctxA, &apipb.ListFriendsRequest{
			State: 0,
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("failed to list mutual friends: %v", err)
		}
		if len(flist.Friends) != 1 || flist.Friends[0].User.Id != userBID {
			t.Errorf("expected mutual friends, got: %+v", flist)
		}

		// 3. Block user C
		_, err = frClient.BlockFriends(ctxA, &apipb.BlockFriendsRequest{
			Ids: []string{userCID},
		})
		if err != nil {
			t.Fatalf("failed to block C: %v", err)
		}

		// 4. Delete friend B
		_, err = frClient.DeleteFriends(ctxA, &apipb.DeleteFriendsRequest{
			Ids: []string{userBID},
		})
		if err != nil {
			t.Fatalf("failed to delete friend B: %v", err)
		}
	})

	// ==========================================
	// D. Groups Test
	// ==========================================
	t.Run("Groups Flow", func(t *testing.T) {
		// 1. Create Group
		g, err := gpClient.CreateGroup(ctxA, &apipb.CreateGroupRequest{
			Name:        "warriors",
			Description: "elite warriors guild",
			AvatarUrl:   "avatar.png",
			LangTag:     "en",
			Open:        true,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}
		if g.Name != "warriors" || g.CreatorId != userAID {
			t.Errorf("unexpected group properties: %+v", g)
		}

		// 2. B joins group
		_, err = gpClient.JoinGroup(ctxB, &apipb.JoinGroupRequest{
			Id: g.Id,
		})
		if err != nil {
			t.Fatalf("failed to join group B: %v", err)
		}

		// 3. List group members
		members, err := gpClient.ListGroupUsers(ctx, &apipb.ListGroupUsersRequest{
			GroupId: g.Id,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("failed to list group users: %v", err)
		}
		if len(members.GroupUsers) != 2 {
			t.Errorf("expected 2 group members, got: %d", len(members.GroupUsers))
		}

		// 4. Promote B
		_, err = gpClient.PromoteGroupUsers(ctxA, &apipb.PromoteGroupUsersRequest{
			GroupId: g.Id,
			UserIds: []string{userBID},
		})
		if err != nil {
			t.Fatalf("failed to promote B: %v", err)
		}

		// 5. Demote B
		_, err = gpClient.DemoteGroupUsers(ctxA, &apipb.DemoteGroupUsersRequest{
			GroupId: g.Id,
			UserIds: []string{userBID},
		})
		if err != nil {
			t.Fatalf("failed to demote B: %v", err)
		}

		// 6. Leave Group B
		_, err = gpClient.LeaveGroup(ctxB, &apipb.LeaveGroupRequest{
			Id: g.Id,
		})
		if err != nil {
			t.Fatalf("failed to leave group B: %v", err)
		}

		// 7. Delete Group
		_, err = gpClient.DeleteGroup(ctxA, &apipb.DeleteGroupRequest{
			Id: g.Id,
		})
		if err != nil {
			t.Fatalf("failed to delete group: %v", err)
		}
	})
}
