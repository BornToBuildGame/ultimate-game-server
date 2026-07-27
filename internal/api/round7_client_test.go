package api

import (
	"context"
	"testing"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/matchmaker"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

func TestMatchmakerServer_AddMatchmaker_UsesResolvedSessionID(t *testing.T) {
	tm, err := auth.NewTokenManager([]byte("0123456789abcdef0123456789abcdef"), time.Hour)
	require.NoError(t, err)
	token, _, err := tm.GenerateSession("user-1", "alice")
	require.NoError(t, err)

	mm := matchmaker.NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, nil)
	defer mm.Stop()

	srv := NewMatchmakerServer(mm, tm, func(userID string) string {
		require.Equal(t, "user-1", userID)
		return "ws-sess-abc"
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	ticket, err := srv.AddMatchmaker(ctx, &apipb.AddMatchmakerRequest{
		MinCount: 2, MaxCount: 2, Query: "*",
	})
	require.NoError(t, err)
	require.NotEmpty(t, ticket.GetTicketId())

	stored, err := mm.GetTicket(ctx, ticket.GetTicketId())
	require.NoError(t, err)
	require.Equal(t, "ws-sess-abc", stored.SessionID)
	require.Equal(t, "user-1", stored.UserID)
}

func TestMatchmakerServer_AddMatchmaker_EmptySessionWhenNoSocket(t *testing.T) {
	tm, err := auth.NewTokenManager([]byte("0123456789abcdef0123456789abcdef"), time.Hour)
	require.NoError(t, err)
	token, _, err := tm.GenerateSession("user-2", "bob")
	require.NoError(t, err)

	mm := matchmaker.NewMatchmaker(zap.NewNop(), nil, nil, nil, nil, nil)
	defer mm.Stop()
	srv := NewMatchmakerServer(mm, tm, func(userID string) string { return "" })

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	ticket, err := srv.AddMatchmaker(ctx, &apipb.AddMatchmakerRequest{MinCount: 2, MaxCount: 2})
	require.NoError(t, err)
	stored, err := mm.GetTicket(ctx, ticket.GetTicketId())
	require.NoError(t, err)
	// Empty resolver → matchmaker assigns a unique session id (never UserID).
	require.NotEmpty(t, stored.SessionID)
	require.NotEqual(t, stored.UserID, stored.SessionID)
}

func TestSocialAuthCreateFlagReadable(t *testing.T) {
	req := &apipb.AuthenticateAppleRequest{Token: "t", Create: false, Username: "u"}
	require.False(t, req.GetCreate())
	req2 := &apipb.AuthenticateGoogleRequest{Token: "t", Create: true, Username: "u"}
	require.True(t, req2.GetCreate())
	req3 := &apipb.AuthenticateCustomRequest{Id: "c1", Create: false}
	require.False(t, req3.GetCreate())
}
