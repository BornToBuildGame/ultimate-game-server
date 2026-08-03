package api

import (
	"context"
	"strings"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api/apipb"
	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/matchmaker"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SessionIDResolver func(userID string) string

type MatchmakerServer struct {
	mm             *matchmaker.Matchmaker
	tokenMgr       *auth.TokenManager
	resolveSession SessionIDResolver
}

func NewMatchmakerServer(mm *matchmaker.Matchmaker, tm *auth.TokenManager, resolver SessionIDResolver) *MatchmakerServer {
	return &MatchmakerServer{
		mm:             mm,
		tokenMgr:       tm,
		resolveSession: resolver,
	}
}

func (s *MatchmakerServer) authenticate(ctx context.Context) (*auth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	headers := md.Get("authorization")
	if len(headers) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(headers[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims, nil
}

// GetMatchmakerStats returns aggregate matchmaker statistics (reference-compatible).
func (s *MatchmakerServer) GetMatchmakerStats(ctx context.Context, _ *emptypb.Empty) (*apipb.MatchmakerStats, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	stats := s.mm.GetStats(ctx)
	var oldestTS *timestamppb.Timestamp
	if !stats.OldestTicketCreateTime.IsZero() {
		oldestTS = timestamppb.New(stats.OldestTicketCreateTime)
	}
	completions := make([]*apipb.MatchmakerCompletionStats, len(stats.Completions))
	for i, c := range stats.Completions {
		completions[i] = &apipb.MatchmakerCompletionStats{
			CreateTime:   timestamppb.New(c.CompletedAt),
			CompleteTime: timestamppb.New(c.CompletedAt),
		}
	}
	return &apipb.MatchmakerStats{
		TicketCount:            int32(stats.TicketCount),
		OldestTicketCreateTime: oldestTS,
		Completions:            completions,
	}, nil
}
