package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/matchmaker"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MatchmakerServer implements the apipb.MatchmakerServiceServer interface.
type MatchmakerServer struct {
	apipb.UnimplementedMatchmakerServiceServer
	mm       *matchmaker.Matchmaker
	tokenMgr *auth.TokenManager
}

// NewMatchmakerServer creates a new MatchmakerServer instance.
func NewMatchmakerServer(mm *matchmaker.Matchmaker, tm *auth.TokenManager) *MatchmakerServer {
	return &MatchmakerServer{
		mm:       mm,
		tokenMgr: tm,
	}
}

func (s *MatchmakerServer) authenticate(ctx context.Context) (*auth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims, nil
}

// AddMatchmaker handles ticket submissions.
func (s *MatchmakerServer) AddMatchmaker(ctx context.Context, req *apipb.AddMatchmakerRequest) (*apipb.MatchmakerTicket, error) {
	claims, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	queueName := req.GetQueueName()
	if queueName == "" {
		queueName = "default"
	}

	t := &matchmaker.Ticket{
		ID:                uuid.New().String(),
		UserID:            claims.UserID,
		Username:          claims.Username,
		SessionID:         claims.UserID,
		Region:            req.GetStringProperties()["region"],
		CreatedAt:         time.Now(),
		Query:             req.GetQuery(),
		MinCount:          int(req.GetMinCount()),
		MaxCount:          int(req.GetMaxCount()),
		StringProperties:  req.GetStringProperties(),
		NumericProperties: req.GetNumericProperties(),
		CountMultiple:     int(req.GetCountMultiple()),
		ReversePrecision:  req.GetReversePrecision(),
		QueueName:         queueName,
	}

	// Extract skill rating from numeric properties if present
	if skillVal, ok := req.GetNumericProperties()["skill"]; ok {
		t.SkillRating = int(skillVal)
	} else {
		t.SkillRating = 1000 // default fallback
	}

	err = s.mm.Submit(ctx, t)
	if err != nil {
		if err == matchmaker.ErrTooManyTickets || err == matchmaker.ErrRateLimited {
			return nil, status.Errorf(codes.ResourceExhausted, "%v", err)
		}
		if errors.Is(err, matchmaker.ErrInvalidTicket) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to submit ticket: %v", err)
	}

	return &apipb.MatchmakerTicket{
		TicketId:  t.ID,
		QueueName: t.QueueName,
		Status:    "queued",
	}, nil
}

// RemoveMatchmaker handles ticket cancellations.
func (s *MatchmakerServer) RemoveMatchmaker(ctx context.Context, req *apipb.RemoveMatchmakerRequest) (*emptypb.Empty, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	ticketID := req.GetTicketId()
	if ticketID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing ticket ID")
	}

	err = s.mm.Cancel(ctx, ticketID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cancel ticket: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// GetMatchmakerTicket retrieves status information for a ticket.
func (s *MatchmakerServer) GetMatchmakerTicket(ctx context.Context, req *apipb.GetMatchmakerTicketRequest) (*apipb.MatchmakerTicket, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	ticketID := req.GetTicketId()
	if ticketID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing ticket ID")
	}

	t, err := s.mm.GetTicket(ctx, ticketID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "ticket not found: %v", err)
	}

	return &apipb.MatchmakerTicket{
		TicketId:   t.ID,
		QueueName:  t.QueueName,
		Status:     t.Status,
		MatchId:    t.MatchID,
		MatchToken: t.MatchToken,
	}, nil
}

// GetQueueStats retrieves statistics for a queue.
func (s *MatchmakerServer) GetQueueStats(ctx context.Context, req *apipb.GetQueueStatsRequest) (*apipb.QueueStats, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	queueName := req.GetQueueName()
	if queueName == "" {
		queueName = "default"
	}

	stats := s.mm.GetStats(ctx)
	return &apipb.QueueStats{
		QueueName:      queueName,
		TicketCount:    int32(stats.TicketCount),
		AverageWaitSec: 0,
		ActiveMatches:  int32(len(stats.Completions)),
	}, nil
}

// GetMatchmakerStats returns aggregate matchmaker statistics (reference-compatible).
func (s *MatchmakerServer) GetMatchmakerStats(ctx context.Context, _ *emptypb.Empty) (*apipb.MatchmakerStats, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	stats := s.mm.GetStats(ctx)
	oldest := ""
	if !stats.OldestTicketCreateTime.IsZero() {
		oldest = stats.OldestTicketCreateTime.Format(time.RFC3339)
	}
	return &apipb.MatchmakerStats{
		TicketCount:            int32(stats.TicketCount),
		OldestTicketCreateTime: oldest,
		CompletionCount:        int32(len(stats.Completions)),
	}, nil
}
