package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/tournament"
	"ultimate-game-server/internal/api/apipb"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TournamentServer struct {
	apipb.UnimplementedTournamentServiceServer
	dbPool   *pgxpool.Pool
	rdb      *redis.Client
	tokenMgr *auth.TokenManager
}

func NewTournamentServer(dbPool *pgxpool.Pool, rdb *redis.Client, tokenMgr *auth.TokenManager) *TournamentServer {
	return &TournamentServer{
		dbPool:   dbPool,
		rdb:      rdb,
		tokenMgr: tokenMgr,
	}
}

func (s *TournamentServer) authenticate(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", "", status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims.UserID, claims.Username, nil
}

func (s *TournamentServer) CreateTournament(ctx context.Context, req *apipb.CreateTournamentRequest) (*emptypb.Empty, error) {
	sortOrder := leaderboard.SortOrderDescending
	if strings.ToLower(req.GetSortOrder()) == "ascending" {
		sortOrder = leaderboard.SortOrderAscending
	}

	operator := leaderboard.OperatorBest
	switch strings.ToLower(req.GetOperator()) {
	case "set":
		operator = leaderboard.OperatorSet
	case "increment", "incr":
		operator = leaderboard.OperatorIncrement
	case "decrement", "decr":
		operator = leaderboard.OperatorDecrement
	}

	endTime := time.Unix(0, 0).UTC()
	if req.GetEndTime() != nil {
		endTime = req.GetEndTime().AsTime()
	}

	startTime := time.Now().UTC()
	if req.GetStartTime() != nil {
		startTime = req.GetStartTime().AsTime()
	}

	lb := &leaderboard.Leaderboard{
		ID:            req.GetId(),
		Authoritative: req.GetAuthoritative(),
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: req.GetResetSchedule(),
		Metadata:      req.GetMetadata(),
		Category:      int(req.GetCategory()),
		Description:   req.GetDescription(),
		Duration:      int(req.GetDuration()),
		EndTime:       endTime,
		JoinRequired:  req.GetJoinRequired(),
		MaxSize:       int(req.GetMaxSize()),
		MaxNumScore:   int(req.GetMaxNumScore()),
		Title:         req.GetTitle(),
		StartTime:     startTime,
	}

	err := leaderboard.CreateLeaderboard(ctx, s.dbPool, lb)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create tournament: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) DeleteTournament(ctx context.Context, req *apipb.DeleteTournamentRequest) (*emptypb.Empty, error) {
	err := leaderboard.DeleteLeaderboard(ctx, s.dbPool, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete tournament: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) JoinTournament(ctx context.Context, req *apipb.JoinTournamentRequest) (*emptypb.Empty, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	err = tournament.JoinTournament(ctx, s.dbPool, req.GetId(), userID, username)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to join tournament: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) ListTournaments(ctx context.Context, req *apipb.ListTournamentsRequest) (*apipb.TournamentList, error) {
	startTime := time.Unix(0, 0).UTC()
	if req.GetStartTime() != nil {
		startTime = req.GetStartTime().AsTime()
	}

	endTime := time.Unix(0, 0).UTC()
	if req.GetEndTime() != nil {
		endTime = req.GetEndTime().AsTime()
	}

	list, nextCursor, err := tournament.ListTournaments(ctx, s.dbPool, int(req.GetCategoryStart()), int(req.GetCategoryEnd()), startTime, endTime, int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list tournaments: %v", err)
	}

	protoTournaments := make([]*apipb.Tournament, len(list))
	for i, lb := range list {
		protoTournaments[i] = toProtoTournament(lb)
	}

	return &apipb.TournamentList{
		Tournaments: protoTournaments,
		NextCursor:  nextCursor,
	}, nil
}

func (s *TournamentServer) WriteTournamentRecord(ctx context.Context, req *apipb.WriteTournamentRecordRequest) (*apipb.LeaderboardRecord, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	record, err := leaderboard.SubmitScore(ctx, s.dbPool, s.rdb, req.GetTournamentId(), userID, username, req.GetScore(), req.GetSubscore(), req.GetMetadata(), true)
	if err != nil {
		if errors.Is(err, leaderboard.ErrLeaderboardNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, leaderboard.ErrAuthoritative) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if errors.Is(err, leaderboard.ErrJoinRequired) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "failed to write tournament score: %v", err)
	}

	return toProtoRecord(record), nil
}

func (s *TournamentServer) ListTournamentRecords(ctx context.Context, req *apipb.ListTournamentRecordsRequest) (*apipb.LeaderboardRecordList, error) {
	expiryTime := time.Unix(0, 0).UTC()

	if len(req.GetOwnerIds()) > 0 {
		records, _, err := leaderboard.GetLeaderboardRecords(ctx, s.dbPool, s.rdb, req.GetTournamentId(), 1000, "", expiryTime)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list tournament records: %v", err)
		}
		var filtered []*apipb.LeaderboardRecord
		ownerSet := make(map[string]bool)
		for _, oid := range req.GetOwnerIds() {
			ownerSet[oid] = true
		}
		for _, r := range records {
			if ownerSet[r.OwnerID] {
				filtered = append(filtered, toProtoRecord(r))
			}
		}
		return &apipb.LeaderboardRecordList{Records: filtered}, nil
	}

	records, nextCursor, err := leaderboard.GetLeaderboardRecords(ctx, s.dbPool, s.rdb, req.GetTournamentId(), int(req.GetLimit()), req.GetCursor(), expiryTime)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list tournament records: %v", err)
	}

	protoRecords := make([]*apipb.LeaderboardRecord, len(records))
	for i, r := range records {
		protoRecords[i] = toProtoRecord(r)
	}

	return &apipb.LeaderboardRecordList{
		Records:    protoRecords,
		NextCursor: nextCursor,
	}, nil
}

func toProtoTournament(lb *leaderboard.Leaderboard) *apipb.Tournament {
	return &apipb.Tournament{
		Id:            lb.ID,
		SortOrder:     strconv.Itoa(lb.SortOrder),
		Operator:      strconv.Itoa(lb.Operator),
		ResetSchedule: lb.ResetSchedule,
		Metadata:      lb.Metadata,
		Authoritative: lb.Authoritative,
		Category:      int32(lb.Category),
		Description:   lb.Description,
		Duration:      int32(lb.Duration),
		EndTime:       timestamppb.New(lb.EndTime),
		JoinRequired:  lb.JoinRequired,
		MaxSize:       int32(lb.MaxSize),
		MaxNumScore:   int32(lb.MaxNumScore),
		Title:         lb.Title,
		StartTime:     timestamppb.New(lb.StartTime),
		Size:          int32(lb.Size),
	}
}

// REST Tournament Handlers on Server

type restCreateTournamentRequest struct {
	ID            string                 `json:"id"`
	SortOrder     string                 `json:"sort_order"`
	Operator      string                 `json:"operator"`
	ResetSchedule string                 `json:"reset_schedule"`
	Metadata      map[string]interface{} `json:"metadata"`
	Authoritative bool                   `json:"authoritative"`
	Category      int                    `json:"category"`
	Description   string                 `json:"description"`
	Duration      int                    `json:"duration"`
	EndTime       int64                  `json:"end_time"` // unix timestamp
	JoinRequired  bool                   `json:"join_required"`
	MaxSize       int                    `json:"max_size"`
	MaxNumScore   int                    `json:"max_num_score"`
	Title         string                 `json:"title"`
	StartTime     int64                  `json:"start_time"` // unix timestamp
}

func (s *Server) handleCreateTournament(w http.ResponseWriter, r *http.Request) {
	var req restCreateTournamentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	sortOrder := leaderboard.SortOrderDescending
	if strings.ToLower(req.SortOrder) == "ascending" {
		sortOrder = leaderboard.SortOrderAscending
	}

	operator := leaderboard.OperatorBest
	switch strings.ToLower(req.Operator) {
	case "set":
		operator = leaderboard.OperatorSet
	case "increment", "incr":
		operator = leaderboard.OperatorIncrement
	case "decrement", "decr":
		operator = leaderboard.OperatorDecrement
	}

	metaBytes, _ := json.Marshal(req.Metadata)

	endTime := time.Unix(0, 0).UTC()
	if req.EndTime > 0 {
		endTime = time.Unix(req.EndTime, 0).UTC()
	}

	startTime := time.Now().UTC()
	if req.StartTime > 0 {
		startTime = time.Unix(req.StartTime, 0).UTC()
	}

	lb := &leaderboard.Leaderboard{
		ID:            req.ID,
		Authoritative: req.Authoritative,
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: req.ResetSchedule,
		Metadata:      string(metaBytes),
		Category:      req.Category,
		Description:   req.Description,
		Duration:      req.Duration,
		EndTime:       endTime,
		JoinRequired:  req.JoinRequired,
		MaxSize:       req.MaxSize,
		MaxNumScore:   req.MaxNumScore,
		Title:         req.Title,
		StartTime:     startTime,
	}

	err := leaderboard.CreateLeaderboard(r.Context(), s.dbPool, lb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteTournament(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing tournament id", http.StatusBadRequest)
		return
	}

	err := leaderboard.DeleteLeaderboard(r.Context(), s.dbPool, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJoinTournament(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(UserIDKey).(string)
	username := r.Context().Value(UsernameKey).(string)
	id := r.PathValue("id")

	err := tournament.JoinTournament(r.Context(), s.dbPool, id, userID, username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListTournaments(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	categoryStart := 0
	if val := q.Get("category_start"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			categoryStart = parsed
		}
	}

	categoryEnd := 1000000
	if val := q.Get("category_end"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			categoryEnd = parsed
		}
	}

	var startTime time.Time
	if val := q.Get("start_time"); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			startTime = time.Unix(parsed, 0).UTC()
		}
	}

	var endTime time.Time
	if val := q.Get("end_time"); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			endTime = time.Unix(parsed, 0).UTC()
		}
	}

	limitVal := 10
	if val := q.Get("limit"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			limitVal = parsed
		}
	}

	cursor := q.Get("cursor")

	list, nextCursor, err := tournament.ListTournaments(r.Context(), s.dbPool, categoryStart, categoryEnd, startTime, endTime, limitVal, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tournaments": list,
		"next_cursor": nextCursor,
	})
}
