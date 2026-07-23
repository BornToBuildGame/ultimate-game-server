package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/tournament"

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
	return &TournamentServer{dbPool: dbPool, rdb: rdb, tokenMgr: tokenMgr}
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
	sortOrder, operator := parseSortOperator(req.GetSortOrder(), req.GetOperator())
	endTime := time.Unix(0, 0).UTC()
	if req.GetEndTime() != nil {
		endTime = req.GetEndTime().AsTime()
	}
	startTime := time.Now().UTC()
	if req.GetStartTime() != nil {
		startTime = req.GetStartTime().AsTime()
	}
	lb := &leaderboard.Leaderboard{
		ID: req.GetId(), Authoritative: req.GetAuthoritative(), SortOrder: sortOrder, Operator: operator,
		ResetSchedule: req.GetResetSchedule(), Metadata: req.GetMetadata(), Category: int(req.GetCategory()),
		Description: req.GetDescription(), Duration: int(req.GetDuration()), EndTime: endTime,
		JoinRequired: req.GetJoinRequired(), MaxSize: int(req.GetMaxSize()), MaxNumScore: int(req.GetMaxNumScore()),
		Title: req.GetTitle(), StartTime: startTime, EnableRanks: true,
	}
	if err := leaderboard.CreateLeaderboard(ctx, s.dbPool, lb); err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) DeleteTournament(ctx context.Context, req *apipb.DeleteTournamentRequest) (*emptypb.Empty, error) {
	if err := leaderboard.DeleteLeaderboard(ctx, s.dbPool, req.GetId()); err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) JoinTournament(ctx context.Context, req *apipb.JoinTournamentRequest) (*emptypb.Empty, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err = tournament.JoinTournament(ctx, s.dbPool, req.GetId(), userID, username); err != nil {
		return nil, mapTournamentErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *TournamentServer) ListTournaments(ctx context.Context, req *apipb.ListTournamentsRequest) (*apipb.TournamentList, error) {
	startTime := time.Time{}
	if req.GetStartTime() != nil {
		startTime = req.GetStartTime().AsTime()
	}
	endTime := time.Time{}
	if req.GetEndTime() != nil {
		endTime = req.GetEndTime().AsTime()
	}
	list, nextCursor, err := tournament.ListTournaments(ctx, s.dbPool, int(req.GetCategoryStart()), int(req.GetCategoryEnd()), startTime, endTime, int(req.GetLimit()), req.GetCursor(), req.GetActive())
	if err != nil {
		return nil, mapTournamentErr(err)
	}
	protoTournaments := make([]*apipb.Tournament, len(list))
	for i, v := range list {
		protoTournaments[i] = toProtoTournamentView(v)
	}
	return &apipb.TournamentList{Tournaments: protoTournaments, NextCursor: nextCursor}, nil
}

func (s *TournamentServer) WriteTournamentRecord(ctx context.Context, req *apipb.WriteTournamentRecordRequest) (*apipb.LeaderboardRecord, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	record, err := leaderboard.SubmitScore(ctx, s.dbPool, s.rdb, req.GetTournamentId(), userID, username, req.GetScore(), req.GetSubscore(), req.GetMetadata(), true, mapProtoOperator(req.GetOverrideOperator()))
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return toProtoRecord(record), nil
}

func (s *TournamentServer) ListTournamentRecords(ctx context.Context, req *apipb.ListTournamentRecordsRequest) (*apipb.LeaderboardRecordList, error) {
	expiryOverride := req.GetExpiry()
	expiryTime := time.Time{}
	if expiryOverride != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiryOverride)
	}
	if len(req.GetOwnerIds()) > 0 {
		records, err := leaderboard.GetOwnerRecords(ctx, s.dbPool, req.GetTournamentId(), req.GetOwnerIds(), expiryTime)
		if err != nil {
			return nil, mapLeaderboardErr(err)
		}
		return &apipb.LeaderboardRecordList{OwnerRecords: toProtoRecords(records)}, nil
	}
	records, nextCursor, prevCursor, err := leaderboard.GetLeaderboardRecordsPaged(ctx, s.dbPool, req.GetTournamentId(), int(req.GetLimit()), req.GetCursor(), expiryTime, expiryOverride)
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &apipb.LeaderboardRecordList{Records: toProtoRecords(records), NextCursor: nextCursor, PrevCursor: prevCursor}, nil
}

func (s *TournamentServer) ListTournamentRecordsAroundOwner(ctx context.Context, req *apipb.ListTournamentRecordsAroundOwnerRequest) (*apipb.LeaderboardRecordList, error) {
	expiryTime := time.Time{}
	if req.GetExpiry() != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(req.GetExpiry())
	}
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(ctx, s.dbPool, s.rdb, req.GetTournamentId(), req.GetOwnerId(), int(req.GetLimit()), expiryTime)
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &apipb.LeaderboardRecordList{Records: toProtoRecords(records)}, nil
}

func (s *TournamentServer) DeleteTournamentRecord(ctx context.Context, req *apipb.DeleteTournamentRecordRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err := leaderboard.DeleteRecord(ctx, s.dbPool, req.GetTournamentId(), userID); err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &emptypb.Empty{}, nil
}

func mapTournamentErr(err error) error {
	switch {
	case errors.Is(err, tournament.ErrTournamentNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, tournament.ErrTournamentMaxSizeReached):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, tournament.ErrTournamentOutsideDuration), errors.Is(err, tournament.ErrTournamentEnded):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return mapLeaderboardErr(err)
	}
}

func toProtoTournamentView(v *tournament.TournamentView) *apipb.Tournament {
	lb := v.Leaderboard
	return &apipb.Tournament{
		Id: lb.ID, SortOrder: strconv.Itoa(lb.SortOrder), Operator: strconv.Itoa(lb.Operator),
		ResetSchedule: lb.ResetSchedule, Metadata: lb.Metadata, Authoritative: lb.Authoritative,
		Category: int32(lb.Category), Description: lb.Description, Duration: int32(lb.Duration),
		EndTime: timestamppb.New(lb.EndTime), JoinRequired: lb.JoinRequired, MaxSize: int32(lb.MaxSize),
		MaxNumScore: int32(lb.MaxNumScore), Title: lb.Title, StartTime: timestamppb.New(lb.StartTime),
		Size: int32(lb.Size), CanEnter: v.CanEnter, StartActive: v.StartActive, EndActive: v.EndActive,
		PrevReset: v.PrevReset, NextReset: v.NextReset,
	}
}

// --- REST ---

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
	EndTime       int64                  `json:"end_time"`
	JoinRequired  bool                   `json:"join_required"`
	MaxSize       int                    `json:"max_size"`
	MaxNumScore   int                    `json:"max_num_score"`
	Title         string                 `json:"title"`
	StartTime     int64                  `json:"start_time"`
}

func (s *Server) handleCreateTournament(w http.ResponseWriter, r *http.Request) {
	var req restCreateTournamentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sortOrder, operator := parseSortOperator(req.SortOrder, req.Operator)
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
		ID: req.ID, Authoritative: req.Authoritative, SortOrder: sortOrder, Operator: operator,
		ResetSchedule: req.ResetSchedule, Metadata: string(metaBytes), Category: req.Category,
		Description: req.Description, Duration: req.Duration, EndTime: endTime, JoinRequired: req.JoinRequired,
		MaxSize: req.MaxSize, MaxNumScore: req.MaxNumScore, Title: req.Title, StartTime: startTime, EnableRanks: true,
	}
	if err := leaderboard.CreateLeaderboard(r.Context(), s.dbPool, lb); err != nil {
		writeLeaderboardHTTPError(w, err)
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
	if err := leaderboard.DeleteLeaderboard(r.Context(), s.dbPool, id); err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJoinTournament(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := tournament.JoinTournament(r.Context(), s.dbPool, r.PathValue("id"), userID, username); err != nil {
		writeTournamentHTTPError(w, err)
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
	categoryEnd := 1<<31 - 1
	if val := q.Get("category_end"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			categoryEnd = parsed
		}
	}
	var startTime, endTime time.Time
	if val := q.Get("start_time"); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			startTime = time.Unix(parsed, 0).UTC()
		}
	}
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
	activeOnly := q.Get("active") == "true"
	list, nextCursor, err := tournament.ListTournaments(r.Context(), s.dbPool, categoryStart, categoryEnd, startTime, endTime, limitVal, q.Get("cursor"), activeOnly)
	if err != nil {
		writeTournamentHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"tournaments": list, "next_cursor": nextCursor})
}

func (s *Server) handleSubmitTournamentScore(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req restSubmitScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	metaBytes, _ := json.Marshal(req.Metadata)
	record, err := leaderboard.SubmitScore(r.Context(), s.dbPool, s.rdb, r.PathValue("id"), userID, username, req.Score, req.Subscore, string(metaBytes), true, parseOverrideOperator(req.OverrideOperator))
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(record)
}

func (s *Server) handleListTournamentRecords(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()
	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	var expiryOverride int64
	if expStr := q.Get("expiry"); expStr != "" {
		expiryOverride, _ = strconv.ParseInt(expStr, 10, 64)
	}
	expiryTime := time.Time{}
	if expiryOverride != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiryOverride)
	}
	if ownerIDsStr := q.Get("owner_ids"); ownerIDsStr != "" {
		records, err := leaderboard.GetOwnerRecords(r.Context(), s.dbPool, id, strings.Split(ownerIDsStr, ","), expiryTime)
		if err != nil {
			writeLeaderboardHTTPError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"owner_records": records})
		return
	}
	records, nextCursor, prevCursor, err := leaderboard.GetLeaderboardRecordsPaged(r.Context(), s.dbPool, id, limitVal, q.Get("cursor"), expiryTime, expiryOverride)
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"records": records, "next_cursor": nextCursor, "prev_cursor": prevCursor,
	})
}

func (s *Server) handleTournamentAroundPlayer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ownerID := r.PathValue("owner_id")
	q := r.URL.Query()
	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	var expiryTime time.Time
	if expStr := q.Get("expiry"); expStr != "" {
		if parsed, err := strconv.ParseInt(expStr, 10, 64); err == nil {
			expiryTime = leaderboard.ResolveExpiryTime(parsed)
		}
	}
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(r.Context(), s.dbPool, s.rdb, id, ownerID, limitVal, expiryTime)
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"records": records})
}

func (s *Server) handleDeleteTournamentRecord(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := leaderboard.DeleteRecord(r.Context(), s.dbPool, r.PathValue("id"), userID); err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeTournamentHTTPError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, tournament.ErrTournamentNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, tournament.ErrTournamentMaxSizeReached):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, tournament.ErrTournamentOutsideDuration), errors.Is(err, tournament.ErrTournamentEnded):
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
	default:
		writeLeaderboardHTTPError(w, err)
	}
}

func (s *Server) handleListTournamentSeasons(w http.ResponseWriter, r *http.Request) {
	limitVal := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	stats, err := leaderboard.ListSeasonStats(r.Context(), s.dbPool, r.PathValue("id"), r.URL.Query().Get("season_key"), limitVal)
	if err != nil {
		writeTournamentHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"seasons": stats})
}
