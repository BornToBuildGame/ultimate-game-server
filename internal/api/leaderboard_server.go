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
	"ultimate-game-server/internal/api/apipb"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LeaderboardServer struct {
	apipb.UnimplementedLeaderboardServiceServer
	dbPool   *pgxpool.Pool
	rdb      *redis.Client
	tokenMgr *auth.TokenManager
}

func NewLeaderboardServer(dbPool *pgxpool.Pool, rdb *redis.Client, tokenMgr *auth.TokenManager) *LeaderboardServer {
	return &LeaderboardServer{
		dbPool:   dbPool,
		rdb:      rdb,
		tokenMgr: tokenMgr,
	}
}

func (s *LeaderboardServer) authenticate(ctx context.Context) (string, string, error) {
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

func (s *LeaderboardServer) CreateLeaderboard(ctx context.Context, req *apipb.CreateLeaderboardRequest) (*emptypb.Empty, error) {
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

	lb := &leaderboard.Leaderboard{
		ID:            req.GetId(),
		Authoritative: req.GetAuthoritative(),
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: req.GetResetSchedule(),
		Metadata:      req.GetMetadata(),
	}

	err := leaderboard.CreateLeaderboard(ctx, s.dbPool, lb)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create leaderboard: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *LeaderboardServer) DeleteLeaderboard(ctx context.Context, req *apipb.DeleteLeaderboardRequest) (*emptypb.Empty, error) {
	err := leaderboard.DeleteLeaderboard(ctx, s.dbPool, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete leaderboard: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *LeaderboardServer) WriteLeaderboardRecord(ctx context.Context, req *apipb.WriteLeaderboardRecordRequest) (*apipb.LeaderboardRecord, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	record, err := leaderboard.SubmitScore(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), userID, username, req.GetScore(), req.GetSubscore(), req.GetMetadata(), true)
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
		return nil, status.Errorf(codes.Internal, "failed to submit score: %v", err)
	}

	return toProtoRecord(record), nil
}

func (s *LeaderboardServer) ListLeaderboardRecords(ctx context.Context, req *apipb.ListLeaderboardRecordsRequest) (*apipb.LeaderboardRecordList, error) {
	expiryTime := time.Unix(0, 0).UTC()
	
	if len(req.GetOwnerIds()) > 0 {
		records, _, err := leaderboard.GetLeaderboardRecords(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), 1000, "", expiryTime)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get records: %v", err)
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

	records, nextCursor, err := leaderboard.GetLeaderboardRecords(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), int(req.GetLimit()), req.GetCursor(), expiryTime)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get records: %v", err)
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

func (s *LeaderboardServer) ListLeaderboardRecordsAroundOwner(ctx context.Context, req *apipb.ListLeaderboardRecordsAroundOwnerRequest) (*apipb.LeaderboardRecordList, error) {
	expiryTime := time.Unix(0, 0).UTC()
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), req.GetOwnerId(), int(req.GetLimit()), expiryTime)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get records around owner: %v", err)
	}

	protoRecords := make([]*apipb.LeaderboardRecord, len(records))
	for i, r := range records {
		protoRecords[i] = toProtoRecord(r)
	}

	return &apipb.LeaderboardRecordList{
		Records: protoRecords,
	}, nil
}

func (s *LeaderboardServer) DeleteLeaderboardRecord(ctx context.Context, req *apipb.DeleteLeaderboardRecordRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	err = leaderboard.DeleteRecord(ctx, s.dbPool, req.GetLeaderboardId(), userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete record: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func toProtoRecord(r *leaderboard.LeaderboardRecord) *apipb.LeaderboardRecord {
	return &apipb.LeaderboardRecord{
		LeaderboardId: r.LeaderboardID,
		OwnerId:       r.OwnerID,
		Username:      r.Username,
		Score:         r.Score,
		Subscore:      r.Subscore,
		Metadata:      r.Metadata,
		Rank:          r.Rank,
		CreateTime:    timestamppb.New(r.CreateTime),
		UpdateTime:    timestamppb.New(r.UpdateTime),
		ExpiryTime:    timestamppb.New(r.ExpiryTime),
		NumScore:      int32(r.NumScore),
		MaxNumScore:   int32(r.MaxNumScore),
	}
}

// REST Leaderboard Handlers on Server

type restCreateLeaderboardRequest struct {
	ID            string                 `json:"id"`
	SortOrder     string                 `json:"sort_order"`
	Operator      string                 `json:"operator"`
	ResetSchedule string                 `json:"reset_schedule"`
	Metadata      map[string]interface{} `json:"metadata"`
	Authoritative bool                   `json:"authoritative"`
}

func (s *Server) handleCreateLeaderboard(w http.ResponseWriter, r *http.Request) {
	var req restCreateLeaderboardRequest
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

	lb := &leaderboard.Leaderboard{
		ID:            req.ID,
		Authoritative: req.Authoritative,
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: req.ResetSchedule,
		Metadata:      string(metaBytes),
	}

	err := leaderboard.CreateLeaderboard(r.Context(), s.dbPool, lb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteLeaderboard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing leaderboard id", http.StatusBadRequest)
		return
	}

	err := leaderboard.DeleteLeaderboard(r.Context(), s.dbPool, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type restSubmitScoreRequest struct {
	Score    int64                  `json:"score"`
	Subscore int64                  `json:"subscore"`
	Metadata map[string]interface{} `json:"metadata"`
}

func (s *Server) handleSubmitScore(w http.ResponseWriter, r *http.Request) {
	userID, username, err := s.authenticateRESTWithUsername(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")

	var req restSubmitScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	metaBytes, _ := json.Marshal(req.Metadata)

	record, err := leaderboard.SubmitScore(r.Context(), s.dbPool, nil, id, userID, username, req.Score, req.Subscore, string(metaBytes), true)
	if err != nil {
		if errors.Is(err, leaderboard.ErrLeaderboardNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if errors.Is(err, leaderboard.ErrAuthoritative) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if errors.Is(err, leaderboard.ErrJoinRequired) {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(record)
}

func (s *Server) handleListLeaderboardRecords(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()

	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}

	cursor := q.Get("cursor")
	ownerIDsStr := q.Get("owner_ids")

	expiryTime := time.Unix(0, 0).UTC()

	if ownerIDsStr != "" {
		ownerIDs := strings.Split(ownerIDsStr, ",")
		records, _, err := leaderboard.GetLeaderboardRecords(r.Context(), s.dbPool, nil, id, 1000, "", expiryTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var filtered []*leaderboard.LeaderboardRecord
		ownerSet := make(map[string]bool)
		for _, oid := range ownerIDs {
			ownerSet[oid] = true
		}
		for _, r := range records {
			if ownerSet[r.OwnerID] {
				filtered = append(filtered, r)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"records": filtered})
		return
	}

	records, nextCursor, err := leaderboard.GetLeaderboardRecords(r.Context(), s.dbPool, nil, id, limitVal, cursor, expiryTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"records":     records,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleGetOwnerRecord(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ownerID := r.PathValue("owner_id")

	expiryTime := time.Unix(0, 0).UTC()
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(r.Context(), s.dbPool, nil, id, ownerID, 0, expiryTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var match *leaderboard.LeaderboardRecord
	for _, r := range records {
		if r.OwnerID == ownerID {
			match = r
			break
		}
	}

	if match == nil {
		http.Error(w, "record not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(match)
}

func (s *Server) handleAroundPlayerLookup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ownerID := r.PathValue("owner_id")
	q := r.URL.Query()

	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}

	expiryTime := time.Unix(0, 0).UTC()
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(r.Context(), s.dbPool, nil, id, ownerID, limitVal, expiryTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"records": records,
	})
}

func (s *Server) handleDeleteRecord(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")

	err = leaderboard.DeleteRecord(r.Context(), s.dbPool, id, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
