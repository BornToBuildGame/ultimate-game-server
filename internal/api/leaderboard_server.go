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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type LeaderboardServer struct {
	dbPool   *pgxpool.Pool
	rdb      *redis.Client
	tokenMgr *auth.TokenManager
}

func NewLeaderboardServer(dbPool *pgxpool.Pool, rdb *redis.Client, tokenMgr *auth.TokenManager) *LeaderboardServer {
	return &LeaderboardServer{dbPool: dbPool, rdb: rdb, tokenMgr: tokenMgr}
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

func parseSortOperator(sortOrderStr, operatorStr string) (int, int) {
	sortOrder := leaderboard.SortOrderDescending
	if strings.ToLower(sortOrderStr) == "ascending" {
		sortOrder = leaderboard.SortOrderAscending
	}
	operator := leaderboard.OperatorBest
	switch strings.ToLower(operatorStr) {
	case "set":
		operator = leaderboard.OperatorSet
	case "increment", "incr":
		operator = leaderboard.OperatorIncrement
	case "decrement", "decr":
		operator = leaderboard.OperatorDecrement
	}
	return sortOrder, operator
}



func (s *LeaderboardServer) WriteLeaderboardRecord(ctx context.Context, req *apipb.WriteLeaderboardRecordRequest) (*apipb.LeaderboardRecord, error) {
	userID, username, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	rec := req.GetRecord()
	score, subscore, metadataStr, op := int64(0), int64(0), "", apipb.Operator_NO_OVERRIDE
	if rec != nil {
		score = rec.GetScore()
		subscore = rec.GetSubscore()
		metadataStr = rec.GetMetadata()
		op = rec.GetOperator()
	}
	record, err := leaderboard.SubmitScore(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), userID, username, score, subscore, metadataStr, true, mapProtoOperator(op))
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return toProtoRecord(record), nil
}

func mapProtoOperator(op apipb.Operator) int {
	switch op {
	case apipb.Operator_BEST:
		return leaderboard.OperatorBest
	case apipb.Operator_SET:
		return leaderboard.OperatorSet
	case apipb.Operator_INCREMENT:
		return leaderboard.OperatorIncrement
	case apipb.Operator_DECREMENT:
		return leaderboard.OperatorDecrement
	default:
		return leaderboard.OperatorNoOverride
	}
}

func (s *LeaderboardServer) ListLeaderboardRecords(ctx context.Context, req *apipb.ListLeaderboardRecordsRequest) (*apipb.LeaderboardRecordList, error) {
	expiryOverride := int64(0)
	if req.GetExpiry() != nil {
		expiryOverride = req.GetExpiry().GetValue()
	}
	expiryTime := time.Time{}
	if expiryOverride != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiryOverride)
	}

	if len(req.GetOwnerIds()) > 0 {
		records, err := leaderboard.GetOwnerRecords(ctx, s.dbPool, req.GetLeaderboardId(), req.GetOwnerIds(), expiryTime)
		if err != nil {
			return nil, mapLeaderboardErr(err)
		}
		return &apipb.LeaderboardRecordList{OwnerRecords: toProtoRecords(records)}, nil
	}

	limit := int(req.GetLimit().GetValue())
	records, nextCursor, prevCursor, err := leaderboard.GetLeaderboardRecordsPaged(ctx, s.dbPool, req.GetLeaderboardId(), limit, req.GetCursor(), expiryTime, expiryOverride)
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &apipb.LeaderboardRecordList{
		Records:    toProtoRecords(records),
		NextCursor: nextCursor,
		PrevCursor: prevCursor,
	}, nil
}

func (s *LeaderboardServer) ListLeaderboardRecordsAroundOwner(ctx context.Context, req *apipb.ListLeaderboardRecordsAroundOwnerRequest) (*apipb.LeaderboardRecordList, error) {
	expiryOverride := int64(0)
	if req.GetExpiry() != nil {
		expiryOverride = req.GetExpiry().GetValue()
	}
	expiryTime := time.Time{}
	if expiryOverride != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiryOverride)
	}

	limit := 0
	if req.GetLimit() != nil {
		limit = int(req.GetLimit().GetValue())
	}
	records, err := leaderboard.GetLeaderboardRecordsAroundPlayer(ctx, s.dbPool, s.rdb, req.GetLeaderboardId(), req.GetOwnerId(), limit, expiryTime)
	if err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &apipb.LeaderboardRecordList{
		Records: toProtoRecords(records),
	}, nil
}

func (s *LeaderboardServer) DeleteLeaderboardRecord(ctx context.Context, req *apipb.DeleteLeaderboardRecordRequest) (*emptypb.Empty, error) {
	userID, _, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err := leaderboard.DeleteRecord(ctx, s.dbPool, req.GetLeaderboardId(), userID); err != nil {
		return nil, mapLeaderboardErr(err)
	}
	return &emptypb.Empty{}, nil
}

func mapLeaderboardErr(err error) error {
	switch {
	case errors.Is(err, leaderboard.ErrLeaderboardNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, leaderboard.ErrAuthoritative):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, leaderboard.ErrJoinRequired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, leaderboard.ErrRateLimited):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, leaderboard.ErrMaxAttemptsReached):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, leaderboard.ErrInvalidCursor):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, leaderboard.ErrInvalidLeaderboardID), errors.Is(err, leaderboard.ErrMetadataTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func toProtoRecord(r *leaderboard.LeaderboardRecord) *apipb.LeaderboardRecord {
	return &apipb.LeaderboardRecord{
		LeaderboardId: r.LeaderboardID,
		OwnerId:       r.OwnerID,
		Username:      wrapperspb.String(r.Username),
		Score:         r.Score,
		Subscore:      r.Subscore,
		Metadata:      r.Metadata,
		Rank:          r.Rank,
		CreateTime:    timestamppb.New(r.CreateTime),
		UpdateTime:    timestamppb.New(r.UpdateTime),
		ExpiryTime:    timestamppb.New(r.ExpiryTime),
		NumScore:      int32(r.NumScore),
		MaxNumScore:   uint32(r.MaxNumScore),
	}
}

func toProtoRecords(records []*leaderboard.LeaderboardRecord) []*apipb.LeaderboardRecord {
	out := make([]*apipb.LeaderboardRecord, len(records))
	for i, r := range records {
		out[i] = toProtoRecord(r)
	}
	return out
}

// --- REST handlers ---

type restCreateLeaderboardRequest struct {
	ID            string                 `json:"id"`
	SortOrder     string                 `json:"sort_order"`
	Operator      string                 `json:"operator"`
	ResetSchedule string                 `json:"reset_schedule"`
	Metadata      map[string]interface{} `json:"metadata"`
	Authoritative bool                   `json:"authoritative"`
	EnableRanks   *bool                  `json:"enable_ranks"`
}

func (s *Server) handleCreateLeaderboard(w http.ResponseWriter, r *http.Request) {
	var req restCreateLeaderboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sortOrder, operator := parseSortOperator(req.SortOrder, req.Operator)
	metaBytes, _ := json.Marshal(req.Metadata)
	enableRanks := true
	if req.EnableRanks != nil {
		enableRanks = *req.EnableRanks
	}
	lb := &leaderboard.Leaderboard{
		ID: req.ID, Authoritative: req.Authoritative, SortOrder: sortOrder, Operator: operator,
		ResetSchedule: req.ResetSchedule, Metadata: string(metaBytes), EnableRanks: enableRanks,
	}
	if err := leaderboard.CreateLeaderboard(r.Context(), s.dbPool, lb); err != nil {
		writeLeaderboardHTTPError(w, err)
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
	if err := leaderboard.DeleteLeaderboard(r.Context(), s.dbPool, id); err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListLeaderboards(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limitVal := 10
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	list, next, err := leaderboard.ListLeaderboards(r.Context(), s.dbPool, limitVal, q.Get("cursor"))
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"leaderboards": list, "next_cursor": next})
}

type restSubmitScoreRequest struct {
	Score             int64                  `json:"score"`
	Subscore          int64                  `json:"subscore"`
	Metadata          map[string]interface{} `json:"metadata"`
	OverrideOperator  string                 `json:"override_operator"`
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
	record, err := leaderboard.SubmitScore(r.Context(), s.dbPool, s.rdb, id, userID, username, req.Score, req.Subscore, string(metaBytes), true, parseOverrideOperator(req.OverrideOperator))
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(record)
}

func parseOverrideOperator(s string) int {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BEST":
		return leaderboard.OperatorBest
	case "SET":
		return leaderboard.OperatorSet
	case "INCREMENT", "INCR":
		return leaderboard.OperatorIncrement
	case "DECREMENT", "DECR":
		return leaderboard.OperatorDecrement
	default:
		return leaderboard.OperatorNoOverride
	}
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
	var expiryOverride int64
	if expStr := q.Get("expiry"); expStr != "" {
		expiryOverride, _ = strconv.ParseInt(expStr, 10, 64)
	}
	expiryTime := time.Time{}
	if expiryOverride != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiryOverride)
	}

	if ownerIDsStr := q.Get("owner_ids"); ownerIDsStr != "" {
		ownerIDs := strings.Split(ownerIDsStr, ",")
		records, err := leaderboard.GetOwnerRecords(r.Context(), s.dbPool, id, ownerIDs, expiryTime)
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

func (s *Server) handleGetOwnerRecord(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ownerID := r.PathValue("owner_id")
	records, err := leaderboard.GetOwnerRecords(r.Context(), s.dbPool, id, []string{ownerID}, time.Time{})
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	if len(records) == 0 {
		http.Error(w, "record not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records[0])
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

func (s *Server) handleDeleteRecord(w http.ResponseWriter, r *http.Request) {
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

func writeLeaderboardHTTPError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, leaderboard.ErrLeaderboardNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, leaderboard.ErrAuthoritative):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, leaderboard.ErrJoinRequired):
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
	case errors.Is(err, leaderboard.ErrRateLimited), errors.Is(err, leaderboard.ErrMaxAttemptsReached):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	case errors.Is(err, leaderboard.ErrInvalidLeaderboardID), errors.Is(err, leaderboard.ErrMetadataTooLarge), errors.Is(err, leaderboard.ErrInvalidCursor), errors.Is(err, leaderboard.ErrNoRecordsPossible):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleManualLeaderboardReset(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	archive := r.URL.Query().Get("archive") != "false"
	n, err := leaderboard.ManualReset(r.Context(), s.dbPool, r.PathValue("id"), archive)
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"archived": n})
}

func (s *Server) handleListLeaderboardArchive(w http.ResponseWriter, r *http.Request) {
	limitVal := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			limitVal = parsed
		}
	}
	recs, err := leaderboard.ListArchivedRecords(r.Context(), s.dbPool, r.PathValue("id"), r.URL.Query().Get("season_key"), limitVal)
	if err != nil {
		writeLeaderboardHTTPError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"records": recs})
}
