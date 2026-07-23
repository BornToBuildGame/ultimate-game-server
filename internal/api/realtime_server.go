package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/match"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RealtimeServer implements apipb.RealtimeServiceServer.
type RealtimeServer struct {
	apipb.UnimplementedRealtimeServiceServer
	logger      *zap.Logger
	matchRouter *match.Router
	rdb         *redis.Client
	tokenMgr    *auth.TokenManager
}

// NewRealtimeServer creates a new RealtimeServer instance.
func NewRealtimeServer(logger *zap.Logger, matchRouter *match.Router, rdb *redis.Client, tm *auth.TokenManager) *RealtimeServer {
	return &RealtimeServer{
		logger:      logger,
		matchRouter: matchRouter,
		rdb:         rdb,
		tokenMgr:    tm,
	}
}

func (s *RealtimeServer) authenticate(ctx context.Context) (*auth.Claims, error) {
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

// CreateMatch creates a new match on the server.
func (s *RealtimeServer) CreateMatch(ctx context.Context, req *apipb.CreateMatchRequest) (*apipb.Match, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	matchID := match.NewAuthoritativeMatchID()
	module := req.GetModule()
	if module == "" {
		return nil, status.Error(codes.InvalidArgument, "missing match module")
	}

	params := make(map[string]interface{})
	for k, v := range req.GetParams() {
		params[k] = v
	}

	err = s.matchRouter.CreateAndRegisterMatch(ctx, matchID, module, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create match: %v", err)
	}

	return &apipb.Match{
		MatchId:       matchID,
		Authoritative: true,
		Label:         "",
		Size:          0,
		MaxSize:       100,
	}, nil
}

// ListMatches queries active matches.
func (s *RealtimeServer) ListMatches(ctx context.Context, req *apipb.ListMatchesRequest) (*apipb.MatchList, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	var activeMatches []*apipb.Match

	if s.rdb != nil {
		matchIDs, err := s.rdb.SMembers(ctx, "match:ids").Result()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to query matches from Redis: %v", err)
		}

		for _, matchID := range matchIDs {
			meta, err := s.rdb.HGetAll(ctx, "match:metadata:"+matchID).Result()
			if err != nil || len(meta) == 0 {
				continue
			}

			authVal, _ := strconv.ParseBool(meta["authoritative"])
			sizeVal, _ := strconv.Atoi(meta["size"])
			maxSizeVal, _ := strconv.Atoi(meta["max_size"])

			if req.GetAuthoritative() && !authVal {
				continue
			}
			if req.GetLabel() != "" && !strings.Contains(meta["label"], req.GetLabel()) {
				continue
			}
			if req.GetMinSize() > 0 && int32(sizeVal) < req.GetMinSize() {
				continue
			}
			if req.GetMaxSize() > 0 && int32(sizeVal) > req.GetMaxSize() {
				continue
			}

			activeMatches = append(activeMatches, &apipb.Match{
				MatchId:       matchID,
				Authoritative: authVal,
				Label:         meta["label"],
				Size:          int32(sizeVal),
				MaxSize:       int32(maxSizeVal),
			})

			if req.GetLimit() > 0 && int32(len(activeMatches)) >= req.GetLimit() {
				break
			}
		}
	} else {
		for _, m := range s.matchRouter.GetLocalMatches() {
			if req.GetAuthoritative() && !m.Authoritative {
				continue
			}
			labelBytes, _ := json.Marshal(m.Label)
			labelStr := string(labelBytes)
			if req.GetLabel() != "" && !strings.Contains(labelStr, req.GetLabel()) {
				continue
			}
			if req.GetMinSize() > 0 && int32(m.PlayerCount) < req.GetMinSize() {
				continue
			}
			if req.GetMaxSize() > 0 && int32(m.PlayerCount) > req.GetMaxSize() {
				continue
			}

			activeMatches = append(activeMatches, &apipb.Match{
				MatchId:       m.MatchID,
				Authoritative: m.Authoritative,
				Label:         labelStr,
				Size:          int32(m.PlayerCount),
				MaxSize:       int32(m.MaxSize),
			})

			if req.GetLimit() > 0 && int32(len(activeMatches)) >= req.GetLimit() {
				break
			}
		}
	}

	return &apipb.MatchList{
		Matches: activeMatches,
	}, nil
}

// GetMatch returns detail and presence info for a match.
func (s *RealtimeServer) GetMatch(ctx context.Context, req *apipb.GetMatchRequest) (*apipb.Match, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	matchID := req.GetMatchId()
	if matchID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing match ID")
	}

	if s.rdb != nil {
		meta, err := s.rdb.HGetAll(ctx, "match:metadata:"+matchID).Result()
		if err != nil || len(meta) == 0 {
			return nil, status.Error(codes.NotFound, "match not found")
		}

		authVal, _ := strconv.ParseBool(meta["authoritative"])
		sizeVal, _ := strconv.Atoi(meta["size"])
		maxSizeVal, _ := strconv.Atoi(meta["max_size"])

		var matchPresences []*apipb.MatchPresence
		if loop, ok := s.matchRouter.GetMatchLoop(matchID); ok {
			for _, p := range loop.GetPresences() {
				matchPresences = append(matchPresences, &apipb.MatchPresence{
					UserId:    p.UserID,
					Username:  p.Username,
					SessionId: p.SessionID,
				})
			}
		}

		return &apipb.Match{
			MatchId:       matchID,
			Authoritative: authVal,
			Label:         meta["label"],
			Size:          int32(sizeVal),
			MaxSize:       int32(maxSizeVal),
			Presences:     matchPresences,
		}, nil
	} else {
		m, presences, ok := s.matchRouter.GetLocalMatch(matchID)
		if !ok {
			return nil, status.Error(codes.NotFound, "match not found")
		}

		var matchPresences []*apipb.MatchPresence
		for _, p := range presences {
			matchPresences = append(matchPresences, &apipb.MatchPresence{
				UserId:    p.UserID,
				Username:  p.Username,
				SessionId: p.SessionID,
			})
		}

		labelBytes, _ := json.Marshal(m.Label)
		return &apipb.Match{
			MatchId:       m.MatchID,
			Authoritative: m.Authoritative,
			Label:         string(labelBytes),
			Size:          int32(m.PlayerCount),
			MaxSize:       int32(m.MaxSize),
			Presences:     matchPresences,
		}, nil
	}
}

// MatchSignal forwards an admin signal command.
func (s *RealtimeServer) MatchSignal(ctx context.Context, req *apipb.MatchSignalRequest) (*apipb.MatchSignalResponse, error) {
	_, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	matchID := req.GetMatchId()
	if matchID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing match ID")
	}

	res, err := s.matchRouter.ForwardSignal(ctx, matchID, req.GetPayload())
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "signal execution failed: %v", err)
	}

	return &apipb.MatchSignalResponse{
		Response: res,
	}, nil
}

// REST Server Handler Methods

func (s *Server) handleCreateMatch(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Module string                 `json:"module"`
		Params map[string]interface{} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	matchID := match.NewAuthoritativeMatchID()
	err := s.MatchRouter.CreateAndRegisterMatch(r.Context(), matchID, req.Module, req.Params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"match_id": matchID})
}

func (s *Server) handleListMatches(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	authoritativeStr := r.URL.Query().Get("authoritative")
	label := r.URL.Query().Get("label")
	minSizeStr := r.URL.Query().Get("min_size")
	maxSizeStr := r.URL.Query().Get("max_size")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	authoritative := true
	if authoritativeStr != "" {
		if a, err := strconv.ParseBool(authoritativeStr); err == nil {
			authoritative = a
		}
	}

	minSize := 0
	if minSizeStr != "" {
		if ms, err := strconv.Atoi(minSizeStr); err == nil {
			minSize = ms
		}
	}

	maxSize := 0
	if maxSizeStr != "" {
		if ms, err := strconv.Atoi(maxSizeStr); err == nil {
			maxSize = ms
		}
	}

	var activeMatches []map[string]interface{}

	if s.rdb != nil {
		ctx := r.Context()
		matchIDs, err := s.rdb.SMembers(ctx, "match:ids").Result()
		if err == nil {
			for _, id := range matchIDs {
				meta, err := s.rdb.HGetAll(ctx, "match:metadata:"+id).Result()
				if err != nil || len(meta) == 0 {
					continue
				}

				authVal, _ := strconv.ParseBool(meta["authoritative"])
				sizeVal, _ := strconv.Atoi(meta["size"])
				maxSizeVal, _ := strconv.Atoi(meta["max_size"])

				if authoritative && !authVal {
					continue
				}
				if label != "" && !strings.Contains(meta["label"], label) {
					continue
				}
				if minSize > 0 && sizeVal < minSize {
					continue
				}
				if maxSize > 0 && sizeVal > maxSize {
					continue
				}

				labelMap := make(map[string]interface{})
				_ = json.Unmarshal([]byte(meta["label"]), &labelMap)

				activeMatches = append(activeMatches, map[string]interface{}{
					"match_id":      id,
					"authoritative": authVal,
					"label":         labelMap,
					"size":          sizeVal,
					"max_size":      maxSizeVal,
				})

				if len(activeMatches) >= limit {
					break
				}
			}
		}
	} else if s.MatchRouter != nil {
		for _, m := range s.MatchRouter.GetLocalMatches() {
			if authoritative && !m.Authoritative {
				continue
			}
			labelBytes, _ := json.Marshal(m.Label)
			if label != "" && !strings.Contains(string(labelBytes), label) {
				continue
			}
			if minSize > 0 && m.PlayerCount < minSize {
				continue
			}
			if maxSize > 0 && m.PlayerCount > maxSize {
				continue
			}

			activeMatches = append(activeMatches, map[string]interface{}{
				"match_id":      m.MatchID,
				"authoritative": m.Authoritative,
				"label":         m.Label,
				"size":          m.PlayerCount,
				"max_size":      m.MaxSize,
			})

			if len(activeMatches) >= limit {
				break
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"matches": activeMatches})
}

func (s *Server) handleGetMatch(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	matchID := r.PathValue("match_id")
	if matchID == "" {
		http.Error(w, "missing match ID", http.StatusBadRequest)
		return
	}

	if s.rdb != nil {
		ctx := r.Context()
		meta, err := s.rdb.HGetAll(ctx, "match:metadata:"+matchID).Result()
		if err != nil || len(meta) == 0 {
			http.Error(w, "match not found", http.StatusNotFound)
			return
		}

		authVal, _ := strconv.ParseBool(meta["authoritative"])
		sizeVal, _ := strconv.Atoi(meta["size"])
		maxSizeVal, _ := strconv.Atoi(meta["max_size"])

		var presences []map[string]interface{}
		if loop, ok := s.MatchRouter.GetMatchLoop(matchID); ok {
			for _, p := range loop.GetPresences() {
				presences = append(presences, map[string]interface{}{
					"user_id":    p.UserID,
					"username":   p.Username,
					"session_id": p.SessionID,
				})
			}
		}

		labelMap := make(map[string]interface{})
		_ = json.Unmarshal([]byte(meta["label"]), &labelMap)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"match_id":      matchID,
			"authoritative": authVal,
			"label":         labelMap,
			"size":          sizeVal,
			"max_size":      maxSizeVal,
			"presences":     presences,
		})
	} else if s.MatchRouter != nil {
		m, presences, ok := s.MatchRouter.GetLocalMatch(matchID)
		if !ok {
			http.Error(w, "match not found", http.StatusNotFound)
			return
		}

		var pList []map[string]interface{}
		for _, p := range presences {
			pList = append(pList, map[string]interface{}{
				"user_id":    p.UserID,
				"username":   p.Username,
				"session_id": p.SessionID,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"match_id":      m.MatchID,
			"authoritative": m.Authoritative,
			"label":         m.Label,
			"size":          m.PlayerCount,
			"max_size":      m.MaxSize,
			"presences":     pList,
		})
	} else {
		http.Error(w, "match not found", http.StatusNotFound)
	}
}

func (s *Server) handleMatchSignal(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateREST(r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	matchID := r.PathValue("match_id")
	if matchID == "" {
		http.Error(w, "missing match ID", http.StatusBadRequest)
		return
	}

	var req struct {
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	res, err := s.MatchRouter.ForwardSignal(r.Context(), matchID, req.Payload)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"response": res})
}
