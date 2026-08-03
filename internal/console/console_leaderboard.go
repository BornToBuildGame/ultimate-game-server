package console

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/leaderboard"

	"github.com/google/uuid"
)

func (s *Server) registerLeaderboardRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /console/api/leaderboard", s.handleConsoleListLeaderboards)
	mux.HandleFunc("GET /console/api/leaderboard/{id}", s.handleConsoleGetLeaderboard)
	mux.HandleFunc("DELETE /console/api/leaderboard/{id}", s.handleConsoleDeleteLeaderboard)
	mux.HandleFunc("GET /console/api/leaderboard/{id}/records", s.handleConsoleListRecords)
	mux.HandleFunc("DELETE /console/api/leaderboard/{id}/records/{owner_id}", s.handleConsoleDeleteRecord)
	mux.HandleFunc("POST /console/api/leaderboard/{id}/reset", s.handleConsoleResetLeaderboard)
	mux.HandleFunc("GET /console/api/leaderboard/{id}/archive", s.handleConsoleListArchive)
	mux.HandleFunc("GET /console/api/tournament/{id}/season", s.handleConsoleListSeasonStats)
}

func (s *Server) handleConsoleListLeaderboards(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	list, next, err := leaderboard.ListLeaderboards(r.Context(), s.pool, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"leaderboards": list, "next_cursor": next})
}

func (s *Server) handleConsoleGetLeaderboard(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	lb, err := leaderboard.GetLeaderboard(r.Context(), s.pool, r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(lb)
}

func (s *Server) handleConsoleDeleteLeaderboard(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	if err := leaderboard.DeleteLeaderboard(r.Context(), s.pool, id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.pushAudit(claims, "delete_leaderboard", id, "console deleted leaderboard", nil)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleConsoleListRecords(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	recs, next, err := leaderboard.GetLeaderboardRecords(r.Context(), s.pool, nil, r.PathValue("id"), limit, r.URL.Query().Get("cursor"), time.Time{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"records": recs, "next_cursor": next})
}

func (s *Server) handleConsoleDeleteRecord(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	ownerID := r.PathValue("owner_id")
	if err := leaderboard.DeleteRecord(r.Context(), s.pool, id, ownerID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.pushAudit(claims, "delete_leaderboard_record", id+"/"+ownerID, "console deleted record", nil)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleConsoleResetLeaderboard(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	archive := r.URL.Query().Get("archive") != "false"
	n, err := leaderboard.ManualReset(r.Context(), s.pool, id, archive)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.pushAudit(claims, "reset_leaderboard", id, "manual reset", map[string]any{"archived": n})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"archived": n})
}

func (s *Server) handleConsoleListArchive(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	recs, err := leaderboard.ListArchivedRecords(r.Context(), s.pool, r.PathValue("id"), r.URL.Query().Get("season_key"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"records": recs})
}

func (s *Server) handleConsoleListSeasonStats(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	stats, err := leaderboard.ListSeasonStats(r.Context(), s.pool, r.PathValue("id"), r.URL.Query().Get("season_key"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"seasons": stats})
}

func (s *Server) pushAudit(claims *ConsoleClaims, action, resource, message string, meta map[string]any) {
	if claims == nil {
		return
	}
	entry := &AuditLogEntry{
		ID: uuid.New().String(), ConsoleUserID: claims.UserID, ConsoleUsername: claims.Username,
		Email: claims.Email, Action: action, Resource: resource, Message: message,
		Metadata: meta, CreateTime: time.Now(),
	}
	select {
	case s.auditChan <- entry:
	default:
	}
}
