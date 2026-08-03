package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Server) registerPlayerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /console/api/users", s.handleSearchPlayers)
	mux.HandleFunc("GET /console/api/users/{id}", s.handleGetPlayer)
	mux.HandleFunc("POST /console/api/users/{id}/unban", s.handleUnbanPlayer)
	mux.HandleFunc("POST /console/api/users/{id}/tombstone", s.handleTombstonePlayer)
	mux.HandleFunc("GET /console/api/users/{id}/notes", s.handleListNotes)
	mux.HandleFunc("POST /console/api/users/{id}/notes", s.handleAddNote)
	mux.HandleFunc("GET /console/api/audit", s.handleListAudit)
}

func (s *Server) canReadPlayers(claims *ConsoleClaims) bool {
	return hasACL(claims, "admin") || hasACL(claims, "write_players") || hasACL(claims, "read_players")
}

func (s *Server) handleSearchPlayers(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("username")
	}
	if len(q) < 3 {
		http.Error(w, "query must be at least 3 characters", http.StatusBadRequest)
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 50 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	like := "%" + q + "%"
	rows, err := s.pool.Query(ctx, `
SELECT id::text, username, COALESCE(display_name,''), COALESCE(email,''), create_time, update_time, disable_time
FROM users
WHERE username ILIKE $1 OR COALESCE(email,'') ILIKE $1 OR COALESCE(display_name,'') ILIKE $1 OR id::text = $2
ORDER BY username ASC
LIMIT $3`, like, q, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id, username, displayName, email string
		var createTime, updateTime, disableTime time.Time
		if err := rows.Scan(&id, &username, &displayName, &email, &createTime, &updateTime, &disableTime); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out = append(out, map[string]interface{}{
			"id": id, "username": username, "display_name": displayName, "email": email,
			"create_time": createTime, "update_time": updateTime, "disable_time": disableTime,
			"banned": disableTime.After(time.Unix(0, 0)),
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": out})
}

func (s *Server) handleGetPlayer(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var username, displayName string
	var email *string
	var wallet []byte
	var metadata []byte
	var createTime, updateTime, disableTime time.Time
	err = s.pool.QueryRow(r.Context(), `
SELECT username, COALESCE(display_name,''), email, wallet, metadata, create_time, update_time, disable_time
FROM users WHERE id = $1`, id).Scan(&username, &displayName, &email, &wallet, &metadata, &createTime, &updateTime, &disableTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var walletMap map[string]int64
	_ = json.Unmarshal(wallet, &walletMap)
	var metaMap map[string]interface{}
	_ = json.Unmarshal(metadata, &metaMap)
	emailStr := ""
	if email != nil {
		emailStr = *email
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id": id, "username": username, "display_name": displayName, "email": emailStr,
		"wallet": walletMap, "metadata": metaMap,
		"create_time": createTime, "update_time": updateTime, "disable_time": disableTime,
		"banned": disableTime.After(time.Unix(0, 0)),
	})
}

func (s *Server) handleUnbanPlayer(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	_, err = s.pool.Exec(r.Context(), `UPDATE users SET disable_time = '1970-01-01 00:00:00 UTC', update_time = now() WHERE id = $1`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.queueAudit(claims, "unban_player", "users/"+id, "Unbanned user "+id, map[string]any{"target_user_id": id})
	writeJSON(w, http.StatusOK, map[string]string{"status": "unbanned", "user_id": id})
}

func (s *Server) handleTombstonePlayer(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	id := r.PathValue("id")
	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := auth.SoftDeleteUser(r.Context(), s.pool, uid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if s.sessionRevoker != nil {
		s.sessionRevoker.RevokeAllSessions(id)
	}
	s.queueAudit(claims, "tombstone_player", "users/"+id, "Tombstoned user "+id, map[string]any{"target_user_id": id})
	writeJSON(w, http.StatusOK, map[string]string{"status": "tombstoned", "user_id": id})
}

func (s *Server) handleListNotes(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	rows, err := s.pool.Query(r.Context(), `
SELECT id::text, note, create_time, update_time, create_id::text, update_id::text
FROM users_notes WHERE user_id = $1 ORDER BY create_time DESC LIMIT 100`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var noteID, note string
		var createTime, updateTime time.Time
		var createID, updateID *string
		if err := rows.Scan(&noteID, &note, &createTime, &updateTime, &createID, &updateID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		item := map[string]interface{}{"id": noteID, "note": note, "create_time": createTime, "update_time": updateTime}
		if createID != nil {
			item["create_id"] = *createID
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"notes": out})
}

func (s *Server) handleAddNote(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var req struct {
		Note string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Note == "" {
		http.Error(w, "note required", http.StatusBadRequest)
		return
	}
	noteID := uuid.New().String()
	_, err = s.pool.Exec(r.Context(), `
INSERT INTO users_notes (id, user_id, note, create_id, update_id)
VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $4::uuid)`, noteID, id, req.Note, claims.UserID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.queueAudit(claims, "add_note", "users/"+id+"/notes", "Added note", map[string]any{"note_id": noteID})
	writeJSON(w, http.StatusCreated, map[string]string{"id": noteID, "note": req.Note})
}

func (s *Server) handleListAudit(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "admin") {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	action := r.URL.Query().Get("action")
	resource := r.URL.Query().Get("resource")
	var rows pgx.Rows
	switch {
	case action != "" && resource != "":
		rows, err = s.pool.Query(r.Context(), `
SELECT id::text, console_user_id::text, console_username, email, action, resource, message, metadata, create_time
FROM console_audit_log WHERE action = $1 AND resource ILIKE $2
ORDER BY create_time DESC LIMIT $3`, action, "%"+resource+"%", limit)
	case action != "":
		rows, err = s.pool.Query(r.Context(), `
SELECT id::text, console_user_id::text, console_username, email, action, resource, message, metadata, create_time
FROM console_audit_log WHERE action = $1 ORDER BY create_time DESC LIMIT $2`, action, limit)
	default:
		rows, err = s.pool.Query(r.Context(), `
SELECT id::text, console_user_id::text, console_username, email, action, resource, message, metadata, create_time
FROM console_audit_log ORDER BY create_time DESC LIMIT $1`, limit)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id, consoleUserID, username, email, act, res, message string
		var meta []byte
		var createTime time.Time
		if err := rows.Scan(&id, &consoleUserID, &username, &email, &act, &res, &message, &meta, &createTime); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var metaMap map[string]interface{}
		_ = json.Unmarshal(meta, &metaMap)
		out = append(out, map[string]interface{}{
			"id": id, "console_user_id": consoleUserID, "console_username": username, "email": email,
			"action": act, "resource": res, "message": message, "metadata": metaMap, "create_time": createTime,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": out})
}
