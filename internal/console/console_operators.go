package console

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

func (s *Server) registerOperatorRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /console/users", s.handleListOperators)
	mux.HandleFunc("POST /console/users", s.handleCreateOperator)
	mux.HandleFunc("DELETE /console/users/{id}", s.handleDeleteOperator)
}

func (s *Server) handleListOperators(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "admin") {
		return
	}
	rows, err := s.pool.Query(r.Context(), `
SELECT id::text, username, email, acl, mfa_required, disable_time, create_time, update_time
FROM console_user ORDER BY create_time ASC`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id, username, email string
		var aclBytes []byte
		var mfaRequired bool
		var disableTime, createTime, updateTime interface{}
		if err := rows.Scan(&id, &username, &email, &aclBytes, &mfaRequired, &disableTime, &createTime, &updateTime); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out = append(out, map[string]interface{}{
			"id": id, "username": username, "email": email,
			"acl": parseACL(aclBytes), "mfa_required": mfaRequired,
			"disable_time": disableTime, "create_time": createTime, "update_time": updateTime,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": out})
}

func (s *Server) handleCreateOperator(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "admin") {
		return
	}
	var req struct {
		Username string                 `json:"username"`
		Email    string                 `json:"email"`
		Password string                 `json:"password"`
		ACL      map[string]interface{} `json:"acl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Username == "" || req.Email == "" || req.Password == "" {
		http.Error(w, "username, email, password required", http.StatusBadRequest)
		return
	}
	if req.ACL == nil {
		req.ACL = map[string]interface{}{"admin": false, "write_players": true, "read_players": true}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "hash failed", http.StatusInternalServerError)
		return
	}
	aclBytes, _ := json.Marshal(req.ACL)
	id := uuid.New().String()
	_, err = s.pool.Exec(r.Context(), `
INSERT INTO console_user (id, username, email, password, acl)
VALUES ($1::uuid, $2, $3, $4, $5::jsonb)`, id, req.Username, req.Email, hash, string(aclBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "create_console_user", "console_user/"+id, "Created console user "+req.Username, map[string]any{"id": id})
	writeJSON(w, http.StatusCreated, map[string]interface{}{"id": id, "username": req.Username, "email": req.Email, "acl": req.ACL})
}

func (s *Server) handleDeleteOperator(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "admin") {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if id == claims.UserID {
		http.Error(w, "cannot delete self", http.StatusBadRequest)
		return
	}
	tag, err := s.pool.Exec(r.Context(), `DELETE FROM console_user WHERE id = $1::uuid`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.queueAudit(claims, "delete_console_user", "console_user/"+id, "Deleted console user "+id, map[string]any{"id": id})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
