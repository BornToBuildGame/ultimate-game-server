package console

import (
	"encoding/json"
	"net/http"
	"strconv"

	"ultimate-game-server/internal/social"

	"github.com/google/uuid"
)

func (s *Server) registerFriendsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /console/api/user/{user_id}/friends", s.handleConsoleGetFriends)
	mux.HandleFunc("DELETE /console/api/user/{user_id}/friend/{friend_id}", s.handleConsoleDeleteFriend)
}

func (s *Server) handleConsoleGetFriends(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticateRequest(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	userID := r.PathValue("user_id")
	if userID == "" {
		http.Error(w, "missing user_id", http.StatusBadRequest)
		return
	}
	state := -1
	if v := r.URL.Query().Get("state"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			state = n
		}
	}
	limit := 1000
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}

	var all []social.Friend
	if state >= 0 {
		list, _, err := social.ListFriends(r.Context(), s.pool, userID, state, limit, r.URL.Query().Get("cursor"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		all = list
	} else {
		for st := 0; st <= 3; st++ {
			list, _, err := social.ListFriends(r.Context(), s.pool, userID, st, limit, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			all = append(all, list...)
		}
	}

	out := make([]map[string]interface{}, len(all))
	for i, f := range all {
		out[i] = map[string]interface{}{
			"user_id":     f.User.ID,
			"username":    f.User.Username,
			"state":       f.State,
			"update_time": f.UpdateTime,
			"metadata":    f.Metadata,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"friends": out})
}

func (s *Server) handleConsoleDeleteFriend(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	userID := r.PathValue("user_id")
	friendID := r.PathValue("friend_id")
	if userID == "" || friendID == "" {
		http.Error(w, "missing user_id or friend_id", http.StatusBadRequest)
		return
	}
	if err := social.DeleteFriend(r.Context(), s.pool, userID, friendID, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.pushAudit(claims, "delete_friend", userID, "console deleted friend relationship", map[string]any{
		"friend_id": friendID,
		"audit_id":  uuid.New().String(),
	})
	w.WriteHeader(http.StatusOK)
}
