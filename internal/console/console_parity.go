package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/economy"
	"github.com/BornToBuildGame/ultimate-game-server/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Server) registerParityRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /console/api/users/{id}/wallet/ledger", s.handleWalletLedger)
	mux.HandleFunc("POST /console/api/users/{id}/wallet", s.handleWalletUpdate)
	mux.HandleFunc("GET /console/api/storage", s.handleStorageList)
	mux.HandleFunc("GET /console/api/storage/{collection}/{user_id}/{key}", s.handleStorageGet)
	mux.HandleFunc("PUT /console/api/storage", s.handleStorageWrite)
	mux.HandleFunc("DELETE /console/api/storage/{collection}/{user_id}/{key}", s.handleStorageDelete)
	mux.HandleFunc("GET /console/api/users/{id}/purchases", s.handleListPurchases)
	mux.HandleFunc("GET /console/api/users/{id}/subscriptions", s.handleListUserSubscriptions)
	mux.HandleFunc("GET /console/api/matches", s.handleListMatches)
	mux.HandleFunc("GET /console/api/matches/{id}", s.handleGetMatch)
	mux.HandleFunc("GET /console/api/status", s.handleStatus)
	mux.HandleFunc("POST /console/api/rpc/{id}", s.handleCallRPC)
}

func (s *Server) handleWalletLedger(w http.ResponseWriter, r *http.Request) {
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
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	list, err := economy.ListWalletLedger(r.Context(), s.pool, id, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		if errors.Is(err, economy.ErrLedgerCursorInvalid) {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleWalletUpdate(w http.ResponseWriter, r *http.Request) {
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
		Changeset    map[string]int64 `json:"changeset"`
		Metadata     map[string]interface{} `json:"metadata"`
		UpdateLedger *bool            `json:"update_ledger"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Changeset) == 0 {
		http.Error(w, "changeset required", http.StatusBadRequest)
		return
	}
	updateLedger := true
	if req.UpdateLedger != nil {
		updateLedger = *req.UpdateLedger
	}
	if req.Metadata == nil {
		req.Metadata = map[string]interface{}{}
	}
	req.Metadata["console_user"] = claims.Username
	results, err := economy.UpdateWallets(r.Context(), s.pool, []economy.WalletUpdate{{
		UserID: id, Changeset: req.Changeset, Metadata: req.Metadata,
	}}, updateLedger)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "wallet_update", "users/"+id+"/wallet", "Updated wallet", map[string]any{"changeset": req.Changeset})
	writeJSON(w, http.StatusOK, map[string]interface{}{"results": results})
}

func (s *Server) handleStorageList(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	collection := r.URL.Query().Get("collection")
	if collection == "" {
		http.Error(w, "collection required", http.StatusBadRequest)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	var owner *uuid.UUID
	if oid := r.URL.Query().Get("user_id"); oid != "" {
		u, err := uuid.Parse(oid)
		if err != nil {
			http.Error(w, "invalid user_id", http.StatusBadRequest)
			return
		}
		owner = &u
	}
	// Authoritative console read: use nil UUID caller with authoritative list via empty caller + owner filter.
	caller := uuid.Nil
	list, err := storage.ListStorageObjects(r.Context(), s.pool, caller, owner, collection, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleStorageGet(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	objs, err := storage.ReadStorageObjects(r.Context(), s.pool, uuid.Nil, []storage.ReadRequest{{
		Collection: r.PathValue("collection"),
		Key:        r.PathValue("key"),
		UserID:     r.PathValue("user_id"),
	}})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(objs) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, objs[0])
}

func (s *Server) handleStorageWrite(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	var req struct {
		Collection string `json:"collection"`
		Key        string `json:"key"`
		UserID     string `json:"user_id"`
		Value      string `json:"value"`
		Version    string `json:"version"`
		Read       int16  `json:"permission_read"`
		Write      int16  `json:"permission_write"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	acks, err := storage.WriteStorageObjects(r.Context(), s.pool, true, []*storage.StorageObject{{
		Collection: req.Collection, Key: req.Key, UserID: req.UserID,
		Value: req.Value, Version: req.Version, Read: req.Read, Write: req.Write,
	}})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "storage_write", "storage/"+req.Collection+"/"+req.UserID+"/"+req.Key, "Wrote storage object", nil)
	writeJSON(w, http.StatusOK, map[string]interface{}{"acks": acks})
}

func (s *Server) handleStorageDelete(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "write_players") {
		return
	}
	collection := r.PathValue("collection")
	userID := r.PathValue("user_id")
	key := r.PathValue("key")
	if err := storage.DeleteStorageObjects(r.Context(), s.pool, true, []storage.DeleteRequest{{
		Collection: collection, Key: key, UserID: userID,
	}}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "storage_delete", "storage/"+collection+"/"+userID+"/"+key, "Deleted storage object", nil)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleListPurchases(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	list, err := economy.ListPurchases(r.Context(), s.pool, r.PathValue("id"), 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"validated_purchases": list})
}

func (s *Server) handleListUserSubscriptions(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.canReadPlayers(claims) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	list, err := economy.ListSubscriptions(r.Context(), s.pool, r.PathValue("id"), 100, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleListMatches(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasACL(claims, "admin") && !hasACL(claims, "read_players") {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if s.matchLister == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"matches": []interface{}{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"matches": s.matchLister.ListMatches()})
}

func (s *Server) handleGetMatch(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasACL(claims, "admin") && !hasACL(claims, "read_players") {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if s.matchStater == nil {
		http.Error(w, "match state unavailable", http.StatusServiceUnavailable)
		return
	}
	state, err := s.matchStater.GetMatchState(r.PathValue("id"))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || err.Error() == "not found" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !hasACL(claims, "admin") && !hasACL(claims, "read_players") {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	out := map[string]interface{}{
		"started_at":     s.startTime,
		"uptime_seconds": int64(time.Since(s.startTime).Seconds()),
	}
	if s.statusProvider != nil {
		for k, v := range s.statusProvider.ConsoleStatus() {
			out[k] = v
		}
	} else if s.matchLister != nil {
		out["match_count"] = len(s.matchLister.ListMatches())
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCallRPC(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.requireACL(w, claims, "admin") {
		return
	}
	if s.rpcDispatcher == nil {
		http.Error(w, "rpc unavailable", http.StatusServiceUnavailable)
		return
	}
	rpcID := r.PathValue("id")
	var req struct {
		Payload string `json:"payload"`
		UserID  string `json:"user_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	userID := req.UserID
	if userID == "" {
		userID = claims.UserID
	}
	res, err := s.rpcDispatcher.DispatchRPC(r.Context(), rpcID, req.Payload, userID, claims.Username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "call_rpc", "rpc/"+rpcID, "Called RPC "+rpcID, map[string]any{"user_id": userID})
	writeJSON(w, http.StatusOK, map[string]string{"payload": res})
}
