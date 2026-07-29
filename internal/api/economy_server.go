package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

type EconomyServer struct {
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
}

func NewEconomyServer(pool *pgxpool.Pool, tm *auth.TokenManager, hooks *runtime.HookRegistry) *EconomyServer {
	return &EconomyServer{dbPool: pool, tokenMgr: tm, hooks: hooks}
}

func (s *EconomyServer) SetHooks(hooks *runtime.HookRegistry) { s.hooks = hooks }



func (s *Server) handleGetWallet(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	wallet, err := economy.GetWallet(r.Context(), s.dbPool, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"wallet": wallet})
}

func (s *Server) handleListWalletLedger(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	list, err := economy.ListWalletLedger(r.Context(), s.dbPool, userID, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		if errors.Is(err, economy.ErrLedgerCursorInvalid) {
			http.Error(w, "cursor is invalid", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]map[string]interface{}, 0, len(list.Items))
	for _, item := range list.Items {
		items = append(items, map[string]interface{}{
			"id": item.ID, "user_id": item.UserID,
			"changeset": item.Changeset, "metadata": item.Metadata,
			"create_time": item.CreateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			"update_time": item.UpdateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": items, "next_cursor": list.NextCursor})
}
