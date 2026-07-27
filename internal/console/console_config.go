package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"ultimate-game-server/internal/console/acl"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DeleteAllData truncates application tables and re-seeds the system user.
func DeleteAllData(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("database not configured")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tables := []string{
		"wallet_ledger", "storage", "user_device", "users",
		"leaderboard_record", "notification", "group_edge", "groups",
		"friend_edge", "matchmaker_ticket",
	}
	for _, t := range tables {
		if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+t+" CASCADE"); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO users (id, username, display_name, metadata)
		VALUES ('00000000-0000-0000-0000-000000000001', 'system', 'System', '{}')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AccountExport is the portable account import blob.
type AccountExport struct {
	UserID      string                 `json:"user_id"`
	Username    string                 `json:"username"`
	DisplayName string                 `json:"display_name"`
	Metadata    map[string]interface{} `json:"metadata"`
	Wallet      map[string]int64       `json:"wallet"`
	Storage     []StorageExportObject  `json:"storage"`
}

// StorageExportObject is a storage row in import/export.
type StorageExportObject struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	Version    string `json:"version"`
}

// ImportAccount creates or replaces an account from export data.
func ImportAccount(ctx context.Context, pool *pgxpool.Pool, data *AccountExport) error {
	if pool == nil || data == nil {
		return fmt.Errorf("invalid import")
	}
	meta, _ := json.Marshal(data.Metadata)
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO users (id, username, display_name, metadata, wallet)
		VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
		ON CONFLICT (id) DO UPDATE SET
			username = EXCLUDED.username,
			display_name = EXCLUDED.display_name,
			metadata = EXCLUDED.metadata,
			wallet = EXCLUDED.wallet,
			update_time = now()`,
		data.UserID, data.Username, data.DisplayName, string(meta), walletJSON(data.Wallet))
	if err != nil {
		return err
	}
	for _, obj := range data.Storage {
		_, err = tx.Exec(ctx, `
			INSERT INTO storage (collection, key, user_id, value, version, read, write)
			VALUES ($1, $2, $3, $4::jsonb, $5, 2, 1)
			ON CONFLICT (collection, key, user_id) DO UPDATE SET
				value = EXCLUDED.value, version = EXCLUDED.version, update_time = now()`,
			obj.Collection, obj.Key, data.UserID, obj.Value, obj.Version)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func walletJSON(w map[string]int64) string {
	b, _ := json.Marshal(w)
	return string(b)
}

func (s *Server) handleDeleteAllData(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAllData, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	if err := DeleteAllData(r.Context(), s.pool); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleImportAccount(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceStorageDataImport, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var data AccountExport
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if id := r.PathValue("id"); id != "" {
		data.UserID = id
	}
	if err := ImportAccount(r.Context(), s.pool, &data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
