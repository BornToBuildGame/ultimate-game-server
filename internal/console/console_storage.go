package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/BornToBuildGame/ultimate-game-server/internal/console/acl"
	"github.com/BornToBuildGame/ultimate-game-server/internal/console/consolepb"
	"github.com/BornToBuildGame/ultimate-game-server/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DeleteStorage truncates the storage table (wipe-all).
func DeleteStorage(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("database not configured")
	}
	_, err := pool.Exec(ctx, "TRUNCATE TABLE storage")
	return err
}

// ImportStorageObjects writes a batch of storage objects authoritatively.
func ImportStorageObjects(ctx context.Context, pool *pgxpool.Pool, objects []*storage.StorageObject) error {
	if len(objects) == 0 {
		return nil
	}
	if pool == nil {
		return fmt.Errorf("database not configured")
	}
	_, err := storage.WriteStorageObjects(ctx, pool, true, objects)
	return err
}

func (s *Server) registerStorageAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v2/console/storage/import", s.handleImportStorage)
	mux.HandleFunc("DELETE /v2/console/storage", s.handleDeleteStorage)
}

func (s *Server) handleDeleteStorage(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceStorageData, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	if err := DeleteStorage(r.Context(), s.pool); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleImportStorage(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceStorageDataImport, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var body struct {
		Objects []importStorageObjectJSON `json:"objects"`
	}
	// Also accept a bare array.
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&body); err != nil {
		// rewind not possible; try re-read is hard — require objects wrapper or use raw
		http.Error(w, "invalid body (expect {\"objects\":[...]})", http.StatusBadRequest)
		return
	}
	if len(body.Objects) == 0 {
		http.Error(w, "objects required", http.StatusBadRequest)
		return
	}
	objs := make([]*storage.StorageObject, 0, len(body.Objects))
	for _, o := range body.Objects {
		val := o.Value
		if val == "" {
			val = "{}"
		}
		read := int16(1)
		write := int16(1)
		if o.PermissionRead != 0 {
			read = int16(o.PermissionRead)
		}
		if o.PermissionWrite != 0 {
			write = int16(o.PermissionWrite)
		}
		objs = append(objs, &storage.StorageObject{
			Collection: o.Collection,
			Key:        o.Key,
			UserID:     o.UserID,
			Value:      val,
			Read:       read,
			Write:      write,
		})
	}
	if err := ImportStorageObjects(r.Context(), s.pool, objs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type importStorageObjectJSON struct {
	Collection      string `json:"collection"`
	Key             string `json:"key"`
	UserID          string `json:"user_id"`
	Value           string `json:"value"`
	PermissionRead  int    `json:"permission_read"`
	PermissionWrite int    `json:"permission_write"`
}

func (g *GRPCConsole) DeleteStorage(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if g.Server == nil || g.Server.pool == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	if err := DeleteStorage(ctx, g.Server.pool); err != nil {
		return nil, status.Errorf(codes.Internal, "delete storage: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (g *GRPCConsole) ImportStorage(ctx context.Context, in *consolepb.StorageImport) (*emptypb.Empty, error) {
	if g.Server == nil || g.Server.pool == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	if in == nil || len(in.GetObjects()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "objects required")
	}
	objs := make([]*storage.StorageObject, 0, len(in.GetObjects()))
	for _, o := range in.GetObjects() {
		val := o.GetValue()
		if val == "" {
			val = "{}"
		}
		read, write := int16(o.GetPermissionRead()), int16(o.GetPermissionWrite())
		if read == 0 {
			read = 1
		}
		if write == 0 {
			write = 1
		}
		objs = append(objs, &storage.StorageObject{
			Collection: o.GetCollection(),
			Key:        o.GetKey(),
			UserID:     o.GetUserId(),
			Value:      val,
			Read:       read,
			Write:      write,
		})
	}
	if err := ImportStorageObjects(ctx, g.Server.pool, objs); err != nil {
		return nil, status.Errorf(codes.Internal, "import storage: %v", err)
	}
	return &emptypb.Empty{}, nil
}
