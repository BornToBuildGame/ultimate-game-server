package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EconomyServer implements apipb.EconomyServiceServer.
type EconomyServer struct {
	apipb.UnimplementedEconomyServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
}

func NewEconomyServer(pool *pgxpool.Pool, tm *auth.TokenManager, hooks *runtime.HookRegistry) *EconomyServer {
	return &EconomyServer{dbPool: pool, tokenMgr: tm, hooks: hooks}
}

func (s *EconomyServer) SetHooks(hooks *runtime.HookRegistry) { s.hooks = hooks }

func (s *EconomyServer) authenticate(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims.UserID, nil
}

func (s *EconomyServer) GetWallet(ctx context.Context, _ *emptypb.Empty) (*apipb.Wallet, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	w, err := economy.GetWallet(ctx, s.dbPool, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get wallet: %v", err)
	}
	b, _ := json.Marshal(w)
	return &apipb.Wallet{Wallet: string(b)}, nil
}

func (s *EconomyServer) ListWalletLedger(ctx context.Context, req *apipb.ListWalletLedgerRequest) (*apipb.WalletLedgerList, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	list, err := economy.ListWalletLedger(ctx, s.dbPool, userID, int(req.GetLimit()), req.GetCursor())
	if err != nil {
		if errors.Is(err, economy.ErrLedgerCursorInvalid) {
			return nil, status.Error(codes.InvalidArgument, "cursor is invalid")
		}
		return nil, status.Errorf(codes.Internal, "list ledger: %v", err)
	}
	out := &apipb.WalletLedgerList{NextCursor: list.NextCursor}
	for _, item := range list.Items {
		cs, _ := json.Marshal(item.Changeset)
		meta, _ := json.Marshal(item.Metadata)
		out.Items = append(out.Items, &apipb.WalletLedgerItem{
			Id: item.ID, UserId: item.UserID,
			Changeset: string(cs), Metadata: string(meta),
			CreateTime: timestamppb.New(item.CreateTime),
			UpdateTime: timestamppb.New(item.UpdateTime),
		})
	}
	return out, nil
}

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
