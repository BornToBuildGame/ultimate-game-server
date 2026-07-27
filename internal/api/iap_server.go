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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// IAPServer implements apipb.IAPServiceServer.
type IAPServer struct {
	apipb.UnimplementedIAPServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
	hooks    *runtime.HookRegistry
	cfg      economy.IAPConfig
}

func NewIAPServer(pool *pgxpool.Pool, tm *auth.TokenManager, hooks *runtime.HookRegistry, cfg economy.IAPConfig) *IAPServer {
	return &IAPServer{dbPool: pool, tokenMgr: tm, hooks: hooks, cfg: cfg}
}

func (s *IAPServer) SetHooks(hooks *runtime.HookRegistry) { s.hooks = hooks }

func (s *IAPServer) SetConfig(cfg economy.IAPConfig) { s.cfg = cfg }

func (s *IAPServer) authenticate(ctx context.Context) (string, error) {
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

func toProtoPurchase(vp *economy.ValidatedPurchase) *apipb.ValidatedPurchase {
	if vp == nil {
		return nil
	}
	out := &apipb.ValidatedPurchase{
		UserId: vp.UserID, ProductId: vp.ProductID, TransactionId: vp.TransactionID,
		Store: int32(vp.Store), PurchaseTime: timestamppb.New(vp.PurchaseTime),
		SeenBefore: vp.SeenBefore, Environment: int32(vp.Environment),
		ProviderResponse: vp.RawResponse,
	}
	if !vp.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(vp.CreateTime)
	}
	if !vp.UpdateTime.IsZero() {
		out.UpdateTime = timestamppb.New(vp.UpdateTime)
	}
	if !vp.RefundTime.IsZero() && vp.RefundTime.Unix() > 0 {
		out.RefundTime = timestamppb.New(vp.RefundTime)
	}
	return out
}

func toProtoSub(sub *economy.ValidatedSubscription) *apipb.ValidatedSubscription {
	if sub == nil {
		return nil
	}
	out := &apipb.ValidatedSubscription{
		UserId: sub.UserID, ProductId: sub.ProductID, OriginalTransactionId: sub.OriginalTransactionID,
		Store: int32(sub.Store), PurchaseTime: timestamppb.New(sub.PurchaseTime),
		ExpireTime: timestamppb.New(sub.ExpireTime), Active: sub.Active,
		SeenBefore: sub.SeenBefore, Environment: int32(sub.Environment),
		ProviderResponse: sub.RawResponse,
	}
	if !sub.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(sub.CreateTime)
	}
	if !sub.UpdateTime.IsZero() {
		out.UpdateTime = timestamppb.New(sub.UpdateTime)
	}
	if !sub.RefundTime.IsZero() && sub.RefundTime.Unix() > 0 {
		out.RefundTime = timestamppb.New(sub.RefundTime)
	}
	return out
}

func grpcPersist(p *bool) bool {
	if p == nil {
		return true
	}
	return *p
}

func (s *IAPServer) invokeBefore(ctx context.Context, name string, in interface{}) (interface{}, error) {
	if s.hooks == nil {
		return in, nil
	}
	before, ok := s.hooks.GetBefore(name)
	if !ok || before == nil {
		return in, nil
	}
	out, err := before(ctx, nil, nil, nil, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if out == nil {
		return nil, status.Error(codes.NotFound, "Requested resource was not found.")
	}
	return out, nil
}

func (s *IAPServer) invokeAfter(ctx context.Context, name string, out, in interface{}) {
	if s.hooks == nil {
		return
	}
	after, ok := s.hooks.GetAfter(name)
	if !ok || after == nil {
		return
	}
	_ = after(ctx, nil, nil, nil, out, in)
}

func (s *IAPServer) ValidatePurchaseApple(ctx context.Context, req *apipb.ValidatePurchaseAppleRequest) (*apipb.ValidatePurchaseResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidatePurchaseApple", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidatePurchaseAppleRequest); ok {
		req = casted
	}
	vp, err := economy.ValidatePurchaseApple(ctx, s.dbPool, s.cfg, userID, req.GetReceipt(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidatePurchaseResponse{ValidatedPurchases: []*apipb.ValidatedPurchase{toProtoPurchase(vp)}}
	s.invokeAfter(ctx, "ValidatePurchaseApple", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidatePurchaseGoogle(ctx context.Context, req *apipb.ValidatePurchaseGoogleRequest) (*apipb.ValidatePurchaseResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidatePurchaseGoogle", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidatePurchaseGoogleRequest); ok {
		req = casted
	}
	productID, token := resolveGooglePurchaseFields(req)
	vp, err := economy.ValidatePurchaseGoogle(ctx, s.dbPool, s.cfg, userID, productID, token, grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidatePurchaseResponse{ValidatedPurchases: []*apipb.ValidatedPurchase{toProtoPurchase(vp)}}
	s.invokeAfter(ctx, "ValidatePurchaseGoogle", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidatePurchaseHuawei(ctx context.Context, req *apipb.ValidatePurchaseHuaweiRequest) (*apipb.ValidatePurchaseResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidatePurchaseHuawei", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidatePurchaseHuaweiRequest); ok {
		req = casted
	}
	purchaseData := req.GetPurchaseData()
	if purchaseData == "" {
		purchaseData = req.GetPurchase()
	}
	vp, err := economy.ValidatePurchaseHuawei(ctx, s.dbPool, s.cfg, userID, purchaseData, req.GetSignature(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidatePurchaseResponse{ValidatedPurchases: []*apipb.ValidatedPurchase{toProtoPurchase(vp)}}
	s.invokeAfter(ctx, "ValidatePurchaseHuawei", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidatePurchaseFacebookInstant(ctx context.Context, req *apipb.ValidatePurchaseFacebookInstantRequest) (*apipb.ValidatePurchaseResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidatePurchaseFacebookInstant", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidatePurchaseFacebookInstantRequest); ok {
		req = casted
	}
	vp, err := economy.ValidatePurchaseFacebookInstant(ctx, s.dbPool, s.cfg, userID, req.GetSignedRequest(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidatePurchaseResponse{ValidatedPurchases: []*apipb.ValidatedPurchase{toProtoPurchase(vp)}}
	s.invokeAfter(ctx, "ValidatePurchaseFacebookInstant", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidatePurchaseSamsung(ctx context.Context, req *apipb.ValidatePurchaseSamsungRequest) (*apipb.ValidatePurchaseResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidatePurchaseSamsung", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidatePurchaseSamsungRequest); ok {
		req = casted
	}
	vp, err := economy.ValidatePurchaseSamsung(ctx, s.dbPool, s.cfg, userID, req.GetPurchaseId(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidatePurchaseResponse{ValidatedPurchases: []*apipb.ValidatedPurchase{toProtoPurchase(vp)}}
	s.invokeAfter(ctx, "ValidatePurchaseSamsung", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidateSubscriptionApple(ctx context.Context, req *apipb.ValidateSubscriptionAppleRequest) (*apipb.ValidateSubscriptionResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidateSubscriptionApple", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidateSubscriptionAppleRequest); ok {
		req = casted
	}
	sub, err := economy.ValidateSubscriptionApple(ctx, s.dbPool, s.cfg, userID, req.GetReceipt(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidateSubscriptionResponse{ValidatedSubscription: toProtoSub(sub)}
	s.invokeAfter(ctx, "ValidateSubscriptionApple", resp, req)
	return resp, nil
}

func (s *IAPServer) ValidateSubscriptionGoogle(ctx context.Context, req *apipb.ValidateSubscriptionGoogleRequest) (*apipb.ValidateSubscriptionResponse, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ValidateSubscriptionGoogle", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ValidateSubscriptionGoogleRequest); ok {
		req = casted
	}
	sub, err := economy.ValidateSubscriptionGoogle(ctx, s.dbPool, s.cfg, userID, req.GetProductId(), req.GetPurchaseToken(), grpcPersist(req.Persist))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	resp := &apipb.ValidateSubscriptionResponse{ValidatedSubscription: toProtoSub(sub)}
	s.invokeAfter(ctx, "ValidateSubscriptionGoogle", resp, req)
	return resp, nil
}

func (s *IAPServer) ListSubscriptions(ctx context.Context, req *apipb.ListSubscriptionsRequest) (*apipb.SubscriptionList, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "ListSubscriptions", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.ListSubscriptionsRequest); ok {
		req = casted
	}
	list, err := economy.ListSubscriptions(ctx, s.dbPool, userID, int(req.GetLimit()), req.GetCursor())
	if err != nil {
		if errors.Is(err, economy.ErrSubscriptionsListInvalidCursor) {
			return nil, status.Error(codes.InvalidArgument, "cursor is invalid")
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &apipb.SubscriptionList{Cursor: list.Cursor, PrevCursor: list.PrevCursor}
	for _, sub := range list.Subscriptions {
		out.ValidatedSubscriptions = append(out.ValidatedSubscriptions, toProtoSub(sub))
	}
	s.invokeAfter(ctx, "ListSubscriptions", out, req)
	return out, nil
}

func (s *IAPServer) GetSubscription(ctx context.Context, req *apipb.GetSubscriptionRequest) (*apipb.ValidatedSubscription, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if in, err := s.invokeBefore(ctx, "GetSubscription", req); err != nil {
		return nil, err
	} else if casted, ok := in.(*apipb.GetSubscriptionRequest); ok {
		req = casted
	}
	sub, err := economy.GetSubscriptionByProductID(ctx, s.dbPool, userID, req.GetProductId())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "subscription not found")
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := toProtoSub(sub)
	s.invokeAfter(ctx, "GetSubscription", out, req)
	return out, nil
}

func persistFlag(r *http.Request, bodyPersist *bool) bool {
	if bodyPersist != nil {
		return *bodyPersist
	}
	return true
}

func (s *Server) iapCfg() economy.IAPConfig {
	return s.iapConfig
}

func (s *Server) handleValidatePurchaseApple(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Receipt string `json:"receipt"`
		Persist *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	vp, err := economy.ValidatePurchaseApple(r.Context(), s.dbPool, s.iapCfg(), userID, req.Receipt, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writePurchaseResp(w, vp)
}

func (s *Server) handleValidatePurchaseGoogle(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Purchase      string `json:"purchase"`
		ProductID     string `json:"product_id"`
		PurchaseToken string `json:"purchase_token"`
		Persist       *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	productID, token := parseGooglePurchaseBlob(req.Purchase, req.ProductID, req.PurchaseToken)
	vp, err := economy.ValidatePurchaseGoogle(r.Context(), s.dbPool, s.iapCfg(), userID, productID, token, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writePurchaseResp(w, vp)
}

func (s *Server) handleValidatePurchaseHuawei(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Purchase     string `json:"purchase"`
		PurchaseData string `json:"purchase_data"`
		Signature    string `json:"signature"`
		Persist      *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	purchaseData := req.PurchaseData
	if purchaseData == "" {
		purchaseData = req.Purchase
	}
	vp, err := economy.ValidatePurchaseHuawei(r.Context(), s.dbPool, s.iapCfg(), userID, purchaseData, req.Signature, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writePurchaseResp(w, vp)
}

func (s *Server) handleValidatePurchaseFacebookInstant(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		SignedRequest string `json:"signed_request"`
		Persist       *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	vp, err := economy.ValidatePurchaseFacebookInstant(r.Context(), s.dbPool, s.iapCfg(), userID, req.SignedRequest, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writePurchaseResp(w, vp)
}

func (s *Server) handleValidatePurchaseSamsung(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		PurchaseID string `json:"purchase_id"`
		Persist    *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	vp, err := economy.ValidatePurchaseSamsung(r.Context(), s.dbPool, s.iapCfg(), userID, req.PurchaseID, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writePurchaseResp(w, vp)
}

func (s *Server) handleValidateSubscriptionApple(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		Receipt string `json:"receipt"`
		Persist *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sub, err := economy.ValidateSubscriptionApple(r.Context(), s.dbPool, s.iapCfg(), userID, req.Receipt, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeSubResp(w, sub)
}

func (s *Server) handleValidateSubscriptionGoogle(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req struct {
		ProductID     string `json:"product_id"`
		PurchaseToken string `json:"purchase_token"`
		Persist       *bool  `json:"persist"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sub, err := economy.ValidateSubscriptionGoogle(r.Context(), s.dbPool, s.iapCfg(), userID, req.ProductID, req.PurchaseToken, persistFlag(r, req.Persist))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeSubResp(w, sub)
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
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
	cursor := r.URL.Query().Get("cursor")
	if r.Method == http.MethodPost {
		var body struct {
			Limit  int    `json:"limit"`
			Cursor string `json:"cursor"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Limit > 0 {
			limit = body.Limit
		}
		if body.Cursor != "" {
			cursor = body.Cursor
		}
	}
	list, err := economy.ListSubscriptions(r.Context(), s.dbPool, userID, limit, cursor)
	if err != nil {
		if errors.Is(err, economy.ErrSubscriptionsListInvalidCursor) {
			http.Error(w, "cursor is invalid", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]map[string]interface{}, 0, len(list.Subscriptions))
	for _, sub := range list.Subscriptions {
		items = append(items, subToMap(sub))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"validated_subscriptions": items,
		"cursor":                  list.Cursor,
		"prev_cursor":             list.PrevCursor,
	})
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	productID := r.PathValue("product_id")
	sub, err := economy.GetSubscriptionByProductID(r.Context(), s.dbPool, userID, productID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(subToMap(sub))
}

func writePurchaseResp(w http.ResponseWriter, vp *economy.ValidatedPurchase) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"validated_purchases": []map[string]interface{}{purchaseToMap(vp)},
	})
}

func writeSubResp(w http.ResponseWriter, sub *economy.ValidatedSubscription) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"validated_subscription": subToMap(sub),
	})
}

func purchaseToMap(vp *economy.ValidatedPurchase) map[string]interface{} {
	m := map[string]interface{}{
		"user_id": vp.UserID, "product_id": vp.ProductID, "transaction_id": vp.TransactionID,
		"store": vp.Store, "purchase_time": vp.PurchaseTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		"seen_before": vp.SeenBefore, "environment": vp.Environment,
		"provider_response": vp.RawResponse,
	}
	if !vp.CreateTime.IsZero() {
		m["create_time"] = vp.CreateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	if !vp.UpdateTime.IsZero() {
		m["update_time"] = vp.UpdateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	if !vp.RefundTime.IsZero() && vp.RefundTime.Unix() > 0 {
		m["refund_time"] = vp.RefundTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	return m
}

func subToMap(sub *economy.ValidatedSubscription) map[string]interface{} {
	m := map[string]interface{}{
		"user_id": sub.UserID, "product_id": sub.ProductID,
		"original_transaction_id": sub.OriginalTransactionID, "store": sub.Store,
		"purchase_time": sub.PurchaseTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		"expire_time":   sub.ExpireTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		"active": sub.Active, "seen_before": sub.SeenBefore, "environment": sub.Environment,
		"provider_response": sub.RawResponse,
	}
	if !sub.CreateTime.IsZero() {
		m["create_time"] = sub.CreateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	if !sub.UpdateTime.IsZero() {
		m["update_time"] = sub.UpdateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	if !sub.RefundTime.IsZero() && sub.RefundTime.Unix() > 0 {
		m["refund_time"] = sub.RefundTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	return m
}

func resolveGooglePurchaseFields(req *apipb.ValidatePurchaseGoogleRequest) (productID, token string) {
	if req == nil {
		return "", ""
	}
	return parseGooglePurchaseBlob(req.GetPurchase(), req.GetProductId(), req.GetPurchaseToken())
}

func parseGooglePurchaseBlob(purchase, productID, token string) (string, string) {
	if productID != "" && token != "" {
		return productID, token
	}
	if purchase == "" {
		return productID, token
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(purchase), &m); err != nil {
		return productID, token
	}
	if productID == "" {
		productID = stringField(m, "productId", "product_id")
	}
	if token == "" {
		token = stringField(m, "purchaseToken", "purchase_token")
	}
	return productID, token
}

func stringField(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}
