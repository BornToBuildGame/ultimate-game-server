package console

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/BornToBuildGame/ultimate-game-server/internal/economy"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"
)

// IAPNotificationDeps wires store RTDN endpoints on the console mux.
type IAPNotificationDeps struct {
	AppleEndpointID  string
	GoogleEndpointID string
	Registry         *runtime.HookRegistry
	NK               runtime.RuntimeModule
	Logger           runtime.Logger
}

// SetIAPNotificationDeps configures Apple/Google notification webhook routes.
func (s *Server) SetIAPNotificationDeps(d IAPNotificationDeps) {
	s.iapNotify = d
}

func (s *Server) registerIAPNotificationRoutes(mux *http.ServeMux) {
	appleID := s.iapNotify.AppleEndpointID
	googleID := s.iapNotify.GoogleEndpointID
	if appleID == "" && googleID == "" {
		return
	}
	cbs := economy.IAPNotificationCallbacks{}
	if s.iapNotify.Registry != nil {
		cbs = economy.IAPNotificationCallbacks{
			PurchaseApple: func(ctx context.Context, ntype economy.NotificationType, purchase *economy.ValidatedPurchase, raw json.RawMessage) error {
				fn := s.iapNotify.Registry.GetPurchaseNotificationApple()
				if fn == nil {
					return nil
				}
				return fn(ctx, s.iapNotify.Logger, nil, s.iapNotify.NK, int(ntype), purchaseView(purchase), string(raw))
			},
			PurchaseGoogle: func(ctx context.Context, ntype economy.NotificationType, purchase *economy.ValidatedPurchase, raw json.RawMessage) error {
				fn := s.iapNotify.Registry.GetPurchaseNotificationGoogle()
				if fn == nil {
					return nil
				}
				return fn(ctx, s.iapNotify.Logger, nil, s.iapNotify.NK, int(ntype), purchaseView(purchase), string(raw))
			},
			SubscriptionApple: func(ctx context.Context, ntype economy.NotificationType, sub *economy.ValidatedSubscription, raw json.RawMessage) error {
				fn := s.iapNotify.Registry.GetSubscriptionNotificationApple()
				if fn == nil {
					return nil
				}
				return fn(ctx, s.iapNotify.Logger, nil, s.iapNotify.NK, int(ntype), subscriptionView(sub), string(raw))
			},
			SubscriptionGoogle: func(ctx context.Context, ntype economy.NotificationType, sub *economy.ValidatedSubscription, raw json.RawMessage) error {
				fn := s.iapNotify.Registry.GetSubscriptionNotificationGoogle()
				if fn == nil {
					return nil
				}
				return fn(ctx, s.iapNotify.Logger, nil, s.iapNotify.NK, int(ntype), subscriptionView(sub), string(raw))
			},
		}
	}
	log := &economyNotifyLog{l: s.logger}
	if appleID != "" {
		h := economy.AppleNotificationHandler(log, s.pool, appleID, cbs)
		mux.HandleFunc("POST /v2/console/apple/notifications/{id}", h)
		mux.HandleFunc("POST /v2/console/apple/subscriptions/{id}", h) // legacy
	}
	if googleID != "" {
		h := economy.GoogleNotificationHandler(log, s.pool, googleID, cbs)
		mux.HandleFunc("POST /v2/console/google/notifications/{id}", h)
		mux.HandleFunc("POST /v2/console/google/subscriptions/{id}", h) // legacy
	}
}

type economyNotifyLog struct{ l Logger }

func (e *economyNotifyLog) Error(msg string, err error) {
	if e != nil && e.l != nil {
		e.l.Error(msg, "err", err)
	}
}
func (e *economyNotifyLog) Warn(msg string) {
	if e != nil && e.l != nil {
		e.l.Info(msg) // Logger has no Warn; use Info
	}
}

func purchaseView(p *economy.ValidatedPurchase) *runtime.ValidatedPurchaseView {
	if p == nil {
		return nil
	}
	return &runtime.ValidatedPurchaseView{
		UserID: p.UserID, ProductID: p.ProductID, TransactionID: p.TransactionID,
		Store: p.Store, PurchaseTime: p.PurchaseTime, SeenBefore: p.SeenBefore, Environment: p.Environment,
	}
}

func subscriptionView(s *economy.ValidatedSubscription) *runtime.ValidatedSubscriptionView {
	if s == nil {
		return nil
	}
	return &runtime.ValidatedSubscriptionView{
		UserID: s.UserID, ProductID: s.ProductID, OriginalTransactionID: s.OriginalTransactionID,
		Store: s.Store, PurchaseTime: s.PurchaseTime, ExpireTime: s.ExpireTime,
		Active: s.Active, SeenBefore: s.SeenBefore, Environment: s.Environment,
	}
}
