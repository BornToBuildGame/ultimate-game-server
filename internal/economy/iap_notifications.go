package economy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NotificationLogger is optional logging for RTDN handlers.
type NotificationLogger interface {
	Error(msg string, err error)
	Warn(msg string)
}

// NotificationType mirrors store server-notification categories for runtime hooks.
type NotificationType int

const (
	NotificationTypeUnknown NotificationType = iota
	NotificationTypeSubscribed
	NotificationTypeRenewed
	NotificationTypeRefunded
	NotificationTypeExpired
	NotificationTypeRevoked
	NotificationTypeOther
)

// IAPNotificationCallbacks are optional runtime hooks invoked after RTDN processing.
type IAPNotificationCallbacks struct {
	PurchaseApple      func(ctx context.Context, ntype NotificationType, purchase *ValidatedPurchase, raw json.RawMessage) error
	PurchaseGoogle     func(ctx context.Context, ntype NotificationType, purchase *ValidatedPurchase, raw json.RawMessage) error
	SubscriptionApple  func(ctx context.Context, ntype NotificationType, sub *ValidatedSubscription, raw json.RawMessage) error
	SubscriptionGoogle func(ctx context.Context, ntype NotificationType, sub *ValidatedSubscription, raw json.RawMessage) error
}

// MarkPurchaseRefunded sets refund_time on a purchase row.
func MarkPurchaseRefunded(ctx context.Context, pool *pgxpool.Pool, transactionID string, when time.Time) (*ValidatedPurchase, error) {
	if pool == nil || transactionID == "" {
		return nil, fmt.Errorf("invalid refund args")
	}
	if when.IsZero() {
		when = time.Now().UTC()
	}
	vp := &ValidatedPurchase{}
	var raw []byte
	err := pool.QueryRow(ctx, `
UPDATE purchase SET refund_time = $2, update_time = now(), raw_response = COALESCE(raw_response, '{}'::jsonb)
WHERE transaction_id = $1
RETURNING user_id, product_id, transaction_id, store, purchase_time, refund_time, environment, raw_response, create_time, update_time`,
		transactionID, when).Scan(
		&vp.UserID, &vp.ProductID, &vp.TransactionID, &vp.Store, &vp.PurchaseTime, &vp.RefundTime, &vp.Environment, &raw, &vp.CreateTime, &vp.UpdateTime)
	if err != nil {
		return nil, err
	}
	vp.RawResponse = string(raw)
	return vp, nil
}

// GetSubscriptionByOriginalTransactionID looks up a subscription.
func GetSubscriptionByOriginalTransactionID(ctx context.Context, pool *pgxpool.Pool, originalTransactionID string) (*ValidatedSubscription, error) {
	sub := &ValidatedSubscription{}
	var raw []byte
	err := pool.QueryRow(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription WHERE original_transaction_id = $1`, originalTransactionID).Scan(
		&sub.UserID, &sub.ProductID, &sub.OriginalTransactionID, &sub.Store, &sub.PurchaseTime, &sub.ExpireTime, &sub.RefundTime, &sub.Environment, &raw, &sub.CreateTime, &sub.UpdateTime)
	if err != nil {
		return nil, err
	}
	sub.RawResponse = string(raw)
	sub.Active = sub.ExpireTime.After(time.Now().UTC()) && (sub.RefundTime.IsZero() || sub.RefundTime.Unix() <= 0)
	return sub, nil
}

// UpdateSubscriptionExpiry sets expire/refund times from a notification.
func UpdateSubscriptionExpiry(ctx context.Context, pool *pgxpool.Pool, originalTransactionID string, expire, refund time.Time, rawNotification string) (*ValidatedSubscription, error) {
	sub := &ValidatedSubscription{}
	var raw []byte
	err := pool.QueryRow(ctx, `
UPDATE subscription SET
  expire_time = CASE WHEN $2::timestamptz > '1970-01-02'::timestamptz THEN $2 ELSE expire_time END,
  refund_time = CASE WHEN $3::timestamptz > '1970-01-02'::timestamptz THEN $3 ELSE refund_time END,
  raw_response = CASE WHEN $4 <> '' THEN $4::jsonb ELSE raw_response END,
  update_time = now()
WHERE original_transaction_id = $1
RETURNING user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time`,
		originalTransactionID, expire, refund, coalesceJSON(rawNotification)).Scan(
		&sub.UserID, &sub.ProductID, &sub.OriginalTransactionID, &sub.Store, &sub.PurchaseTime, &sub.ExpireTime, &sub.RefundTime, &sub.Environment, &raw, &sub.CreateTime, &sub.UpdateTime)
	if err != nil {
		return nil, err
	}
	sub.RawResponse = string(raw)
	sub.Active = sub.ExpireTime.After(time.Now().UTC()) && (sub.RefundTime.IsZero() || sub.RefundTime.Unix() <= 0)
	return sub, nil
}

func coalesceJSON(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}

func decodeJWSPayload(jws string) ([]byte, error) {
	parts := strings.Split(jws, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid jws")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

type appleNotificationEnvelope struct {
	SignedPayload     string          `json:"signedPayload"`
	NotificationType  string          `json:"notificationType"`
	Subtype           string          `json:"subtype"`
	Data              json.RawMessage `json:"data"`
	NotificationUUID  string          `json:"notificationUUID"`
}

type appleNotificationData struct {
	SignedTransactionInfo string `json:"signedTransactionInfo"`
	SignedRenewalInfo     string `json:"signedRenewalInfo"`
	Environment           string `json:"environment"`
}

type appleTransactionInfo struct {
	TransactionID         string `json:"transactionId"`
	OriginalTransactionID string `json:"originalTransactionId"`
	ProductID             string `json:"productId"`
	ExpiresDate           int64  `json:"expiresDate"`
	RevocationDate        int64  `json:"revocationDate"`
	Type                  string `json:"type"`
}

func mapAppleNotificationType(t string) NotificationType {
	switch strings.ToUpper(t) {
	case "SUBSCRIBED", "OFFER_REDEEMED":
		return NotificationTypeSubscribed
	case "DID_RENEW":
		return NotificationTypeRenewed
	case "REFUND", "REVOKE":
		return NotificationTypeRefunded
	case "EXPIRED", "GRACE_PERIOD_EXPIRED":
		return NotificationTypeExpired
	default:
		if t == "" {
			return NotificationTypeUnknown
		}
		return NotificationTypeOther
	}
}

func parseAppleNotificationBody(body []byte) (ntype NotificationType, tx *appleTransactionInfo, rawType string, err error) {
	var env appleNotificationEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return NotificationTypeUnknown, nil, "", err
	}
	rawType = env.NotificationType
	dataBytes := env.Data
	if env.SignedPayload != "" {
		payload, derr := decodeJWSPayload(env.SignedPayload)
		if derr != nil {
			return NotificationTypeUnknown, nil, "", derr
		}
		var signed appleNotificationEnvelope
		if err := json.Unmarshal(payload, &signed); err != nil {
			return NotificationTypeUnknown, nil, "", err
		}
		rawType = signed.NotificationType
		dataBytes = signed.Data
	}
	ntype = mapAppleNotificationType(rawType)
	if len(dataBytes) == 0 {
		return ntype, nil, rawType, nil
	}
	var data appleNotificationData
	_ = json.Unmarshal(dataBytes, &data)
	if data.SignedTransactionInfo == "" {
		// Test fixture may embed transaction fields directly under data.
		tx = &appleTransactionInfo{}
		_ = json.Unmarshal(dataBytes, tx)
		if tx.TransactionID == "" && tx.OriginalTransactionID == "" {
			return ntype, nil, rawType, nil
		}
		return ntype, tx, rawType, nil
	}
	txBytes, err := decodeJWSPayload(data.SignedTransactionInfo)
	if err != nil {
		return ntype, nil, rawType, err
	}
	tx = &appleTransactionInfo{}
	if err := json.Unmarshal(txBytes, tx); err != nil {
		return ntype, nil, rawType, err
	}
	return ntype, tx, rawType, nil
}

// AppleNotificationHandler handles App Store Server Notifications V2 (and test JSON fixtures).
func AppleNotificationHandler(logger NotificationLogger, pool *pgxpool.Pool, expectedEndpointID string, cbs IAPNotificationCallbacks) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if expectedEndpointID == "" || id != expectedEndpointID {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		ntype, tx, _, err := parseAppleNotificationBody(body)
		if err != nil {
			if logger != nil {
				logger.Error("apple notification parse failed", err)
			}
			http.Error(w, "invalid notification", http.StatusBadRequest)
			return
		}
		raw := json.RawMessage(body)
		ctx := r.Context()

		if tx != nil && (ntype == NotificationTypeRefunded || ntype == NotificationTypeRevoked) && tx.TransactionID != "" && pool != nil {
			when := time.Now().UTC()
			if tx.RevocationDate > 0 {
				when = time.UnixMilli(tx.RevocationDate).UTC()
			}
			vp, err := MarkPurchaseRefunded(ctx, pool, tx.TransactionID, when)
			if err == nil && cbs.PurchaseApple != nil {
				_ = cbs.PurchaseApple(ctx, ntype, vp, raw)
			}
		}
		if tx != nil && tx.OriginalTransactionID != "" && pool != nil {
			expire := time.Time{}
			refund := time.Time{}
			if tx.ExpiresDate > 0 {
				expire = time.UnixMilli(tx.ExpiresDate).UTC()
			}
			if tx.RevocationDate > 0 {
				refund = time.UnixMilli(tx.RevocationDate).UTC()
			}
			sub, err := UpdateSubscriptionExpiry(ctx, pool, tx.OriginalTransactionID, expire, refund, string(body))
			if err != nil {
				sub, _ = GetSubscriptionByOriginalTransactionID(ctx, pool, tx.OriginalTransactionID)
			}
			if cbs.SubscriptionApple != nil {
				_ = cbs.SubscriptionApple(ctx, ntype, sub, raw)
			}
		} else if cbs.SubscriptionApple != nil && (ntype == NotificationTypeSubscribed || ntype == NotificationTypeRenewed || ntype == NotificationTypeExpired) {
			_ = cbs.SubscriptionApple(ctx, ntype, nil, raw)
		} else if cbs.PurchaseApple != nil && ntype != NotificationTypeUnknown {
			_ = cbs.PurchaseApple(ctx, ntype, nil, raw)
		}
		w.WriteHeader(http.StatusOK)
	}
}

type googleRTDNMessage struct {
	Message struct {
		Data string `json:"data"`
	} `json:"message"`
	// Direct fixture fields (tests / non-PubSub).
	PackageName      string `json:"packageName"`
	SubscriptionNotification *struct {
		NotificationType int    `json:"notificationType"`
		PurchaseToken    string `json:"purchaseToken"`
		SubscriptionID   string `json:"subscriptionId"`
	} `json:"subscriptionNotification"`
	OneTimeProductNotification *struct {
		NotificationType int    `json:"notificationType"`
		PurchaseToken    string `json:"purchaseToken"`
		SKU              string `json:"sku"`
	} `json:"oneTimeProductNotification"`
	VoidedPurchaseNotification *struct {
		PurchaseToken string `json:"purchaseToken"`
		OrderID       string `json:"orderId"`
	} `json:"voidedPurchaseNotification"`
}

func mapGoogleSubNotificationType(n int) NotificationType {
	// https://developer.android.com/google/play/billing/rtdn-reference
	switch n {
	case 1, 2: // recovered / renewed
		return NotificationTypeRenewed
	case 4: // purchased
		return NotificationTypeSubscribed
	case 3, 12, 13: // canceled / revoked / expired
		return NotificationTypeExpired
	case 5, 6: // on hold / in grace — treat as other
		return NotificationTypeOther
	default:
		return NotificationTypeOther
	}
}

// GoogleNotificationHandler handles Play RTDN Pub/Sub push (and flat JSON fixtures).
func GoogleNotificationHandler(logger NotificationLogger, pool *pgxpool.Pool, expectedEndpointID string, cbs IAPNotificationCallbacks) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if expectedEndpointID == "" || id != expectedEndpointID {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		var envelope googleRTDNMessage
		if err := json.Unmarshal(body, &envelope); err != nil {
			http.Error(w, "invalid notification", http.StatusBadRequest)
			return
		}
		payload := body
		if envelope.Message.Data != "" {
			decoded, err := base64.StdEncoding.DecodeString(envelope.Message.Data)
			if err != nil {
				http.Error(w, "invalid pubsub data", http.StatusBadRequest)
				return
			}
			payload = decoded
			if err := json.Unmarshal(decoded, &envelope); err != nil {
				http.Error(w, "invalid pubsub payload", http.StatusBadRequest)
				return
			}
		}
		raw := json.RawMessage(payload)
		ctx := r.Context()

		if envelope.VoidedPurchaseNotification != nil {
			orderID := envelope.VoidedPurchaseNotification.OrderID
			var vp *ValidatedPurchase
			if pool != nil && orderID != "" {
				vp, _ = MarkPurchaseRefunded(ctx, pool, orderID, time.Now().UTC())
			}
			if cbs.PurchaseGoogle != nil {
				_ = cbs.PurchaseGoogle(ctx, NotificationTypeRefunded, vp, raw)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if envelope.OneTimeProductNotification != nil {
			nt := NotificationTypeOther
			if envelope.OneTimeProductNotification.NotificationType == 2 {
				nt = NotificationTypeRefunded
			}
			token := envelope.OneTimeProductNotification.PurchaseToken
			var vp *ValidatedPurchase
			if pool != nil && token != "" {
				vp, _ = GetPurchaseByTransactionID(ctx, pool, token)
			}
			if cbs.PurchaseGoogle != nil {
				_ = cbs.PurchaseGoogle(ctx, nt, vp, raw)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if envelope.SubscriptionNotification != nil {
			nt := mapGoogleSubNotificationType(envelope.SubscriptionNotification.NotificationType)
			token := envelope.SubscriptionNotification.PurchaseToken
			var sub *ValidatedSubscription
			if pool != nil && token != "" {
				sub, _ = GetSubscriptionByOriginalTransactionID(ctx, pool, token)
			}
			if cbs.SubscriptionGoogle != nil {
				_ = cbs.SubscriptionGoogle(ctx, nt, sub, raw)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if logger != nil {
			logger.Warn("google notification with no recognized payload")
		}
		w.WriteHeader(http.StatusOK)
	}
}
