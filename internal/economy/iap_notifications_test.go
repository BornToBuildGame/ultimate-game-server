package economy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAppleNotification_Fixture(t *testing.T) {
	body := []byte(`{
		"notificationType": "REFUND",
		"data": {
			"transactionId": "tx-1",
			"originalTransactionId": "orig-1",
			"productId": "coins",
			"revocationDate": 1700000000000
		}
	}`)
	nt, tx, raw, err := parseAppleNotificationBody(body)
	require.NoError(t, err)
	require.Equal(t, NotificationTypeRefunded, nt)
	require.Equal(t, "REFUND", raw)
	require.NotNil(t, tx)
	require.Equal(t, "tx-1", tx.TransactionID)
	require.Equal(t, "orig-1", tx.OriginalTransactionID)
}

func TestAppleNotificationHandler_EndpointIDAndHook(t *testing.T) {
	var called atomic.Bool
	cbs := IAPNotificationCallbacks{
		PurchaseApple: func(ctx context.Context, ntype NotificationType, purchase *ValidatedPurchase, raw json.RawMessage) error {
			called.Store(true)
			require.Equal(t, NotificationTypeSubscribed, ntype)
			return nil
		},
		SubscriptionApple: func(ctx context.Context, ntype NotificationType, sub *ValidatedSubscription, raw json.RawMessage) error {
			called.Store(true)
			return nil
		},
	}
	h := AppleNotificationHandler(nil, nil, "secret-id", cbs)

	req := httptest.NewRequest(http.MethodPost, "/v2/console/apple/notifications/wrong", bytes.NewReader([]byte(`{"notificationType":"SUBSCRIBED"}`)))
	req.SetPathValue("id", "wrong")
	rr := httptest.NewRecorder()
	h(rr, req)
	require.Equal(t, http.StatusNotFound, rr.Code)

	req = httptest.NewRequest(http.MethodPost, "/v2/console/apple/notifications/secret-id", bytes.NewReader([]byte(`{"notificationType":"SUBSCRIBED"}`)))
	req.SetPathValue("id", "secret-id")
	rr = httptest.NewRecorder()
	h(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.True(t, called.Load())
}

func TestGoogleNotificationHandler_SubscriptionFixture(t *testing.T) {
	var got NotificationType
	cbs := IAPNotificationCallbacks{
		SubscriptionGoogle: func(ctx context.Context, ntype NotificationType, sub *ValidatedSubscription, raw json.RawMessage) error {
			got = ntype
			return nil
		},
	}
	h := GoogleNotificationHandler(nil, nil, "g-id", cbs)
	body := []byte(`{"subscriptionNotification":{"notificationType":4,"purchaseToken":"tok","subscriptionId":"sub1"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v2/console/google/notifications/g-id", bytes.NewReader(body))
	req.SetPathValue("id", "g-id")
	rr := httptest.NewRecorder()
	h(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, NotificationTypeSubscribed, got)
}
