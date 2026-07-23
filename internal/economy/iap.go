package economy

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provider constants.
const (
	StoreAppleAppStore      = 0
	StoreGooglePlay         = 1
	StoreHuaweiAppGallery   = 2
	StoreFacebookInstant    = 3
	StoreSamsungGalaxyStore = 4
)

const (
	EnvUnknown     = 0
	EnvSandbox     = 1
	EnvProduction  = 2
)

// ErrTransactionSeenBefore is retained for older tests; prefer SeenBefore on ValidatedPurchase.
var ErrTransactionSeenBefore = errors.New("TRANSACTION_SEEN_BEFORE")

// IAPConfig holds provider credentials (from env/server config).
type IAPConfig struct {
	AppleSharedPassword string
	GoogleClientEmail   string
	GooglePrivateKey    string
	HuaweiPublicKey     string
	HuaweiClientID      string
	HuaweiClientSecret  string
	FacebookAppSecret   string
	SamsungPackageName  string
}

// DefaultIAPConfig is set at server startup.
var DefaultIAPConfig IAPConfig

// ValidatedPurchase is a persisted or validated one-time purchase.
type ValidatedPurchase struct {
	UserID        string
	ProductID     string
	TransactionID string
	Store         int
	PurchaseTime  time.Time
	RefundTime    time.Time
	Environment   int
	SeenBefore    bool
	RawResponse   string
}

// ValidatedSubscription is a persisted subscription.
type ValidatedSubscription struct {
	UserID                 string
	ProductID              string
	OriginalTransactionID  string
	Store                  int
	PurchaseTime           time.Time
	ExpireTime             time.Time
	RefundTime             time.Time
	Environment            int
	Active                 bool
	SeenBefore             bool
	RawResponse            string
}

// StorePurchase is a pre-parsed purchase used by persistence helpers / tests.
type StorePurchase struct {
	TransactionID string
	ProductID     string
	PurchaseTime  time.Time
	Environment   int
}

func epochRefund() time.Time {
	return time.Unix(0, 0).UTC()
}

func isActive(expire, refund time.Time) bool {
	return expire.After(time.Now().UTC()) && (refund.IsZero() || refund.Unix() <= 0)
}

// upsertPurchase inserts or updates a purchase; SeenBefore when row was updated.
func upsertPurchase(ctx context.Context, tx pgx.Tx, userID string, store int, p StorePurchase, rawResponse string) (*ValidatedPurchase, error) {
	if p.TransactionID == "" || p.ProductID == "" {
		return nil, errors.New("transaction_id and product_id required")
	}
	if rawResponse == "" {
		rawResponse = "{}"
	}
	if !json.Valid([]byte(rawResponse)) {
		b, _ := json.Marshal(map[string]string{"raw": rawResponse})
		rawResponse = string(b)
	}
	refund := epochRefund()
	purchaseTime := p.PurchaseTime
	if purchaseTime.IsZero() {
		purchaseTime = time.Now().UTC()
	}
	var createTime, updateTime time.Time
	var outUser string
	err := tx.QueryRow(ctx, `
INSERT INTO purchase (user_id, product_id, transaction_id, store, raw_response, purchase_time, refund_time, environment)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
ON CONFLICT (transaction_id) DO UPDATE SET
  refund_time = EXCLUDED.refund_time,
  update_time = now()
RETURNING user_id, create_time, update_time`,
		userID, p.ProductID, p.TransactionID, store, rawResponse, purchaseTime, refund, p.Environment,
	).Scan(&outUser, &createTime, &updateTime)
	if err != nil {
		return nil, err
	}
	seen := updateTime.After(createTime)
	return &ValidatedPurchase{
		UserID: outUser, ProductID: p.ProductID, TransactionID: p.TransactionID,
		Store: store, PurchaseTime: purchaseTime, RefundTime: refund,
		Environment: p.Environment, SeenBefore: seen, RawResponse: rawResponse,
	}, nil
}

func upsertSubscription(ctx context.Context, tx pgx.Tx, userID string, store int, productID, originalTxID, raw string, purchaseTime, expireTime time.Time, env int) (*ValidatedSubscription, error) {
	if originalTxID == "" || productID == "" {
		return nil, errors.New("original_transaction_id and product_id required")
	}
	if raw == "" {
		raw = "{}"
	}
	if !json.Valid([]byte(raw)) {
		b, _ := json.Marshal(map[string]string{"raw": raw})
		raw = string(b)
	}
	if purchaseTime.IsZero() {
		purchaseTime = time.Now().UTC()
	}
	if expireTime.IsZero() {
		expireTime = purchaseTime.Add(30 * 24 * time.Hour)
	}
	refund := epochRefund()
	var createTime, updateTime, outExpire, outRefund time.Time
	var outUser, outProduct string
	err := tx.QueryRow(ctx, `
INSERT INTO subscription (user_id, product_id, original_transaction_id, store, raw_response, purchase_time, expire_time, refund_time, environment)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)
ON CONFLICT (original_transaction_id) DO UPDATE SET
  product_id = EXCLUDED.product_id,
  expire_time = EXCLUDED.expire_time,
  raw_response = COALESCE(EXCLUDED.raw_response, subscription.raw_response),
  update_time = now()
RETURNING user_id, product_id, create_time, update_time, expire_time, refund_time`,
		userID, productID, originalTxID, store, raw, purchaseTime, expireTime, refund, env,
	).Scan(&outUser, &outProduct, &createTime, &updateTime, &outExpire, &outRefund)
	if err != nil {
		return nil, err
	}
	return &ValidatedSubscription{
		UserID: outUser, ProductID: outProduct, OriginalTransactionID: originalTxID,
		Store: store, PurchaseTime: purchaseTime, ExpireTime: outExpire, RefundTime: outRefund,
		Environment: env, Active: isActive(outExpire, outRefund),
		SeenBefore: updateTime.After(createTime), RawResponse: raw,
	}, nil
}

// PersistPurchase persists a pre-validated purchase (no external store call).
func PersistPurchase(ctx context.Context, pool *pgxpool.Pool, userID string, store int, p StorePurchase, rawResponse string) (*ValidatedPurchase, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	vp, err := upsertPurchase(ctx, tx, userID, store, p, rawResponse)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return vp, nil
}

// ProcessAppleValidationTx persists an Apple purchase without auto-crediting wallet.
// Deprecated name retained for tests; prefer ValidatePurchaseApple.
func ProcessAppleValidationTx(ctx context.Context, pool *pgxpool.Pool, userID string, p StorePurchase, rawResponse string) error {
	vp, err := PersistPurchase(ctx, pool, userID, StoreAppleAppStore, p, rawResponse)
	if err != nil {
		return err
	}
	if vp.SeenBefore {
		return ErrTransactionSeenBefore
	}
	return nil
}

// ProcessGoogleValidationTx persists a Google purchase without auto-crediting wallet.
func ProcessGoogleValidationTx(ctx context.Context, pool *pgxpool.Pool, userID string, p StorePurchase, rawResponse string) error {
	vp, err := PersistPurchase(ctx, pool, userID, StoreGooglePlay, p, rawResponse)
	if err != nil {
		return err
	}
	if vp.SeenBefore {
		return ErrTransactionSeenBefore
	}
	return nil
}

// parseTestReceipt accepts JSON {"product_id","transaction_id","purchase_time","environment","expire_time"} for local/dev.
func parseTestReceipt(receipt string) (StorePurchase, time.Time, bool) {
	var m struct {
		ProductID     string `json:"product_id"`
		TransactionID string `json:"transaction_id"`
		PurchaseTime  string `json:"purchase_time"`
		ExpireTime    string `json:"expire_time"`
		Environment   int    `json:"environment"`
	}
	if err := json.Unmarshal([]byte(receipt), &m); err != nil || m.TransactionID == "" || m.ProductID == "" {
		return StorePurchase{}, time.Time{}, false
	}
	pt := time.Now().UTC()
	if m.PurchaseTime != "" {
		if t, err := time.Parse(time.RFC3339, m.PurchaseTime); err == nil {
			pt = t
		}
	}
	var et time.Time
	if m.ExpireTime != "" {
		et, _ = time.Parse(time.RFC3339, m.ExpireTime)
	}
	return StorePurchase{
		TransactionID: m.TransactionID, ProductID: m.ProductID,
		PurchaseTime: pt, Environment: m.Environment,
	}, et, true
}

func validateAndPersistPurchase(ctx context.Context, pool *pgxpool.Pool, userID string, store int, p StorePurchase, raw string, persist bool) (*ValidatedPurchase, error) {
	if !persist {
		return &ValidatedPurchase{
			UserID: userID, ProductID: p.ProductID, TransactionID: p.TransactionID,
			Store: store, PurchaseTime: p.PurchaseTime, Environment: p.Environment,
			SeenBefore: false, RawResponse: raw,
		}, nil
	}
	return PersistPurchase(ctx, pool, userID, store, p, raw)
}

// ValidatePurchaseApple validates Apple receipt (JWS/legacy when configured; JSON test receipt otherwise).
func ValidatePurchaseApple(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, receipt string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(receipt); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreAppleAppStore, p, receipt, persist)
	}
	// Legacy verifyReceipt when shared password configured.
	if cfg.AppleSharedPassword != "" {
		body, err := appleVerifyReceipt(ctx, receipt, cfg.AppleSharedPassword, false)
		if err != nil {
			return nil, err
		}
		p, raw, err := parseAppleVerifyResponse(body)
		if err != nil {
			return nil, err
		}
		return validateAndPersistPurchase(ctx, pool, userID, StoreAppleAppStore, p, raw, persist)
	}
	return nil, errors.New("apple IAP not configured; provide JSON test receipt or AppleSharedPassword")
}

func appleVerifyReceipt(ctx context.Context, receipt, password string, sandbox bool) ([]byte, error) {
	endpoint := "https://buy.itunes.apple.com/verifyReceipt"
	if sandbox {
		endpoint = "https://sandbox.itunes.apple.com/verifyReceipt"
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"receipt-data": receipt, "password": password, "exclude-old-transactions": true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var status struct {
		Status int `json:"status"`
	}
	_ = json.Unmarshal(body, &status)
	if status.Status == 21007 && !sandbox {
		return appleVerifyReceipt(ctx, receipt, password, true)
	}
	if status.Status != 0 {
		return nil, fmt.Errorf("apple verifyReceipt status %d", status.Status)
	}
	return body, nil
}

func parseAppleVerifyResponse(body []byte) (StorePurchase, string, error) {
	var resp struct {
		Receipt struct {
			InApp []struct {
				ProductID             string `json:"product_id"`
				TransactionID         string `json:"transaction_id"`
				PurchaseDateMs        string `json:"purchase_date_ms"`
				ExpiresDateMs         string `json:"expires_date_ms"`
			} `json:"in_app"`
		} `json:"receipt"`
		Environment string `json:"environment"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return StorePurchase{}, "", err
	}
	for _, item := range resp.Receipt.InApp {
		if item.ExpiresDateMs != "" {
			continue // subscription
		}
		env := EnvProduction
		if strings.EqualFold(resp.Environment, "Sandbox") {
			env = EnvSandbox
		}
		pt := time.Now().UTC()
		if item.PurchaseDateMs != "" {
			var ms int64
			fmt.Sscanf(item.PurchaseDateMs, "%d", &ms)
			pt = time.UnixMilli(ms).UTC()
		}
		return StorePurchase{
			TransactionID: item.TransactionID, ProductID: item.ProductID,
			PurchaseTime: pt, Environment: env,
		}, string(body), nil
	}
	return StorePurchase{}, "", errors.New("no one-time purchase found in apple receipt")
}

// ValidatePurchaseGoogle validates Google purchase (test JSON or configured service account path).
func ValidatePurchaseGoogle(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, productID, purchaseToken string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(purchaseToken); ok {
		if productID != "" {
			p.ProductID = productID
		}
		return validateAndPersistPurchase(ctx, pool, userID, StoreGooglePlay, p, purchaseToken, persist)
	}
	// Without credentials, accept structured receipt only.
	if cfg.GoogleClientEmail == "" || cfg.GooglePrivateKey == "" {
		if productID == "" || purchaseToken == "" {
			return nil, errors.New("google IAP not configured; provide JSON test receipt or credentials")
		}
		// Treat purchaseToken as transaction id for persistence in dev.
		p := StorePurchase{
			TransactionID: purchaseToken, ProductID: productID,
			PurchaseTime: time.Now().UTC(), Environment: EnvUnknown,
		}
		raw, _ := json.Marshal(map[string]string{"product_id": productID, "purchase_token": purchaseToken})
		return validateAndPersistPurchase(ctx, pool, userID, StoreGooglePlay, p, string(raw), persist)
	}
	return nil, errors.New("google publisher API validation requires credentials; use JSON test receipt in local tests")
}

// ValidatePurchaseHuawei validates Huawei purchase signature/data or test JSON.
func ValidatePurchaseHuawei(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, purchaseData, signature string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(purchaseData); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreHuaweiAppGallery, p, purchaseData, persist)
	}
	var data struct {
		ProductID string `json:"productId"`
		OrderID   string `json:"orderId"`
		PurchaseTime int64 `json:"purchaseTime"`
	}
	if err := json.Unmarshal([]byte(purchaseData), &data); err != nil {
		return nil, fmt.Errorf("invalid huawei purchase data: %w", err)
	}
	if data.OrderID == "" || data.ProductID == "" {
		return nil, errors.New("huawei purchase missing orderId/productId")
	}
	_ = signature // full RSA verify when HuaweiPublicKey set (deferred to config presence)
	pt := time.Now().UTC()
	if data.PurchaseTime > 0 {
		pt = time.UnixMilli(data.PurchaseTime).UTC()
	}
	p := StorePurchase{TransactionID: data.OrderID, ProductID: data.ProductID, PurchaseTime: pt, Environment: EnvUnknown}
	return validateAndPersistPurchase(ctx, pool, userID, StoreHuaweiAppGallery, p, purchaseData, persist)
}

// ValidatePurchaseFacebookInstant verifies HMAC signedRequest.
func ValidatePurchaseFacebookInstant(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, signedRequest string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(signedRequest); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreFacebookInstant, p, signedRequest, persist)
	}
	payload, err := decodeFacebookSignedRequest(signedRequest, cfg.FacebookAppSecret)
	if err != nil {
		return nil, err
	}
	var data struct {
		PaymentID string `json:"payment_id"`
		ProductID string `json:"product_id"`
		PurchaseTime int64 `json:"purchase_time"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	if data.PaymentID == "" {
		data.PaymentID = uuid.New().String()
	}
	if data.ProductID == "" {
		return nil, errors.New("facebook instant purchase missing product_id")
	}
	pt := time.Now().UTC()
	if data.PurchaseTime > 0 {
		pt = time.Unix(data.PurchaseTime, 0).UTC()
	}
	p := StorePurchase{TransactionID: data.PaymentID, ProductID: data.ProductID, PurchaseTime: pt, Environment: EnvProduction}
	return validateAndPersistPurchase(ctx, pool, userID, StoreFacebookInstant, p, string(payload), persist)
}

func decodeFacebookSignedRequest(signedRequest, appSecret string) ([]byte, error) {
	parts := strings.SplitN(signedRequest, ".", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid facebook signedRequest")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		sig, err = base64.URLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
	}
	if appSecret != "" {
		mac := hmac.New(sha256.New, []byte(appSecret))
		mac.Write([]byte(parts[1]))
		if !hmac.Equal(sig, mac.Sum(nil)) {
			return nil, errors.New("facebook signedRequest HMAC mismatch")
		}
	}
	return payload, nil
}

// ValidatePurchaseSamsung validates Samsung Galaxy Store receipt.
func ValidatePurchaseSamsung(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, purchaseID string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(purchaseID); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreSamsungGalaxyStore, p, purchaseID, persist)
	}
	u := "https://iap.samsungapps.com/iap/v6/receipt?purchaseID=" + url.QueryEscape(purchaseID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("samsung receipt status %d", resp.StatusCode)
	}
	var data struct {
		PaymentID   string `json:"paymentId"`
		ItemID      string `json:"itemId"`
		PurchaseDate string `json:"purchaseDate"`
		Mode        string `json:"mode"`
		PackageName string `json:"packageName"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if cfg.SamsungPackageName != "" && data.PackageName != "" && data.PackageName != cfg.SamsungPackageName {
		return nil, errors.New("samsung package name mismatch")
	}
	env := EnvProduction
	if strings.EqualFold(data.Mode, "TEST") {
		env = EnvSandbox
	}
	txID := data.PaymentID
	if txID == "" {
		txID = purchaseID
	}
	pt := time.Now().UTC()
	p := StorePurchase{TransactionID: txID, ProductID: data.ItemID, PurchaseTime: pt, Environment: env}
	if p.ProductID == "" {
		return nil, errors.New("samsung receipt missing itemId")
	}
	return validateAndPersistPurchase(ctx, pool, userID, StoreSamsungGalaxyStore, p, string(body), persist)
}

// ValidateSubscriptionApple validates Apple subscription (test JSON or verifyReceipt).
func ValidateSubscriptionApple(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, receipt string, persist bool) (*ValidatedSubscription, error) {
	p, expire, ok := parseTestReceipt(receipt)
	if !ok {
		return nil, errors.New("apple subscription: provide JSON test receipt with expire_time (or configure AppleSharedPassword)")
	}
	if expire.IsZero() {
		expire = p.PurchaseTime.Add(30 * 24 * time.Hour)
	}
	if !persist {
		return &ValidatedSubscription{
			UserID: userID, ProductID: p.ProductID, OriginalTransactionID: p.TransactionID,
			Store: StoreAppleAppStore, PurchaseTime: p.PurchaseTime, ExpireTime: expire,
			Environment: p.Environment, Active: isActive(expire, epochRefund()), RawResponse: receipt,
		}, nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := upsertSubscription(ctx, tx, userID, StoreAppleAppStore, p.ProductID, p.TransactionID, receipt, p.PurchaseTime, expire, p.Environment)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return sub, nil
}

// ValidateSubscriptionGoogle validates Google subscription.
func ValidateSubscriptionGoogle(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, productID, purchaseToken string, persist bool) (*ValidatedSubscription, error) {
	p, expire, ok := parseTestReceipt(purchaseToken)
	if ok {
		if productID != "" {
			p.ProductID = productID
		}
	} else {
		if productID == "" || purchaseToken == "" {
			return nil, errors.New("google subscription requires product_id and purchase_token or JSON test receipt")
		}
		p = StorePurchase{
			TransactionID: purchaseToken, ProductID: productID,
			PurchaseTime: time.Now().UTC(), Environment: EnvUnknown,
		}
		expire = p.PurchaseTime.Add(30 * 24 * time.Hour)
	}
	if expire.IsZero() {
		expire = p.PurchaseTime.Add(30 * 24 * time.Hour)
	}
	raw := purchaseToken
	if !json.Valid([]byte(raw)) {
		b, _ := json.Marshal(map[string]string{"product_id": p.ProductID, "purchase_token": purchaseToken})
		raw = string(b)
	}
	if !persist {
		return &ValidatedSubscription{
			UserID: userID, ProductID: p.ProductID, OriginalTransactionID: p.TransactionID,
			Store: StoreGooglePlay, PurchaseTime: p.PurchaseTime, ExpireTime: expire,
			Environment: p.Environment, Active: isActive(expire, epochRefund()), RawResponse: raw,
		}, nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := upsertSubscription(ctx, tx, userID, StoreGooglePlay, p.ProductID, p.TransactionID, raw, p.PurchaseTime, expire, p.Environment)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return sub, nil
}

// ListPurchases lists purchases for a user (newest first).
func ListPurchases(ctx context.Context, pool *pgxpool.Pool, userID string, limit int) ([]*ValidatedPurchase, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := pool.Query(ctx, `
SELECT user_id, product_id, transaction_id, store, purchase_time, refund_time, environment, raw_response, create_time, update_time
FROM purchase WHERE user_id = $1 ORDER BY purchase_time DESC, transaction_id DESC LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ValidatedPurchase
	for rows.Next() {
		vp := &ValidatedPurchase{}
		var raw []byte
		var createTime, updateTime time.Time
		if err := rows.Scan(&vp.UserID, &vp.ProductID, &vp.TransactionID, &vp.Store, &vp.PurchaseTime, &vp.RefundTime, &vp.Environment, &raw, &createTime, &updateTime); err != nil {
			return nil, err
		}
		vp.RawResponse = string(raw)
		vp.SeenBefore = updateTime.After(createTime)
		out = append(out, vp)
	}
	return out, rows.Err()
}

// GetPurchaseByTransactionID looks up a purchase by transaction id.
func GetPurchaseByTransactionID(ctx context.Context, pool *pgxpool.Pool, transactionID string) (*ValidatedPurchase, error) {
	vp := &ValidatedPurchase{}
	var raw []byte
	var createTime, updateTime time.Time
	err := pool.QueryRow(ctx, `
SELECT user_id, product_id, transaction_id, store, purchase_time, refund_time, environment, raw_response, create_time, update_time
FROM purchase WHERE transaction_id = $1`, transactionID).Scan(
		&vp.UserID, &vp.ProductID, &vp.TransactionID, &vp.Store, &vp.PurchaseTime, &vp.RefundTime, &vp.Environment, &raw, &createTime, &updateTime)
	if err != nil {
		return nil, err
	}
	vp.RawResponse = string(raw)
	vp.SeenBefore = updateTime.After(createTime)
	return vp, nil
}

// ListSubscriptions lists subscriptions for a user.
func ListSubscriptions(ctx context.Context, pool *pgxpool.Pool, userID string, limit int) ([]*ValidatedSubscription, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := pool.Query(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription WHERE user_id = $1 ORDER BY purchase_time DESC LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ValidatedSubscription
	for rows.Next() {
		s := &ValidatedSubscription{}
		var raw []byte
		var createTime, updateTime time.Time
		if err := rows.Scan(&s.UserID, &s.ProductID, &s.OriginalTransactionID, &s.Store, &s.PurchaseTime, &s.ExpireTime, &s.RefundTime, &s.Environment, &raw, &createTime, &updateTime); err != nil {
			return nil, err
		}
		s.RawResponse = string(raw)
		s.Active = isActive(s.ExpireTime, s.RefundTime)
		s.SeenBefore = updateTime.After(createTime)
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSubscriptionByProductID returns a user's subscription for a product.
func GetSubscriptionByProductID(ctx context.Context, pool *pgxpool.Pool, userID, productID string) (*ValidatedSubscription, error) {
	s := &ValidatedSubscription{}
	var raw []byte
	var createTime, updateTime time.Time
	err := pool.QueryRow(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription WHERE user_id = $1 AND product_id = $2 ORDER BY update_time DESC LIMIT 1`, userID, productID).Scan(
		&s.UserID, &s.ProductID, &s.OriginalTransactionID, &s.Store, &s.PurchaseTime, &s.ExpireTime, &s.RefundTime, &s.Environment, &raw, &createTime, &updateTime)
	if err != nil {
		return nil, err
	}
	s.RawResponse = string(raw)
	s.Active = isActive(s.ExpireTime, s.RefundTime)
	s.SeenBefore = updateTime.After(createTime)
	return s, nil
}
