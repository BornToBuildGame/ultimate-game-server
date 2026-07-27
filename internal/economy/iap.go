package economy

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"ultimate-game-server/internal/iap"

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
	AppleSharedPassword           string
	AppleNotificationsEndpointID  string
	GoogleClientEmail             string
	GooglePrivateKey              string
	GooglePackageName             string
	GoogleNotificationsEndpointID string
	HuaweiPublicKey               string
	HuaweiClientID                string
	HuaweiClientSecret            string
	FacebookAppSecret             string
	SamsungPackageName            string
}

// DefaultIAPConfig is set at server startup for runtime module helpers.
var DefaultIAPConfig IAPConfig

// LoadIAPConfigFromEnv reads IAP credentials from environment variables.
// Empty values leave the corresponding field unset (dev/test-receipt paths still work).
func LoadIAPConfigFromEnv() IAPConfig {
	return IAPConfig{
		AppleSharedPassword:           os.Getenv("APPLE_SHARED_PASSWORD"),
		AppleNotificationsEndpointID:  firstEnv("IAP_APPLE_NOTIFICATIONS_ENDPOINT_ID", "APPLE_NOTIFICATIONS_ENDPOINT_ID"),
		GoogleClientEmail:             os.Getenv("GOOGLE_IAP_CLIENT_EMAIL"),
		GooglePrivateKey:              os.Getenv("GOOGLE_IAP_PRIVATE_KEY"),
		GooglePackageName:             os.Getenv("GOOGLE_IAP_PACKAGE_NAME"),
		GoogleNotificationsEndpointID: firstEnv("IAP_GOOGLE_NOTIFICATIONS_ENDPOINT_ID", "GOOGLE_NOTIFICATIONS_ENDPOINT_ID"),
		HuaweiPublicKey:               os.Getenv("HUAWEI_IAP_PUBLIC_KEY"),
		HuaweiClientID:                os.Getenv("HUAWEI_CLIENT_ID"),
		HuaweiClientSecret:            os.Getenv("HUAWEI_CLIENT_SECRET"),
		FacebookAppSecret:             os.Getenv("FACEBOOK_APP_SECRET"),
		SamsungPackageName:            os.Getenv("SAMSUNG_PACKAGE_NAME"),
	}
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// ValidatedPurchase is a persisted or validated one-time purchase.
type ValidatedPurchase struct {
	UserID        string
	ProductID     string
	TransactionID string
	Store         int
	PurchaseTime  time.Time
	CreateTime    time.Time
	UpdateTime    time.Time
	RefundTime    time.Time
	Environment   int
	SeenBefore    bool
	RawResponse   string
}

// ValidatedSubscription is a persisted subscription.
type ValidatedSubscription struct {
	UserID                string
	ProductID             string
	OriginalTransactionID string
	Store                 int
	PurchaseTime          time.Time
	ExpireTime            time.Time
	CreateTime            time.Time
	UpdateTime            time.Time
	RefundTime            time.Time
	Environment           int
	Active                bool
	SeenBefore            bool
	RawResponse           string
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
		Store: store, PurchaseTime: purchaseTime, CreateTime: createTime, UpdateTime: updateTime,
		RefundTime: refund, Environment: p.Environment, SeenBefore: seen, RawResponse: rawResponse,
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
		Store: store, PurchaseTime: purchaseTime, ExpireTime: outExpire,
		CreateTime: createTime, UpdateTime: updateTime, RefundTime: outRefund,
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

// ValidatePurchaseApple validates Apple receipt (JWS, legacy verifyReceipt, or JSON test receipt).
func ValidatePurchaseApple(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, receipt string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(receipt); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreAppleAppStore, p, receipt, persist)
	}
	if iap.IsJWS(receipt) {
		tx, err := iap.ParseAppleJWSTransaction(receipt)
		if err != nil {
			return nil, err
		}
		if tx.ExpiresDate != 0 {
			return nil, errors.New("subscription receipt: use ValidateSubscriptionApple")
		}
		env := EnvProduction
		if strings.EqualFold(tx.Environment, iap.AppleSandboxEnvironment) {
			env = EnvSandbox
		}
		pt := time.Now().UTC()
		if tx.PurchaseDate > 0 {
			pt = time.UnixMilli(tx.PurchaseDate).UTC()
		}
		p := StorePurchase{
			TransactionID: tx.TransactionID, ProductID: tx.ProductID,
			PurchaseTime: pt, Environment: env,
		}
		raw, _ := json.Marshal(map[string]string{"jws": receipt})
		return validateAndPersistPurchase(ctx, pool, userID, StoreAppleAppStore, p, string(raw), persist)
	}
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
	return nil, errors.New("apple IAP not configured; provide JSON test receipt, JWS, or AppleSharedPassword")
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

// ValidatePurchaseGoogle validates Google purchase (test JSON, Publisher API, or dev token).
func ValidatePurchaseGoogle(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, productID, purchaseToken string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(purchaseToken); ok {
		if productID != "" {
			p.ProductID = productID
		}
		return validateAndPersistPurchase(ctx, pool, userID, StoreGooglePlay, p, purchaseToken, persist)
	}
	if cfg.GoogleClientEmail != "" && cfg.GooglePrivateKey != "" {
		pkg := cfg.GooglePackageName
		pid := productID
		token := purchaseToken
		if gr, err := iap.DecodeReceiptGoogle(purchaseToken); err == nil {
			pkg = gr.PackageName
			if pid == "" {
				pid = gr.ProductID
			}
			token = gr.PurchaseToken
		}
		if pkg == "" {
			return nil, errors.New("google IAP requires GOOGLE_IAP_PACKAGE_NAME or JSON receipt with packageName")
		}
		if pid == "" || token == "" {
			return nil, errors.New("google product_id and purchase_token required")
		}
		resp, raw, err := iap.ValidateProductGoogle(ctx, iap.HTTPClient, cfg.GoogleClientEmail, cfg.GooglePrivateKey, pkg, pid, token)
		if err != nil {
			return nil, err
		}
		env := EnvProduction
		if resp.PurchaseType == 0 {
			env = EnvSandbox
		}
		txID := resp.OrderID
		if txID == "" {
			txID = token
		}
		pt := time.Now().UTC()
		if resp.PurchaseTimeMillis != "" {
			var ms int64
			fmt.Sscanf(resp.PurchaseTimeMillis, "%d", &ms)
			if ms > 0 {
				pt = time.UnixMilli(ms).UTC()
			}
		}
		p := StorePurchase{TransactionID: txID, ProductID: pid, PurchaseTime: pt, Environment: env}
		return validateAndPersistPurchase(ctx, pool, userID, StoreGooglePlay, p, string(raw), persist)
	}
	if productID == "" || purchaseToken == "" {
		return nil, errors.New("google IAP not configured; provide JSON test receipt or credentials")
	}
	// Dev path: treat purchaseToken as transaction id without Publisher API.
	p := StorePurchase{
		TransactionID: purchaseToken, ProductID: productID,
		PurchaseTime: time.Now().UTC(), Environment: EnvUnknown,
	}
	raw, _ := json.Marshal(map[string]string{"product_id": productID, "purchase_token": purchaseToken})
	return validateAndPersistPurchase(ctx, pool, userID, StoreGooglePlay, p, string(raw), persist)
}

// ValidatePurchaseHuawei validates Huawei purchase signature/data or test JSON.
func ValidatePurchaseHuawei(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, purchaseData, signature string, persist bool) (*ValidatedPurchase, error) {
	if p, _, ok := parseTestReceipt(purchaseData); ok {
		return validateAndPersistPurchase(ctx, pool, userID, StoreHuaweiAppGallery, p, purchaseData, persist)
	}
	if cfg.HuaweiPublicKey != "" || (cfg.HuaweiClientID != "" && cfg.HuaweiClientSecret != "") {
		_, data, raw, err := iap.ValidateReceiptHuawei(ctx, iap.HTTPClient, cfg.HuaweiPublicKey, cfg.HuaweiClientID, cfg.HuaweiClientSecret, purchaseData, signature)
		if err != nil {
			return nil, err
		}
		if data.OrderID == "" || data.ProductID == "" {
			return nil, errors.New("huawei purchase missing orderId/productId")
		}
		pt := time.Now().UTC()
		if data.PurchaseTime > 0 {
			pt = time.UnixMilli(data.PurchaseTime).UTC()
		}
		env := EnvProduction
		if data.PurchaseType == 0 {
			env = EnvSandbox
		}
		p := StorePurchase{TransactionID: data.OrderID, ProductID: data.ProductID, PurchaseTime: pt, Environment: env}
		rawStr := purchaseData
		if len(raw) > 0 {
			rawStr = string(raw)
		}
		return validateAndPersistPurchase(ctx, pool, userID, StoreHuaweiAppGallery, p, rawStr, persist)
	}
	var data struct {
		ProductID    string `json:"productId"`
		OrderID      string `json:"orderId"`
		PurchaseTime int64  `json:"purchaseTime"`
	}
	if err := json.Unmarshal([]byte(purchaseData), &data); err != nil {
		return nil, fmt.Errorf("invalid huawei purchase data: %w", err)
	}
	if data.OrderID == "" || data.ProductID == "" {
		return nil, errors.New("huawei purchase missing orderId/productId")
	}
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

// ValidateSubscriptionApple validates Apple subscription (test JSON, JWS, or verifyReceipt).
func ValidateSubscriptionApple(ctx context.Context, pool *pgxpool.Pool, cfg IAPConfig, userID, receipt string, persist bool) (*ValidatedSubscription, error) {
	var productID, originalTxID, raw string
	var purchaseTime, expireTime time.Time
	var env int

	if p, expire, ok := parseTestReceipt(receipt); ok {
		productID = p.ProductID
		originalTxID = p.TransactionID
		purchaseTime = p.PurchaseTime
		expireTime = expire
		env = p.Environment
		raw = receipt
		if expireTime.IsZero() {
			expireTime = purchaseTime.Add(30 * 24 * time.Hour)
		}
	} else if iap.IsJWS(receipt) {
		tx, err := iap.ParseAppleJWSTransaction(receipt)
		if err != nil {
			return nil, err
		}
		if tx.ExpiresDate == 0 {
			return nil, errors.New("one-time purchase receipt: use ValidatePurchaseApple")
		}
		productID = tx.ProductID
		originalTxID = tx.OriginalTransactionID
		if originalTxID == "" {
			originalTxID = tx.TransactionID
		}
		purchaseTime = time.UnixMilli(tx.PurchaseDate).UTC()
		expireTime = time.UnixMilli(tx.ExpiresDate).UTC()
		env = EnvProduction
		if strings.EqualFold(tx.Environment, iap.AppleSandboxEnvironment) {
			env = EnvSandbox
		}
		b, _ := json.Marshal(map[string]string{"jws": receipt})
		raw = string(b)
	} else if cfg.AppleSharedPassword != "" {
		resp, body, err := iap.ValidateLegacyReceiptApple(ctx, iap.HTTPClient, cfg.AppleSharedPassword, receipt)
		if err != nil {
			return nil, err
		}
		if resp.Status != 0 {
			return nil, fmt.Errorf("apple verifyReceipt status %d", resp.Status)
		}
		env = EnvProduction
		if strings.EqualFold(resp.Environment, iap.AppleSandboxEnvironment) {
			env = EnvSandbox
		}
		found := false
		for _, item := range resp.LatestReceiptInfo {
			if item.ExpiresDateMs == "" {
				continue
			}
			productID = item.ProductID
			originalTxID = item.OriginalTransactionID
			if originalTxID == "" {
				originalTxID = item.TransactionID
			}
			purchaseTime = iap.ParseMsTimestamp(item.PurchaseDateMs)
			expireTime = iap.ParseMsTimestamp(item.ExpiresDateMs)
			found = true
			break
		}
		if !found && resp.Receipt != nil {
			for _, item := range resp.Receipt.InApp {
				if item.ExpiresDateMs == "" {
					continue
				}
				productID = item.ProductID
				originalTxID = item.OriginalTransactionID
				if originalTxID == "" {
					originalTxID = item.TransactionID
				}
				purchaseTime = iap.ParseMsTimestamp(item.PurchaseDateMs)
				expireTime = iap.ParseMsTimestamp(item.ExpiresDateMs)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("no subscription found in apple receipt")
		}
		raw = string(body)
	} else {
		return nil, errors.New("apple subscription: provide JSON test receipt, JWS, or AppleSharedPassword")
	}

	if !persist {
		return &ValidatedSubscription{
			UserID: userID, ProductID: productID, OriginalTransactionID: originalTxID,
			Store: StoreAppleAppStore, PurchaseTime: purchaseTime, ExpireTime: expireTime,
			Environment: env, Active: isActive(expireTime, epochRefund()), RawResponse: raw,
		}, nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := upsertSubscription(ctx, tx, userID, StoreAppleAppStore, productID, originalTxID, raw, purchaseTime, expireTime, env)
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
	var p StorePurchase
	var expire time.Time
	var raw string

	if parsed, et, ok := parseTestReceipt(purchaseToken); ok {
		p = parsed
		if productID != "" {
			p.ProductID = productID
		}
		expire = et
		raw = purchaseToken
	} else if cfg.GoogleClientEmail != "" && cfg.GooglePrivateKey != "" {
		pkg := cfg.GooglePackageName
		token := purchaseToken
		pid := productID
		if gr, err := iap.DecodeReceiptGoogle(purchaseToken); err == nil {
			pkg = gr.PackageName
			token = gr.PurchaseToken
			if pid == "" {
				pid = gr.ProductID
			}
		}
		if pkg == "" {
			return nil, errors.New("google subscription requires GOOGLE_IAP_PACKAGE_NAME or JSON receipt with packageName")
		}
		resp, body, err := iap.ValidateSubscriptionGoogle(ctx, iap.HTTPClient, cfg.GoogleClientEmail, cfg.GooglePrivateKey, pkg, token)
		if err != nil {
			return nil, err
		}
		if len(resp.LineItems) == 0 {
			return nil, errors.New("google subscription response missing lineItems")
		}
		item := resp.LineItems[0]
		if pid == "" {
			pid = item.ProductID
		}
		p = StorePurchase{
			TransactionID: token, ProductID: pid,
			PurchaseTime: resp.StartTime, Environment: EnvProduction,
		}
		if p.PurchaseTime.IsZero() {
			p.PurchaseTime = time.Now().UTC()
		}
		expire = item.ExpiryTime
		raw = string(body)
	} else {
		if productID == "" || purchaseToken == "" {
			return nil, errors.New("google subscription requires product_id and purchase_token or JSON test receipt")
		}
		p = StorePurchase{
			TransactionID: purchaseToken, ProductID: productID,
			PurchaseTime: time.Now().UTC(), Environment: EnvUnknown,
		}
		expire = p.PurchaseTime.Add(30 * 24 * time.Hour)
		b, _ := json.Marshal(map[string]string{"product_id": p.ProductID, "purchase_token": purchaseToken})
		raw = string(b)
	}
	if expire.IsZero() {
		expire = p.PurchaseTime.Add(30 * 24 * time.Hour)
	}
	if raw == "" {
		raw = purchaseToken
	}
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
		if err := rows.Scan(&vp.UserID, &vp.ProductID, &vp.TransactionID, &vp.Store, &vp.PurchaseTime, &vp.RefundTime, &vp.Environment, &raw, &vp.CreateTime, &vp.UpdateTime); err != nil {
			return nil, err
		}
		vp.RawResponse = string(raw)
		vp.SeenBefore = vp.UpdateTime.After(vp.CreateTime)
		out = append(out, vp)
	}
	return out, rows.Err()
}

// GetPurchaseByTransactionID looks up a purchase by transaction id.
func GetPurchaseByTransactionID(ctx context.Context, pool *pgxpool.Pool, transactionID string) (*ValidatedPurchase, error) {
	vp := &ValidatedPurchase{}
	var raw []byte
	err := pool.QueryRow(ctx, `
SELECT user_id, product_id, transaction_id, store, purchase_time, refund_time, environment, raw_response, create_time, update_time
FROM purchase WHERE transaction_id = $1`, transactionID).Scan(
		&vp.UserID, &vp.ProductID, &vp.TransactionID, &vp.Store, &vp.PurchaseTime, &vp.RefundTime, &vp.Environment, &raw, &vp.CreateTime, &vp.UpdateTime)
	if err != nil {
		return nil, err
	}
	vp.RawResponse = string(raw)
	vp.SeenBefore = vp.UpdateTime.After(vp.CreateTime)
	return vp, nil
}

var ErrSubscriptionsListInvalidCursor = errors.New("subscriptions list cursor invalid")

type subscriptionsListCursor struct {
	UserID                string
	PurchaseTime          time.Time
	OriginalTransactionID string
	IsNext                bool
}

type SubscriptionListResult struct {
	Subscriptions []*ValidatedSubscription
	Cursor        string
	PrevCursor    string
}

func encodeSubCursor(c *subscriptionsListCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeSubCursor(cursor string) (*subscriptionsListCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, ErrSubscriptionsListInvalidCursor
		}
	}
	out := &subscriptionsListCursor{}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(out); err != nil {
		return nil, ErrSubscriptionsListInvalidCursor
	}
	return out, nil
}

// ListSubscriptions lists subscriptions for a user with optional gob cursor pagination.
func ListSubscriptions(ctx context.Context, pool *pgxpool.Pool, userID string, limit int, cursor string) (*SubscriptionListResult, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}
	incoming, err := decodeSubCursor(cursor)
	if err != nil {
		return nil, err
	}
	if incoming != nil && incoming.UserID != "" && incoming.UserID != userID {
		return nil, ErrSubscriptionsListInvalidCursor
	}

	fetch := limit + 1
	var rows pgx.Rows
	if incoming != nil {
		rows, err = pool.Query(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription
WHERE user_id = $1 AND (purchase_time, original_transaction_id) < ($2::TIMESTAMPTZ, $3)
ORDER BY purchase_time DESC, original_transaction_id DESC LIMIT $4`,
			userID, incoming.PurchaseTime, incoming.OriginalTransactionID, fetch)
	} else {
		rows, err = pool.Query(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription WHERE user_id = $1 ORDER BY purchase_time DESC, original_transaction_id DESC LIMIT $2`, userID, fetch)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ValidatedSubscription
	for rows.Next() {
		s := &ValidatedSubscription{}
		var raw []byte
		if err := rows.Scan(&s.UserID, &s.ProductID, &s.OriginalTransactionID, &s.Store, &s.PurchaseTime, &s.ExpireTime, &s.RefundTime, &s.Environment, &raw, &s.CreateTime, &s.UpdateTime); err != nil {
			return nil, err
		}
		s.RawResponse = string(raw)
		s.Active = isActive(s.ExpireTime, s.RefundTime)
		s.SeenBefore = s.UpdateTime.After(s.CreateTime)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &SubscriptionListResult{Subscriptions: out}
	if len(out) > limit {
		last := out[limit-1]
		result.Subscriptions = out[:limit]
		next, err := encodeSubCursor(&subscriptionsListCursor{
			UserID: userID, PurchaseTime: last.PurchaseTime, OriginalTransactionID: last.OriginalTransactionID, IsNext: true,
		})
		if err != nil {
			return nil, err
		}
		result.Cursor = next
	}
	if incoming != nil && len(result.Subscriptions) > 0 {
		first := result.Subscriptions[0]
		prev, err := encodeSubCursor(&subscriptionsListCursor{
			UserID: userID, PurchaseTime: first.PurchaseTime, OriginalTransactionID: first.OriginalTransactionID, IsNext: false,
		})
		if err != nil {
			return nil, err
		}
		result.PrevCursor = prev
	}
	return result, nil
}

// GetSubscriptionByProductID returns a user's subscription for a product.
func GetSubscriptionByProductID(ctx context.Context, pool *pgxpool.Pool, userID, productID string) (*ValidatedSubscription, error) {
	s := &ValidatedSubscription{}
	var raw []byte
	err := pool.QueryRow(ctx, `
SELECT user_id, product_id, original_transaction_id, store, purchase_time, expire_time, refund_time, environment, raw_response, create_time, update_time
FROM subscription WHERE user_id = $1 AND product_id = $2 ORDER BY update_time DESC LIMIT 1`, userID, productID).Scan(
		&s.UserID, &s.ProductID, &s.OriginalTransactionID, &s.Store, &s.PurchaseTime, &s.ExpireTime, &s.RefundTime, &s.Environment, &raw, &s.CreateTime, &s.UpdateTime)
	if err != nil {
		return nil, err
	}
	s.RawResponse = string(raw)
	s.Active = isActive(s.ExpireTime, s.RefundTime)
	s.SeenBefore = s.UpdateTime.After(s.CreateTime)
	return s, nil
}
