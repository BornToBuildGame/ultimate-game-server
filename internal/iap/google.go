package iap

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func init() {
	jwt.MarshalSingleStringAsArray = false
}

// ReceiptGoogle is the client purchase JSON used to call the Publisher API.
type ReceiptGoogle struct {
	OrderID       string `json:"orderId"`
	PackageName   string `json:"packageName"`
	ProductID     string `json:"productId"`
	PurchaseState int    `json:"purchaseState"`
	PurchaseTime  int64  `json:"purchaseTime"`
	PurchaseToken string `json:"purchaseToken"`
}

// ValidateReceiptGoogleResponse is the Android Publisher product purchase response.
type ValidateReceiptGoogleResponse struct {
	AcknowledgementState int    `json:"acknowledgementState"`
	ConsumptionState     int    `json:"consumptionState"`
	Kind                 string `json:"kind"`
	OrderID              string `json:"orderId"`
	PurchaseState        int    `json:"purchaseState"`
	PurchaseTimeMillis   string `json:"purchaseTimeMillis"`
	PurchaseType         int    `json:"purchaseType"`
	RegionCode           string `json:"regionCode"`
}

// SubscriptionV2GoogleResponse is a minimal subscriptions.v2 response.
type SubscriptionV2GoogleResponse struct {
	LineItems []struct {
		ProductID  string    `json:"productId"`
		ExpiryTime time.Time `json:"expiryTime"`
	} `json:"lineItems"`
	StartTime time.Time `json:"startTime"`
}

type accessTokenGoogle struct {
	AccessToken string    `json:"access_token"`
	ExpiresIn   int       `json:"expires_in"`
	fetchedAt   time.Time
}

func (at *accessTokenGoogle) Expired() bool {
	return at.fetchedAt.Add(time.Duration(at.ExpiresIn)*time.Second - accessTokenExpiresGracePeriod*time.Second).Before(time.Now())
}

var cachedTokensGoogle = &struct {
	sync.RWMutex
	tokenMap map[string]*accessTokenGoogle
}{tokenMap: make(map[string]*accessTokenGoogle)}

// DecodeReceiptGoogle unwraps client receipt JSON (optionally wrapped in {"json":"..."}).
func DecodeReceiptGoogle(receipt string) (*ReceiptGoogle, error) {
	var wrapper map[string]interface{}
	if err := json.Unmarshal([]byte(receipt), &wrapper); err != nil {
		return nil, err
	}
	unwrapped, ok := wrapper["json"].(string)
	if !ok {
		unwrapped = receipt
	}
	var gr ReceiptGoogle
	if err := json.Unmarshal([]byte(unwrapped), &gr); err != nil {
		return nil, errors.New("receipt is malformed")
	}
	if gr.PackageName == "" {
		return nil, errors.New("receipt is malformed")
	}
	return &gr, nil
}

func getGoogleAccessToken(ctx context.Context, httpc *http.Client, email, privateKey string) (string, error) {
	const authURL = "https://accounts.google.com/o/oauth2/token"

	cachedTokensGoogle.RLock()
	if cacheToken, found := cachedTokensGoogle.tokenMap[email]; found && cacheToken.AccessToken != "" && !cacheToken.Expired() {
		cachedTokensGoogle.RUnlock()
		return cacheToken.AccessToken, nil
	}
	cachedTokensGoogle.RUnlock()

	cachedTokensGoogle.Lock()
	defer cachedTokensGoogle.Unlock()
	if cacheToken, found := cachedTokensGoogle.tokenMap[email]; found && cacheToken.AccessToken != "" && !cacheToken.Expired() {
		return cacheToken.AccessToken, nil
	}

	type GoogleClaims struct {
		Scope string `json:"scope,omitempty"`
		jwt.RegisteredClaims
	}
	now := time.Now()
	claims := &GoogleClaims{
		"https://www.googleapis.com/auth/androidpublisher",
		jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{authURL},
			ExpiresAt: jwt.NewNumericDate(now.Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    email,
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		// Allow escaped newlines from env vars.
		pk := strings.ReplaceAll(privateKey, `\n`, "\n")
		block, _ = pem.Decode([]byte(pk))
		if block == nil {
			return "", errors.New("google iap private key invalid")
		}
	}
	pk, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}
	signed, err := token.SignedString(pk)
	if err != nil {
		return "", err
	}
	data := url.Values{}
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	data.Set("assertion", signed)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := httpc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", &ValidationError{Err: errors.New("non-200 response from Google auth"), StatusCode: resp.StatusCode, Payload: string(buf)}
	}
	var newToken accessTokenGoogle
	if err := json.Unmarshal(buf, &newToken); err != nil {
		return "", err
	}
	newToken.fetchedAt = time.Now()
	cachedTokensGoogle.tokenMap[email] = &newToken
	return newToken.AccessToken, nil
}

// ValidateProductGoogle calls Android Publisher purchases.products.get.
func ValidateProductGoogle(ctx context.Context, httpc *http.Client, clientEmail, privateKey, packageName, productID, purchaseToken string) (*ValidateReceiptGoogleResponse, []byte, error) {
	if clientEmail == "" || privateKey == "" {
		return nil, nil, errors.New("google IAP credentials required")
	}
	if packageName == "" || productID == "" || purchaseToken == "" {
		return nil, nil, errors.New("google package_name, product_id, and purchase_token required")
	}
	token, err := getGoogleAccessToken(ctx, httpc, clientEmail, privateKey)
	if err != nil {
		return nil, nil, err
	}
	u := &url.URL{
		Scheme:   "https",
		Host:     "androidpublisher.googleapis.com",
		Path:     fmt.Sprintf("androidpublisher/v3/applications/%s/purchases/products/%s/tokens/%s", packageName, productID, purchaseToken),
		RawQuery: "access_token=" + url.QueryEscape(token),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, &ValidationError{Err: ErrNon200ServiceGoogle, StatusCode: resp.StatusCode, Payload: string(buf)}
	}
	out := &ValidateReceiptGoogleResponse{PurchaseType: -1}
	if err := json.Unmarshal(buf, out); err != nil {
		return nil, nil, err
	}
	return out, buf, nil
}

// ValidateSubscriptionGoogle calls Android Publisher purchases.subscriptionsv2.get.
func ValidateSubscriptionGoogle(ctx context.Context, httpc *http.Client, clientEmail, privateKey, packageName, purchaseToken string) (*SubscriptionV2GoogleResponse, []byte, error) {
	if clientEmail == "" || privateKey == "" {
		return nil, nil, errors.New("google IAP credentials required")
	}
	if packageName == "" || purchaseToken == "" {
		return nil, nil, errors.New("google package_name and purchase_token required")
	}
	token, err := getGoogleAccessToken(ctx, httpc, clientEmail, privateKey)
	if err != nil {
		return nil, nil, err
	}
	u := &url.URL{
		Scheme:   "https",
		Host:     "androidpublisher.googleapis.com",
		Path:     fmt.Sprintf("androidpublisher/v3/applications/%s/purchases/subscriptionsv2/tokens/%s", packageName, purchaseToken),
		RawQuery: "access_token=" + url.QueryEscape(token),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, &ValidationError{Err: ErrNon200ServiceGoogle, StatusCode: resp.StatusCode, Payload: string(buf)}
	}
	out := &SubscriptionV2GoogleResponse{}
	if err := json.Unmarshal(buf, out); err != nil {
		return nil, nil, err
	}
	return out, buf, nil
}
