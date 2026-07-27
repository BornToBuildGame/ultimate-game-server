package iap

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// InAppPurchaseDataHuawei is the Huawei InAppPurchaseData JSON.
type InAppPurchaseDataHuawei struct {
	ApplicationID string `json:"applicationId"`
	AutoRenewing  bool   `json:"autoRenewing"`
	OrderID       string `json:"orderId"`
	Kind          int    `json:"kind"`
	PackageName   string `json:"packageName"`
	ProductID     string `json:"productId"`
	PurchaseTime  int64  `json:"purchaseTime"`
	PurchaseToken string `json:"purchaseToken"`
	AccountFlag   int    `json:"accountFlag"`
	PurchaseType  int    `json:"purchaseType"`
}

// ValidateReceiptHuaweiResponse is the Huawei verify response.
type ValidateReceiptHuaweiResponse struct {
	ResponseCode      string                  `json:"responseCode"`
	ResponseMessage   string                  `json:"responseMessage"`
	PurchaseTokenData InAppPurchaseDataHuawei `json:"purchaseTokenData"`
	DataSignature     string                  `json:"dataSignature"`
}

type accessTokenHuawei struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	ExpiredAt   int64  `json:"-"`
	sync.RWMutex
}

func (at *accessTokenHuawei) Expired() bool {
	return at.ExpiredAt-accessTokenExpiresGracePeriod <= time.Now().Unix()
}

var cachedTokenHuawei accessTokenHuawei

func getHuaweiAccessToken(ctx context.Context, httpc *http.Client, clientID, clientSecret string) (string, error) {
	const authURL = "https://oauth-login.cloud.huawei.com/oauth2/v3/token"

	cachedTokenHuawei.RLock()
	if cachedTokenHuawei.AccessToken != "" && !cachedTokenHuawei.Expired() {
		tok := cachedTokenHuawei.AccessToken
		cachedTokenHuawei.RUnlock()
		return tok, nil
	}
	cachedTokenHuawei.RUnlock()

	cachedTokenHuawei.Lock()
	defer cachedTokenHuawei.Unlock()
	if cachedTokenHuawei.AccessToken != "" && !cachedTokenHuawei.Expired() {
		return cachedTokenHuawei.AccessToken, nil
	}

	urlValue := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {clientSecret}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, strings.NewReader(urlValue.Encode()))
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
		return "", &ValidationError{Err: errors.New("non-200 response from Huawei auth"), StatusCode: resp.StatusCode, Payload: string(buf)}
	}
	var out accessTokenHuawei
	if err := json.Unmarshal(buf, &out); err != nil {
		return "", err
	}
	out.ExpiredAt = time.Now().Unix() + out.ExpiresIn
	cachedTokenHuawei.AccessToken = out.AccessToken
	cachedTokenHuawei.ExpiresIn = out.ExpiresIn
	cachedTokenHuawei.ExpiredAt = out.ExpiredAt
	return out.AccessToken, nil
}

// VerifySignatureHuawei validates the InAppPurchaseData RSA signature.
func VerifySignatureHuawei(base64EncodedPublicKey, data, signature string) error {
	publicKeyByte, err := base64.StdEncoding.DecodeString(base64EncodedPublicKey)
	if err != nil {
		return err
	}
	pub, err := x509.ParsePKIXPublicKey(publicKeyByte)
	if err != nil {
		return err
	}
	hashed := sha256.Sum256([]byte(data))
	signatureByte, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return err
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return errors.New("huawei public key is not RSA")
	}
	return rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, hashed[:], signatureByte)
}

// ValidateReceiptHuawei verifies RSA signature and calls Huawei purchase token verify API.
func ValidateReceiptHuawei(ctx context.Context, httpc *http.Client, pubKey, clientID, clientSecret, purchaseData, signature string) (*ValidateReceiptHuaweiResponse, *InAppPurchaseDataHuawei, []byte, error) {
	if purchaseData == "" {
		return nil, nil, nil, errors.New("'purchaseData' must not be empty")
	}
	if signature == "" {
		return nil, nil, nil, errors.New("'signature' must not be empty")
	}
	data := &InAppPurchaseDataHuawei{PurchaseType: -1}
	if err := json.Unmarshal([]byte(purchaseData), data); err != nil {
		return nil, nil, nil, err
	}
	if pubKey != "" {
		if err := VerifySignatureHuawei(pubKey, purchaseData, signature); err != nil {
			return nil, nil, nil, ErrInvalidSignatureHuawei
		}
	}
	if clientID == "" || clientSecret == "" {
		return nil, data, []byte(purchaseData), nil
	}
	token, err := getHuaweiAccessToken(ctx, httpc, clientID, clientSecret)
	if err != nil {
		return nil, nil, nil, err
	}
	host := "orders-dre.iap.hicloud.com"
	if data.AccountFlag == 1 {
		host = "orders-at-dre.iap.dbankcloud.com"
	}
	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   "/applications/purchases/tokens/verify",
	}
	reqBody, err := json.Marshal(map[string]string{
		"purchaseToken": data.PurchaseToken,
		"productId":     data.ProductID,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := httpc.Do(req)
	if err != nil {
		return nil, nil, nil, err
	}
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, data, nil, err
	}
	if res.StatusCode != 200 {
		return nil, nil, nil, &ValidationError{Err: ErrNon200ServiceHuawei, StatusCode: res.StatusCode, Payload: string(buf)}
	}
	out := &ValidateReceiptHuaweiResponse{}
	if err := json.Unmarshal(buf, out); err != nil {
		return nil, data, nil, err
	}
	if out.ResponseCode != "" && out.ResponseCode != "0" {
		return nil, nil, nil, fmt.Errorf("huawei verify responseCode %s: %s", out.ResponseCode, out.ResponseMessage)
	}
	return out, data, buf, nil
}
