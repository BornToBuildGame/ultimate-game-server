package iap

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const AppleRootPEM = `
-----BEGIN CERTIFICATE-----
MIICQzCCAcmgAwIBAgIILcX8iNLFS5UwCgYIKoZIzj0EAwMwZzEbMBkGA1UEAwwS
QXBwbGUgUm9vdCBDQSAtIEczMSYwJAYDVQQLDB1BcHBsZSBDZXJ0aWZpY2F0aW9u
IEF1dGhvcml0eTETMBEGA1UECgwKQXBwbGUgSW5jLjELMAkGA1UEBhMCVVMwHhcN
MTQwNDMwMTgxOTA2WhcNMzkwNDMwMTgxOTA2WjBnMRswGQYDVQQDDBJBcHBsZSBS
b290IENBIC0gRzMxJjAkBgNVBAsMHUFwcGxlIENlcnRpZmljYXRpb24gQXV0aG9y
aXR5MRMwEQYDVQQKDApBcHBsZSBJbmMuMQswCQYDVQQGEwJVUzB2MBAGByqGSM49
AgEGBSuBBAAiA2IABJjpLz1AcqTtkyJygRMc3RCV8cWjTnHcFBbZDuWmBSp3ZHtf
TjjTuxxEtX/1H7YyYl3J6YRbTzBPEVoA/VhYDKX1DyxNB0cTddqXl5dvMVztK517
IDvYuVTZXpmkOlEKMaNCMEAwHQYDVR0OBBYEFLuw3qFYM4iapIqZ3r6966/ayySr
MA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMDA2gA
MGUCMQCD6cHEFl4aXTQY2e3v9GwOAEZLuN+yRhHFD/3meoyhpmvOwgPUnPWTxnS4
at+qIxUCMG1mihDK1A3UT82NQz60imOlM27jbdoXt2QfyFMm+YhidDkLF1vLUagM
6BgD56KyKA==
-----END CERTIFICATE-----
`

const (
	AppleReceiptValidationURLProduction = "https://buy.itunes.apple.com/verifyReceipt"
	AppleReceiptValidationURLSandbox    = "https://sandbox.itunes.apple.com/verifyReceipt"
	AppleReceiptIsFromTestSandbox       = 21007
)

// AppleJWSTransaction is the decoded StoreKit 2 JWS payload.
type AppleJWSTransaction struct {
	ProductID             string `json:"productId"`
	TransactionID         string `json:"transactionId"`
	OriginalTransactionID string `json:"originalTransactionId"`
	PurchaseDate          int64  `json:"purchaseDate"`
	ExpiresDate           int64  `json:"expiresDate"`
	RevocationDate        int64  `json:"revocationDate"`
	Environment           string `json:"environment"`
}

// IsJWS returns true when receipt looks like a JWS (three base64url segments).
func IsJWS(receipt string) bool {
	return len(strings.Split(receipt, ".")) == 3
}

// ValidateAppleJWSSignature verifies the Apple StoreKit 2 JWS signature chain.
func ValidateAppleJWSSignature(receipt string) error {
	jwsTokens := strings.Split(receipt, ".")
	if len(jwsTokens) != 3 {
		return fmt.Errorf("invalid jws format: expected 3 parts, got %d", len(jwsTokens))
	}
	header, payload, signature := jwsTokens[0], jwsTokens[1], jwsTokens[2]

	headerByte, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return err
	}
	var jwsHeader struct {
		Alg string   `json:"alg"`
		X5c []string `json:"x5c"`
	}
	if err = json.Unmarshal(headerByte, &jwsHeader); err != nil {
		return err
	}
	if len(jwsHeader.X5c) < 3 {
		return fmt.Errorf("invalid jws header: x5c field must contain at least 3 certificates")
	}

	certs := make([][]byte, 0, len(jwsHeader.X5c))
	for _, encodedCert := range jwsHeader.X5c {
		cert, err := base64.StdEncoding.DecodeString(encodedCert)
		if err != nil {
			return err
		}
		certs = append(certs, cert)
	}

	rootCert := x509.NewCertPool()
	if !rootCert.AppendCertsFromPEM([]byte(AppleRootPEM)) {
		return errors.New("failed to parse Apple root PEM")
	}
	leafCert, err := x509.ParseCertificate(certs[0])
	if err != nil {
		return err
	}
	interCert, err := x509.ParseCertificate(certs[1])
	if err != nil {
		return err
	}
	intermediates := x509.NewCertPool()
	intermediates.AddCert(interCert)
	if _, err = x509.ParseCertificate(certs[2]); err != nil {
		return err
	}
	if _, err = leafCert.Verify(x509.VerifyOptions{Roots: rootCert, Intermediates: intermediates}); err != nil {
		return err
	}
	pub, ok := leafCert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("failed to parse leaf certificate public key")
	}
	signingInput := []byte(header + "." + payload)
	hash := sha256.Sum256(signingInput)
	sigBytes, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return fmt.Errorf("failed to decode jws signature: %w", err)
	}
	if len(sigBytes) != 64 {
		return fmt.Errorf("invalid jws signature: invalid length %d, expected 64", len(sigBytes))
	}
	r := new(big.Int).SetBytes(sigBytes[:32])
	s := new(big.Int).SetBytes(sigBytes[32:])
	if !ecdsa.Verify(pub, hash[:], r, s) {
		return errors.New("invalid jws signature: ecdsa verification failed")
	}
	return nil
}

// ParseAppleJWSTransaction validates signature and returns the transaction payload.
func ParseAppleJWSTransaction(receipt string) (*AppleJWSTransaction, error) {
	if err := ValidateAppleJWSSignature(receipt); err != nil {
		return nil, err
	}
	parts := strings.Split(receipt, ".")
	jsonPayload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var tx AppleJWSTransaction
	if err := json.Unmarshal(jsonPayload, &tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

// ValidateReceiptAppleResponse is the legacy verifyReceipt response.
type ValidateReceiptAppleResponse struct {
	Environment       string `json:"environment"`
	Status            int    `json:"status"`
	LatestReceiptInfo []struct {
		ProductID             string `json:"product_id"`
		TransactionID         string `json:"transaction_id"`
		OriginalTransactionID string `json:"original_transaction_id"`
		PurchaseDateMs        string `json:"purchase_date_ms"`
		ExpiresDateMs         string `json:"expires_date_ms"`
		CancellationDateMs    string `json:"cancellation_date_ms"`
	} `json:"latest_receipt_info"`
	Receipt *struct {
		InApp []struct {
			ProductID             string `json:"product_id"`
			TransactionID         string `json:"transaction_id"`
			OriginalTransactionID string `json:"original_transaction_id"`
			PurchaseDateMs        string `json:"purchase_date_ms"`
			ExpiresDateMs         string `json:"expires_date_ms"`
			CancellationDateMs    string `json:"cancellation_date_ms"`
		} `json:"in_app"`
	} `json:"receipt"`
}

// ValidateLegacyReceiptApple calls Apple verifyReceipt (production then sandbox on 21007).
func ValidateLegacyReceiptApple(ctx context.Context, httpc *http.Client, password, receipt string) (*ValidateReceiptAppleResponse, []byte, error) {
	resp, raw, err := validateLegacyReceiptAppleURL(ctx, httpc, AppleReceiptValidationURLProduction, receipt, password)
	if err != nil {
		return nil, nil, err
	}
	if resp.Status == AppleReceiptIsFromTestSandbox {
		return validateLegacyReceiptAppleURL(ctx, httpc, AppleReceiptValidationURLSandbox, receipt, password)
	}
	return resp, raw, nil
}

func validateLegacyReceiptAppleURL(ctx context.Context, httpc *http.Client, endpoint, receipt, password string) (*ValidateReceiptAppleResponse, []byte, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"receipt-data": receipt, "password": password, "exclude-old-transactions": true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, &ValidationError{Err: ErrNon200ServiceApple, StatusCode: resp.StatusCode, Payload: string(body)}
	}
	var out ValidateReceiptAppleResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, nil, err
	}
	return &out, body, nil
}

// ParseMsTimestamp converts Apple millisecond string to time.
func ParseMsTimestamp(ms string) time.Time {
	if ms == "" {
		return time.Time{}
	}
	var n int64
	fmt.Sscanf(ms, "%d", &n)
	if n <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(n).UTC()
}
