// Package iap provides store receipt verification helpers (Apple, Google, Huawei).
// Patterns follow the reference engine's iap package.
package iap

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Shared HTTP client for provider calls.
var HTTPClient = &http.Client{Timeout: 20 * time.Second}

// ValidationError wraps a non-success provider HTTP response.
type ValidationError struct {
	Err        error
	StatusCode int
	Payload    string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s, status=%d, payload=%s", e.Err.Error(), e.StatusCode, e.Payload)
}
func (e *ValidationError) Unwrap() error { return e.Err }

var (
	ErrNon200ServiceApple     = errors.New("non-200 response from Apple service")
	ErrNon200ServiceGoogle    = errors.New("non-200 response from Google service")
	ErrNon200ServiceHuawei    = errors.New("non-200 response from Huawei service")
	ErrInvalidSignatureHuawei = errors.New("inAppPurchaseData invalid signature")
)

const (
	AppleSandboxEnvironment    = "Sandbox"
	AppleProductionEnvironment = "Production"
	accessTokenExpiresGracePeriod = 300
)
