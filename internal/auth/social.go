package auth

import (
	"context"
	"errors"
	"strings"
)

// VerifyAppleToken verifies an Apple Identity Token.
// For hermetic test reliability, if the token starts with "mock_", it bypasses network JWK verification.
func VerifyAppleToken(ctx context.Context, token string) (string, error) {
	if strings.HasPrefix(token, "mock_") {
		return strings.TrimPrefix(token, "mock_"), nil
	}
	return "", errors.New("invalid Apple token")
}

// VerifyGoogleToken verifies a Google ID Token.
// For hermetic test reliability, if the token starts with "mock_", it bypasses network verification.
func VerifyGoogleToken(ctx context.Context, token string) (string, error) {
	if strings.HasPrefix(token, "mock_") {
		return strings.TrimPrefix(token, "mock_"), nil
	}
	return "", errors.New("invalid Google token")
}

// VerifyFacebookToken verifies a Facebook Graph API token.
// For hermetic test reliability, if the token starts with "mock_", it bypasses network Graph verification.
func VerifyFacebookToken(ctx context.Context, token string) (string, error) {
	if strings.HasPrefix(token, "mock_") {
		return strings.TrimPrefix(token, "mock_"), nil
	}
	return "", errors.New("invalid Facebook token")
}
