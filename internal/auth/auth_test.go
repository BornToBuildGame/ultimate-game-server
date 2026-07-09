package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestTokenManager_GenerateAndVerify(t *testing.T) {
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, err := NewTokenManager(secret, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create token manager: %v", err)
	}

	userID := uuid.New().String()
	username := "test_player"

	token, refreshToken, err := tm.GenerateSession(userID, username)
	if err != nil {
		t.Fatalf("failed to generate session: %v", err)
	}

	if token == "" {
		t.Error("expected access token to be non-empty")
	}
	if refreshToken == "" {
		t.Error("expected refresh token to be non-empty")
	}

	// Verify token
	claims, err := tm.VerifyToken(token)
	if err != nil {
		t.Fatalf("failed to verify token: %v", err)
	}

	if claims.UserID != userID {
		t.Errorf("expected userID %q, got %q", userID, claims.UserID)
	}
	if claims.Username != username {
		t.Errorf("expected username %q, got %q", username, claims.Username)
	}
}

func TestTokenManager_ExpiredToken(t *testing.T) {
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	// Manager with 1 millisecond expiry
	tm, err := NewTokenManager(secret, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create token manager: %v", err)
	}

	userID := uuid.New().String()
	username := "test_player"

	token, _, err := tm.GenerateSession(userID, username)
	if err != nil {
		t.Fatalf("failed to generate session: %v", err)
	}

	// Sleep to trigger expiration
	time.Sleep(5 * time.Millisecond)

	_, err = tm.VerifyToken(token)
	if err == nil {
		t.Error("expected error verifying expired token, but got nil")
	}

	if !strings.Contains(err.Error(), jwt.ErrTokenExpired.Error()) {
		t.Errorf("expected token expired error, got: %v", err)
	}
}

func TestSessionRegistry_RotationAndTheftDetection(t *testing.T) {
	sr := NewSessionRegistry()
	userID := "user123"

	// 1. Register parent session
	parentToken := "parent_refresh_token_1"
	sr.RegisterSession(userID, parentToken, "")

	// 2. Rotate to child session
	childToken := "child_refresh_token_2"
	uid, detected, err := sr.ValidateAndRotateSession(parentToken)
	if err != nil {
		t.Fatalf("failed to validate parent token: %v", err)
	}
	if detected {
		t.Fatal("unexpected reuse detection on first rotate")
	}
	if uid != userID {
		t.Errorf("expected user ID %q, got %q", userID, uid)
	}

	// Register rotated session
	sr.RegisterSession(userID, childToken, parentToken)

	// 3. Re-requesting the parent token should trigger theft/reuse detection
	_, detected, err = sr.ValidateAndRotateSession(parentToken)
	if err == nil {
		t.Error("expected error when reusing token, got nil")
	}
	if !detected {
		t.Error("expected reuse/theft detection to be true")
	}

	// 4. Verification of the child token should now fail since the entire family was revoked
	_, _, err = sr.ValidateAndRotateSession(childToken)
	if err == nil {
		t.Error("expected child token validation to fail after family revocation, got nil")
	}
}
