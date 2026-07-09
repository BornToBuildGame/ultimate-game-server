package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Claims defines the custom JWT claims structure.
type Claims struct {
	UserID   string `json:"sub"`
	Username string `json:"usn"`
	jwt.RegisteredClaims
}

// TokenManager handles JWT signing, verification, and key management.
type TokenManager struct {
	secretKey     []byte
	signingMethod jwt.SigningMethod
	expiry        time.Duration
}

// NewTokenManager creates a new instance of TokenManager.
func NewTokenManager(secretKey []byte, expiry time.Duration) (*TokenManager, error) {
	if len(secretKey) < 32 {
		return nil, errors.New("JWT secret key must be at least 256 bits (32 bytes)")
	}
	return &TokenManager{
		secretKey:     secretKey,
		signingMethod: jwt.SigningMethodHS256,
		expiry:        expiry,
	}, nil
}

// GenerateSession generates a stateless JWT access token and a cryptographically secure refresh token.
func (tm *TokenManager) GenerateSession(userID string, username string) (string, string, error) {
	now := time.Now()
	claims := Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(tm.expiry)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	tokenObj := jwt.NewWithClaims(tm.signingMethod, claims)
	accessToken, err := tokenObj.SignedString(tm.secretKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to sign access token: %w", err)
	}

	// Generate random cryptographically secure Refresh Token UUID
	refreshToken := uuid.New().String()

	return accessToken, refreshToken, nil
}

// VerifyToken parses and validates a JWT access token, returning the custom claims if valid.
func (tm *TokenManager) VerifyToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// Ensure correct signing method family
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return tm.secretKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return claims, nil
}
