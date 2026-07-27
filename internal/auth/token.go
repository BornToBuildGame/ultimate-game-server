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
	UserID   string            `json:"sub"`
	Username string            `json:"usn"`
	Vars     map[string]string `json:"vrs,omitempty"`
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

// Expiry returns the configured access-token lifetime.
func (tm *TokenManager) Expiry() time.Duration {
	return tm.expiry
}

// GenerateSession generates a JWT access token (with jti) and opaque refresh token.
func (tm *TokenManager) GenerateSession(userID string, username string) (string, string, error) {
	return tm.GenerateSessionWithVars(userID, username, nil)
}

// GenerateSessionWithVars embeds optional session vars into the access JWT.
func (tm *TokenManager) GenerateSessionWithVars(userID, username string, vars map[string]string) (string, string, error) {
	now := time.Now()
	exp := now.Add(tm.expiry)
	token, err := tm.signAccessToken(userID, username, vars, now, exp)
	if err != nil {
		return "", "", err
	}
	refreshToken := uuid.New().String()
	return token, refreshToken, nil
}

// GenerateAccessToken mints an access JWT with optional custom expiry (unix seconds).
// If expiresAt is 0, the configured TokenManager expiry is used.
func (tm *TokenManager) GenerateAccessToken(userID, username string, expiresAt int64, vars map[string]string) (string, int64, error) {
	now := time.Now()
	exp := now.Add(tm.expiry)
	if expiresAt > 0 {
		exp = time.Unix(expiresAt, 0)
	}
	token, err := tm.signAccessToken(userID, username, vars, now, exp)
	if err != nil {
		return "", 0, err
	}
	return token, exp.Unix(), nil
}

func (tm *TokenManager) signAccessToken(userID, username string, vars map[string]string, now, exp time.Time) (string, error) {
	claims := Claims{
		UserID:   userID,
		Username: username,
		Vars:     vars,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        uuid.New().String(),
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}
	tokenObj := jwt.NewWithClaims(tm.signingMethod, claims)
	return tokenObj.SignedString(tm.secretKey)
}

// VerifyToken parses and validates a JWT access token.
func (tm *TokenManager) VerifyToken(tokenStr string) (*Claims, error) {
	return tm.verifyToken(tokenStr, true)
}

// VerifyTokenIgnoreExpiry validates signature/claims but ignores expiry (logout/blacklist).
func (tm *TokenManager) VerifyTokenIgnoreExpiry(tokenStr string) (*Claims, error) {
	return tm.verifyToken(tokenStr, false)
}

func (tm *TokenManager) verifyToken(tokenStr string, checkExpiry bool) (*Claims, error) {
	opts := []jwt.ParserOption{}
	if !checkExpiry {
		opts = append(opts, jwt.WithoutClaimsValidation())
	}
	parser := jwt.NewParser(opts...)
	token, err := parser.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return tm.secretKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, errors.New("invalid token claims")
	}
	if checkExpiry && !token.Valid {
		return nil, errors.New("invalid token claims")
	}
	if !checkExpiry {
		// still require matching HMAC method / populated claims
		if claims.UserID == "" {
			return nil, errors.New("invalid token claims")
		}
	}
	return claims, nil
}
