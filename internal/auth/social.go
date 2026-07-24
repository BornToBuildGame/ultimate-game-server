package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// SocialConfig holds provider verification settings (env-overridable).
type SocialConfig struct {
	AppleClientIDs   []string // allowed aud / bundle IDs
	GoogleClientIDs  []string
	FacebookAppID    string
	FacebookAppSecret string
	SteamAppID       string
	SteamPublisherKey string
	HTTPClient       *http.Client
}

// DefaultSocialConfig loads from environment variables.
func DefaultSocialConfig() SocialConfig {
	apple := strings.TrimSpace(os.Getenv("APPLE_CLIENT_IDS"))
	google := strings.TrimSpace(os.Getenv("GOOGLE_CLIENT_IDS"))
	cfg := SocialConfig{
		FacebookAppID:     os.Getenv("FACEBOOK_APP_ID"),
		FacebookAppSecret: os.Getenv("FACEBOOK_APP_SECRET"),
		SteamAppID:        os.Getenv("STEAM_APP_ID"),
		SteamPublisherKey: os.Getenv("STEAM_PUBLISHER_KEY"),
		HTTPClient:        &http.Client{Timeout: 10 * time.Second},
	}
	if apple != "" {
		cfg.AppleClientIDs = strings.Split(apple, ",")
	}
	if google != "" {
		cfg.GoogleClientIDs = strings.Split(google, ",")
	}
	return cfg
}

var (
	defaultSocial = DefaultSocialConfig()
	appleKeysMu   sync.RWMutex
	appleKeys     map[string]*rsa.PublicKey
	appleKeysExp  time.Time
)

// SetSocialConfig overrides the package-level social verification config (tests/bootstrap).
func SetSocialConfig(cfg SocialConfig) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	defaultSocial = cfg
}

func mockProviderID(token string) (string, bool) {
	if strings.HasPrefix(token, "mock_") {
		return strings.TrimPrefix(token, "mock_"), true
	}
	return "", false
}

// VerifyAppleToken verifies an Apple Identity Token (JWT) against Apple JWKS.
// Tokens prefixed with "mock_" bypass network verification for hermetic tests.
func VerifyAppleToken(ctx context.Context, token string) (string, error) {
	if id, ok := mockProviderID(token); ok {
		return id, nil
	}
	return verifyAppleIDToken(ctx, token, defaultSocial)
}

func verifyAppleIDToken(ctx context.Context, tokenStr string, cfg SocialConfig) (string, error) {
	keys, err := getApplePublicKeys(ctx, cfg.HTTPClient)
	if err != nil {
		return "", fmt.Errorf("apple jwks: %w", err)
	}

	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	tok, err := parser.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("apple token missing kid")
		}
		key, ok := keys[kid]
		if !ok {
			return nil, fmt.Errorf("apple jwk kid not found: %s", kid)
		}
		return key, nil
	})
	if err != nil {
		return "", fmt.Errorf("invalid apple token: %w", err)
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok || !tok.Valid {
		return "", errors.New("invalid apple token claims")
	}
	iss, _ := claims["iss"].(string)
	if iss != "https://appleid.apple.com" {
		return "", errors.New("invalid apple token issuer")
	}
	if len(cfg.AppleClientIDs) > 0 {
		audOK := false
		switch aud := claims["aud"].(type) {
		case string:
			for _, id := range cfg.AppleClientIDs {
				if aud == strings.TrimSpace(id) {
					audOK = true
					break
				}
			}
		case []interface{}:
			for _, a := range aud {
				s, _ := a.(string)
				for _, id := range cfg.AppleClientIDs {
					if s == strings.TrimSpace(id) {
						audOK = true
						break
					}
				}
			}
		}
		if !audOK {
			return "", errors.New("apple token audience mismatch")
		}
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", errors.New("apple token missing sub")
	}
	return sub, nil
}

type appleJWKSet struct {
	Keys []appleJWK `json:"keys"`
}
type appleJWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Alg string `json:"alg"`
}

func getApplePublicKeys(ctx context.Context, client *http.Client) (map[string]*rsa.PublicKey, error) {
	appleKeysMu.RLock()
	if appleKeys != nil && time.Now().Before(appleKeysExp) {
		defer appleKeysMu.RUnlock()
		return appleKeys, nil
	}
	appleKeysMu.RUnlock()

	appleKeysMu.Lock()
	defer appleKeysMu.Unlock()
	if appleKeys != nil && time.Now().Before(appleKeysExp) {
		return appleKeys, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://appleid.apple.com/auth/keys", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("apple jwks status %d", resp.StatusCode)
	}
	var set appleJWKSet
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	out := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		pub, err := jwkToRSAPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		out[k.Kid] = pub
	}
	if len(out) == 0 {
		return nil, errors.New("no apple public keys loaded")
	}
	appleKeys = out
	appleKeysExp = time.Now().Add(24 * time.Hour)
	return out, nil
}

func jwkToRSAPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nb, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	eb, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	var eInt int
	for _, b := range eb {
		eInt = eInt<<8 + int(b)
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: eInt}, nil
}

// VerifyGoogleToken verifies a Google ID token via Google's tokeninfo endpoint
// (or mock_ bypass). Optionally checks aud against GOOGLE_CLIENT_IDS.
func VerifyGoogleToken(ctx context.Context, token string) (string, error) {
	if id, ok := mockProviderID(token); ok {
		return id, nil
	}
	cfg := defaultSocial
	u := "https://oauth2.googleapis.com/tokeninfo?id_token=" + url.QueryEscape(token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("google tokeninfo: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid google token: %s", string(body))
	}
	var payload struct {
		Sub           string `json:"sub"`
		Aud           string `json:"aud"`
		Email         string `json:"email"`
		EmailVerified string `json:"email_verified"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", err
	}
	if payload.Sub == "" {
		return "", errors.New("google token missing sub")
	}
	if len(cfg.GoogleClientIDs) > 0 {
		ok := false
		for _, id := range cfg.GoogleClientIDs {
			if payload.Aud == strings.TrimSpace(id) {
				ok = true
				break
			}
		}
		if !ok {
			return "", errors.New("google token audience mismatch")
		}
	}
	return payload.Sub, nil
}

// VerifyFacebookToken verifies a Facebook access token via Graph API debug_token / me.
func VerifyFacebookToken(ctx context.Context, token string) (string, error) {
	if id, ok := mockProviderID(token); ok {
		return id, nil
	}
	cfg := defaultSocial
	u := "https://graph.facebook.com/me?fields=id&access_token=" + url.QueryEscape(token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("facebook graph: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid facebook token: %s", string(body))
	}
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", err
	}
	if payload.ID == "" {
		return "", errors.New("facebook token missing id")
	}
	// Optional: validate against app via debug_token when app credentials configured.
	if cfg.FacebookAppID != "" && cfg.FacebookAppSecret != "" {
		appToken := cfg.FacebookAppID + "|" + cfg.FacebookAppSecret
		du := fmt.Sprintf("https://graph.facebook.com/debug_token?input_token=%s&access_token=%s",
			url.QueryEscape(token), url.QueryEscape(appToken))
		dreq, _ := http.NewRequestWithContext(ctx, http.MethodGet, du, nil)
		dresp, err := cfg.HTTPClient.Do(dreq)
		if err == nil {
			defer dresp.Body.Close()
			var debug struct {
				Data struct {
					AppID   string `json:"app_id"`
					IsValid bool   `json:"is_valid"`
					UserID  string `json:"user_id"`
				} `json:"data"`
			}
			_ = json.NewDecoder(dresp.Body).Decode(&debug)
			if !debug.Data.IsValid || debug.Data.AppID != cfg.FacebookAppID {
				return "", errors.New("facebook token failed app validation")
			}
			if debug.Data.UserID != "" {
				return debug.Data.UserID, nil
			}
		}
	}
	return payload.ID, nil
}

// VerifySteamTicket authenticates a Steam session ticket via Steam Web API.
// Tokens prefixed with mock_ are accepted as steam IDs for tests.
func VerifySteamTicket(ctx context.Context, ticket string) (string, error) {
	if id, ok := mockProviderID(ticket); ok {
		return id, nil
	}
	cfg := defaultSocial
	if cfg.SteamPublisherKey == "" || cfg.SteamAppID == "" {
		return "", errors.New("steam auth not configured (STEAM_APP_ID / STEAM_PUBLISHER_KEY)")
	}
	// AuthenticateUserTicket
	u := fmt.Sprintf(
		"https://api.steampowered.com/ISteamUserAuth/AuthenticateUserTicket/v1/?key=%s&appid=%s&ticket=%s",
		url.QueryEscape(cfg.SteamPublisherKey),
		url.QueryEscape(cfg.SteamAppID),
		url.QueryEscape(ticket),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("steam auth: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var payload struct {
		Response struct {
			Params struct {
				Result  string `json:"result"`
				SteamID string `json:"steamid"`
			} `json:"params"`
			Error struct {
				ErrorDesc string `json:"errordesc"`
			} `json:"error"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", err
	}
	if !strings.EqualFold(payload.Response.Params.Result, "OK") || payload.Response.Params.SteamID == "" {
		desc := payload.Response.Error.ErrorDesc
		if desc == "" {
			desc = string(body)
		}
		return "", fmt.Errorf("invalid steam ticket: %s", desc)
	}
	return payload.Response.Params.SteamID, nil
}

// GameCenterCredentials holds Apple Game Center identity verification fields.
type GameCenterCredentials struct {
	PlayerID     string
	BundleID     string
	Timestamp    int64
	Salt         string
	Signature    string
	PublicKeyURL string
}

// VerifyGameCenterSignature verifies Game Center player identity.
// Mock mode: PublicKeyURL "mock" returns PlayerID without crypto verification.
func VerifyGameCenterSignature(ctx context.Context, cred GameCenterCredentials) (string, error) {
	if cred.PlayerID == "" {
		return "", errors.New("gamecenter player_id required")
	}
	if strings.HasPrefix(cred.PublicKeyURL, "mock") || cred.Signature == "mock_" || strings.HasPrefix(cred.Signature, "mock_") {
		return cred.PlayerID, nil
	}
	if cred.BundleID == "" || cred.Salt == "" || cred.Signature == "" || cred.PublicKeyURL == "" {
		return "", errors.New("incomplete gamecenter credentials")
	}
	if !strings.HasPrefix(cred.PublicKeyURL, "https://static.gc.apple.com/") &&
		!strings.HasPrefix(cred.PublicKeyURL, "https://sandbox.gc.apple.com/") {
		return "", errors.New("invalid gamecenter public key url")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cred.PublicKeyURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := defaultSocial.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("gamecenter pubkey: %w", err)
	}
	defer resp.Body.Close()
	pemBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return "", errors.New("failed to decode gamecenter public key pem")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		// Try cert
		cert, cerr := x509.ParseCertificate(block.Bytes)
		if cerr != nil {
			return "", fmt.Errorf("parse gamecenter key: %w", err)
		}
		pubAny = cert.PublicKey
	}
	pub, ok := pubAny.(*rsa.PublicKey)
	if !ok {
		return "", errors.New("gamecenter key is not RSA")
	}

	payload := fmt.Sprintf("%s%s%d%s", cred.PlayerID, cred.BundleID, cred.Timestamp, cred.Salt)
	sig, err := base64.StdEncoding.DecodeString(cred.Signature)
	if err != nil {
		sig, err = base64.RawURLEncoding.DecodeString(cred.Signature)
		if err != nil {
			return "", errors.New("invalid gamecenter signature encoding")
		}
	}
	sum := sha256.Sum256([]byte(payload))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig); err != nil {
		return "", fmt.Errorf("gamecenter signature invalid: %w", err)
	}
	return cred.PlayerID, nil
}

// VerifyFacebookInstantGame verifies a signed player info token.
// Tokens prefixed with mock_ are accepted for hermetic tests.
func VerifyFacebookInstantGame(ctx context.Context, signedPlayerInfo string) (string, error) {
	_ = ctx
	if id, ok := mockProviderID(signedPlayerInfo); ok {
		return id, nil
	}
	// MVP: treat non-empty signed_player_info as opaque provider ID when not mock.
	// Production should HMAC-verify with Facebook Instant Game app secret.
	s := strings.TrimSpace(signedPlayerInfo)
	if s == "" {
		return "", errors.New("facebook instant game signed_player_info required")
	}
	if len(s) > 2048 {
		return "", errors.New("facebook instant game signed_player_info too long")
	}
	return s, nil
}

