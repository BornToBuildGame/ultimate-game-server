package auth

import (
	"context"
	"testing"
	"time"
)

func TestValidateEmailAddress(t *testing.T) {
	if !ValidateEmailAddress("player@example.com") {
		t.Fatal("expected valid email")
	}
	if ValidateEmailAddress("not-an-email") {
		t.Fatal("expected invalid email")
	}
	if ValidateEmailAddress("password") {
		t.Fatal("expected invalid")
	}
}

func TestValidatePasswordPolicy(t *testing.T) {
	if err := ValidatePasswordPolicy("short"); err == nil {
		t.Fatal("expected length error")
	}
	if err := ValidatePasswordPolicy("password"); err == nil {
		t.Fatal("expected common password error")
	}
	if err := ValidatePasswordPolicy("SecurePass9"); err != nil {
		t.Fatalf("expected ok: %v", err)
	}
}

func TestLoginLockout(t *testing.T) {
	l := NewLoginLockout()
	key, ip := "a@b.com", "1.2.3.4"
	for i := 0; i < accountFailLimit; i++ {
		l.RecordFailure(key, ip)
	}
	if err := l.Check(key, ip); err == nil {
		t.Fatal("expected lockout")
	}
	l.ClearSuccess(key, ip)
	// ClearSuccess removes account key; may still be locked via IP if IP limit hit.
}

func TestMemorySessionStore_RotationAndLogout(t *testing.T) {
	store := NewMemorySessionStore()
	ctx := context.Background()
	_ = store.RegisterSession(ctx, "u1", "rt1", "")
	uid, theft, err := store.ValidateAndRotateSession(ctx, "rt1")
	if err != nil || theft || uid != "u1" {
		t.Fatalf("validate: uid=%s theft=%v err=%v", uid, theft, err)
	}
	_ = store.RegisterSession(ctx, "u1", "rt2", "rt1")
	_, theft, err = store.ValidateAndRotateSession(ctx, "rt1")
	if err == nil || !theft {
		t.Fatalf("expected reuse detection, err=%v theft=%v", err, theft)
	}
	_ = store.RegisterSession(ctx, "u1", "rt3", "")
	_ = store.RevokeAllSessions(ctx, "u1")
	_, _, err = store.ValidateAndRotateSession(ctx, "rt3")
	if err == nil {
		t.Fatal("expected revoked")
	}
	_ = store.BlacklistAccessJTI(ctx, "jti1", time.Now().Add(time.Hour))
	denied, _ := store.IsAccessJTIBlacklisted(ctx, "jti1")
	if !denied {
		t.Fatal("expected jti denied")
	}
}

func TestMockSocialTokens(t *testing.T) {
	ctx := context.Background()
	id, err := VerifyAppleToken(ctx, "mock_apple123")
	if err != nil || id != "apple123" {
		t.Fatalf("apple mock: %s %v", id, err)
	}
	id, err = VerifyGoogleToken(ctx, "mock_g1")
	if err != nil || id != "g1" {
		t.Fatalf("google mock: %s %v", id, err)
	}
	id, err = VerifyFacebookToken(ctx, "mock_fb1")
	if err != nil || id != "fb1" {
		t.Fatalf("facebook mock: %s %v", id, err)
	}
	id, err = VerifySteamTicket(ctx, "mock_steam1")
	if err != nil || id != "steam1" {
		t.Fatalf("steam mock: %s %v", id, err)
	}
	id, err = VerifyGameCenterSignature(ctx, GameCenterCredentials{PlayerID: "gc1", Signature: "mock_"})
	if err != nil || id != "gc1" {
		t.Fatalf("gc mock: %s %v", id, err)
	}
}

func TestTokenManager_JTI(t *testing.T) {
	tm, err := NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	access, _, err := tm.GenerateSession("u1", "name")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := tm.VerifyToken(access)
	if err != nil || claims.ID == "" {
		t.Fatalf("expected jti: %+v err=%v", claims, err)
	}
	claims2, err := tm.VerifyTokenIgnoreExpiry(access)
	if err != nil || claims2.ID != claims.ID {
		t.Fatal(err)
	}
}
