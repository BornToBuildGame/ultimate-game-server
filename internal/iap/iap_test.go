package iap

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestIsJWS(t *testing.T) {
	if !IsJWS("aaa.bbb.ccc") {
		t.Fatal("expected JWS format")
	}
	if IsJWS(`{"product_id":"x","transaction_id":"y"}`) {
		t.Fatal("JSON receipt should not be JWS")
	}
	if IsJWS("not-a-jws") {
		t.Fatal("single segment should not be JWS")
	}
}

func TestValidateAppleJWSSignature_InvalidFormat(t *testing.T) {
	err := ValidateAppleJWSSignature("a.b")
	if err == nil {
		t.Fatal("expected error for invalid JWS")
	}
}

func TestValidateAppleJWSSignature_BadPayload(t *testing.T) {
	// Three segments but not a real Apple cert chain.
	err := ValidateAppleJWSSignature("eyJhbGciOiJFUzI1NiJ9.eyJ0eCI6MX0.AAAA")
	if err == nil {
		t.Fatal("expected signature/cert validation failure")
	}
}

func TestVerifySignatureHuawei(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pubDER)
	data := `{"productId":"sku1","orderId":"ord1"}`
	hash := sha256.Sum256([]byte(data))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	sigB64 := base64.StdEncoding.EncodeToString(sig)
	if err := VerifySignatureHuawei(pubB64, data, sigB64); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := VerifySignatureHuawei(pubB64, data, base64.StdEncoding.EncodeToString([]byte("bad"))); err == nil {
		t.Fatal("expected invalid signature error")
	}
}

func TestDecodeReceiptGoogle(t *testing.T) {
	inner := map[string]interface{}{
		"orderId": "GPA.1", "packageName": "com.example", "productId": "gems",
		"purchaseToken": "tok", "purchaseTime": 1,
	}
	innerJSON, _ := json.Marshal(inner)
	wrapped, _ := json.Marshal(map[string]string{"json": string(innerJSON)})
	gr, err := DecodeReceiptGoogle(string(wrapped))
	if err != nil {
		t.Fatal(err)
	}
	if gr.PackageName != "com.example" || gr.ProductID != "gems" || gr.PurchaseToken != "tok" {
		t.Fatalf("unexpected decode: %+v", gr)
	}
	gr2, err := DecodeReceiptGoogle(string(innerJSON))
	if err != nil {
		t.Fatal(err)
	}
	if gr2.PackageName != "com.example" {
		t.Fatal("unwrapped receipt failed")
	}
	if _, err := DecodeReceiptGoogle(`{"productId":"x"}`); err == nil {
		t.Fatal("expected malformed without packageName")
	}
}
