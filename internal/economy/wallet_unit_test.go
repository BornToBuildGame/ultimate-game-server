package economy

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
)

func TestErrWalletNegative_IsInsufficientFunds(t *testing.T) {
	err := &ErrWalletNegative{UserID: "u1", Path: "coins", Current: 5, Amount: -10}
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatal("expected errors.Is(err, ErrInsufficientFunds)")
	}
}

func TestLedgerCursorRoundTrip(t *testing.T) {
	c := &ledgerCursor{UserID: "u1", ID: "00000000-0000-0000-0000-000000000001"}
	enc, err := encodeLedgerCursor(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base64.RawURLEncoding.DecodeString(enc); err != nil {
		t.Fatal(err)
	}
	dec, err := decodeLedgerCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec.UserID != c.UserID || dec.ID != c.ID {
		t.Fatalf("cursor mismatch: %+v vs %+v", dec, c)
	}
	if _, err := decodeLedgerCursor("!!!not-valid!!!"); !errors.Is(err, ErrLedgerCursorInvalid) {
		t.Fatalf("expected invalid cursor, got %v", err)
	}
}

func TestParseTestReceipt(t *testing.T) {
	raw := `{"product_id":"p1","transaction_id":"t1","environment":1}`
	p, _, ok := parseTestReceipt(raw)
	if !ok || p.ProductID != "p1" || p.TransactionID != "t1" {
		t.Fatalf("parse failed: %+v ok=%v", p, ok)
	}
	if _, _, ok := parseTestReceipt("not-json"); ok {
		t.Fatal("expected false for invalid")
	}
	b, _ := json.Marshal(p)
	if len(b) == 0 {
		t.Fatal("marshal")
	}
}
