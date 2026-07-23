package storage

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	uid := uuid.New()
	enc, err := encodeStorageCursor(&storageCursor{Key: "k1", UserID: uid, Read: 2})
	if err != nil || enc == "" {
		t.Fatalf("encode: %v %q", err, enc)
	}
	dec, err := decodeStorageCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.Key != "k1" || dec.UserID != uid || dec.Read != 2 {
		t.Fatalf("mismatch: %+v", dec)
	}
	if _, err := decodeStorageCursor("bad"); !errors.Is(err, ErrListCursorInvalid) {
		t.Fatalf("expected invalid cursor, got %v", err)
	}
}

func TestCalculateVersion(t *testing.T) {
	a := CalculateVersion(`{"a":1}`)
	b := CalculateVersion(`{"a":1}`)
	c := CalculateVersion(`{"a":2}`)
	if a != b || a == c || len(a) != 32 {
		t.Fatalf("versions a=%s b=%s c=%s", a, b, c)
	}
}

func TestErrOCCConflictAlias(t *testing.T) {
	if !errors.Is(ErrOCCConflict, ErrStorageRejectedVersion) {
		t.Fatal("ErrOCCConflict should alias ErrStorageRejectedVersion")
	}
}
