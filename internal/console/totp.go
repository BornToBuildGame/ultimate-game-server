package console

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"time"
)

// ValidateTOTP validates a 6-digit TOTP code against a secret key for the current time.
// It allows a time window of +/- 1 time step (30 seconds) to account for clock drift.
func ValidateTOTP(secret []byte, code string) bool {
	if len(code) != 6 {
		return false
	}

	currentTime := time.Now().Unix()
	step := currentTime / 30

	for i := int64(-1); i <= 1; i++ {
		t := step + i
		expected := generateTOTP(secret, t)
		if expected == code {
			return true
		}
	}

	return false
}

func generateTOTP(secret []byte, step int64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(step))

	mac := hmac.New(sha1.New, secret)
	mac.Write(buf)
	hash := mac.Sum(nil)

	offset := hash[len(hash)-1] & 0xf
	binaryCode := (int32(hash[offset])&0x7f)<<24 |
		(int32(hash[offset+1])&0xff)<<16 |
		(int32(hash[offset+2])&0xff)<<8 |
		(int32(hash[offset+3])&0xff)

	otp := binaryCode % 1000000
	return fmt.Sprintf("%06d", otp)
}
