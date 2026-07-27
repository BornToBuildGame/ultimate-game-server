package api

import (
	"testing"

	"ultimate-game-server/internal/api/apipb"

	"github.com/stretchr/testify/require"
)

func TestParseGooglePurchaseBlob_ReferenceShape(t *testing.T) {
	product, token := parseGooglePurchaseBlob(`{"productId":"coins","purchaseToken":"tok-1"}`, "", "")
	require.Equal(t, "coins", product)
	require.Equal(t, "tok-1", token)

	product, token = parseGooglePurchaseBlob(`{"product_id":"gems","purchase_token":"tok-2"}`, "", "")
	require.Equal(t, "gems", product)
	require.Equal(t, "tok-2", token)

	// Legacy fields win when both set.
	product, token = parseGooglePurchaseBlob(`{"productId":"x","purchaseToken":"y"}`, "legacy", "legacy-tok")
	require.Equal(t, "legacy", product)
	require.Equal(t, "legacy-tok", token)
}

func TestResolveGooglePurchaseFields_Proto(t *testing.T) {
	req := &apipb.ValidatePurchaseGoogleRequest{
		Purchase: `{"productId":"sku","purchaseToken":"abc"}`,
	}
	p, tok := resolveGooglePurchaseFields(req)
	require.Equal(t, "sku", p)
	require.Equal(t, "abc", tok)
}
