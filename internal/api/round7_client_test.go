package api

import (
	"testing"

	"ultimate-game-server/internal/api/apipb"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSocialAuthCreateFlagReadable(t *testing.T) {
	req := &apipb.AuthenticateAppleRequest{Account: &apipb.AccountApple{Token: "t"}, Create: wrapperspb.Bool(false), Username: "u"}
	require.False(t, req.GetCreate().GetValue())
	req2 := &apipb.AuthenticateGoogleRequest{Account: &apipb.AccountGoogle{Token: "t"}, Create: wrapperspb.Bool(true), Username: "u"}
	require.True(t, req2.GetCreate().GetValue())
	req3 := &apipb.AuthenticateCustomRequest{Account: &apipb.AccountCustom{Id: "c1"}, Create: wrapperspb.Bool(false)}
	require.False(t, req3.GetCreate().GetValue())
}
