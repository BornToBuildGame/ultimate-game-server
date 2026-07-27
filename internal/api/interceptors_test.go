package api

import (
	"net/http"
	"testing"
)

func TestResolveHTTPHookID_LeaderboardTournament(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{http.MethodPost, "/v2/leaderboard/weekly", "WriteLeaderboardRecord"},
		{http.MethodGet, "/v2/leaderboard/weekly", "ListLeaderboardRecords"},
		{http.MethodGet, "/v2/leaderboard/weekly/around/user-1", "ListLeaderboardRecordsAroundOwner"},
		{http.MethodDelete, "/v2/leaderboard/weekly/owner/user-1", "DeleteLeaderboardRecord"},
		{http.MethodGet, "/v2/tournament", "ListTournaments"},
		{http.MethodPost, "/v2/tournament/arena/join", "JoinTournament"},
		{http.MethodPost, "/v2/tournament/arena", "WriteTournamentRecord"},
		{http.MethodGet, "/v2/tournament/arena", "ListTournamentRecords"},
		{http.MethodGet, "/v2/tournament/arena/around/user-1", "ListTournamentRecordsAroundOwner"},
		{http.MethodDelete, "/v2/tournament/arena/owner/user-1", "DeleteTournamentRecord"},
		{http.MethodPost, "/v2/storage", "WriteStorageObjects"},
		{http.MethodPost, "/v2/friend", "AddFriends"},
		{http.MethodGet, "/v2/friend", "ListFriends"},
		{http.MethodGet, "/v2/friend/friends", "ListFriendsOfFriends"},
		{http.MethodDelete, "/v2/friend", "DeleteFriends"},
		{http.MethodPost, "/v2/friend/block/user-1", "BlockFriends"},
		{http.MethodDelete, "/v2/friend/block/user-1", "UnblockFriends"},
		{http.MethodPost, "/v2/friend/block", "BlockFriends"},
		{http.MethodPost, "/v2/friend/unblock", "UnblockFriends"},
		{http.MethodGet, "/v2/user", "GetUsers"},
		{http.MethodPost, "/v2/event", "Event"},
		{http.MethodPost, "/v2/session/logout", "SessionLogout"},
		{http.MethodPut, "/v2/tournament/arena", "WriteTournamentRecord"},
		{http.MethodGet, "/v2/matchmaker/stats", "GetMatchmakerStats"},
		{http.MethodPost, "/v2/friend/facebook", "ImportFacebookFriends"},
		{http.MethodPost, "/v2/friend/steam", "ImportSteamFriends"},
		{http.MethodPost, "/v2/group", "CreateGroup"},
		{http.MethodGet, "/v2/group", "ListGroups"},
		{http.MethodPut, "/v2/group/g1", "UpdateGroup"},
		{http.MethodDelete, "/v2/group/g1", "DeleteGroup"},
		{http.MethodPost, "/v2/group/g1/join", "JoinGroup"},
		{http.MethodPost, "/v2/group/g1/leave", "LeaveGroup"},
		{http.MethodGet, "/v2/group/g1/user", "ListGroupUsers"},
		{http.MethodGet, "/v2/user/u1/group", "ListUserGroups"},
		{http.MethodGet, "/v2/notification", "ListNotifications"},
		{http.MethodDelete, "/v2/notification", "DeleteNotifications"},
		{http.MethodGet, "/v2/wallet", "GetWallet"},
		{http.MethodGet, "/v2/wallet/ledger", "ListWalletLedger"},
		{http.MethodPost, "/v2/match", "CreateMatch"},
		{http.MethodGet, "/v2/match", "ListMatches"},
		{http.MethodGet, "/v2/match/m1", "GetMatch"},
		{http.MethodGet, "/v2/party", "ListParties"},
		{http.MethodGet, "/v2/channel/ch1", "ListChannelMessages"},
		{http.MethodPost, "/v2/rpc/myfn", ""},
		{http.MethodGet, "/v2/account", ""},
	}
	for _, tc := range cases {
		got := resolveHTTPHookID(tc.method, tc.path)
		if got != tc.want {
			t.Fatalf("%s %s: got %q want %q", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestResolveHTTPHookID_IAP(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{http.MethodPost, "/v2/iap/purchase/apple", "ValidatePurchaseApple"},
		{http.MethodPost, "/v2/iap/purchase/google", "ValidatePurchaseGoogle"},
		{http.MethodPost, "/v2/iap/purchase/huawei", "ValidatePurchaseHuawei"},
		{http.MethodPost, "/v2/iap/purchase/facebookinstant", "ValidatePurchaseFacebookInstant"},
		{http.MethodPost, "/v2/iap/purchase/samsung", "ValidatePurchaseSamsung"},
		{http.MethodPost, "/v2/iap/subscription/apple", "ValidateSubscriptionApple"},
		{http.MethodPost, "/v2/iap/subscription/google", "ValidateSubscriptionGoogle"},
		{http.MethodPost, "/v2/iap/subscription", "ListSubscriptions"},
		{http.MethodGet, "/v2/iap/subscription/sku_gold", "GetSubscription"},
	}
	for _, tc := range cases {
		got := resolveHTTPHookID(tc.method, tc.path)
		if got != tc.want {
			t.Errorf("%s %s: got %q want %q", tc.method, tc.path, got, tc.want)
		}
	}
}
