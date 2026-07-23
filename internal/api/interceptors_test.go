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
		{http.MethodDelete, "/v2/friend/block/user-1", "DeleteFriends"},
		{http.MethodPost, "/v2/friend/facebook", "ImportFacebookFriends"},
		{http.MethodPost, "/v2/friend/steam", "ImportSteamFriends"},
	}
	for _, tc := range cases {
		got := resolveHTTPHookID(tc.method, tc.path)
		if got != tc.want {
			t.Fatalf("%s %s: got %q want %q", tc.method, tc.path, got, tc.want)
		}
	}
}
