package api

import (
	"encoding/json"
	"testing"

	"ultimate-game-server/internal/matchmaker"

	"github.com/stretchr/testify/require"
)

func TestBuildMatchmakerMatchedPayload_ReferenceShape(t *testing.T) {
	result := matchmaker.MatchResult{
		MatchID:       "match-abc",
		MatchToken:    "tok-xyz",
		Authoritative: false,
		MatchedUsers: []matchmaker.MatchedUser{
			{
				UserID: "u1", Username: "alice", SessionID: "s1",
				TicketID: "t1", PartyID: "p1",
				StringProperties:  map[string]string{"region": "us"},
				NumericProperties: map[string]float64{"skill": 10},
			},
			{
				UserID: "u2", Username: "bob", SessionID: "s2",
				TicketID: "t2",
			},
		},
	}
	users := buildMatchmakerMatchedUsers(result, nil)
	require.Len(t, users, 2)
	require.Equal(t, "alice", users[0].Presence.Username)
	require.Equal(t, "p1", users[0].PartyID)

	payload := buildMatchmakerMatchedPayload(result, users, 0)
	require.Equal(t, "t1", payload["ticket"])
	require.Equal(t, "tok-xyz", payload["token"])
	_, hasMatchID := payload["match_id"]
	require.False(t, hasMatchID, "relayed match should use token oneof, not match_id")
	_, hasTicketID := payload["ticket_id"]
	require.False(t, hasTicketID, "legacy ticket_id must not be present")

	self := payload["self"].(matchmakerUserWire)
	require.Equal(t, "u1", self.Presence.UserID)
	require.Equal(t, "s1", self.Presence.SessionID)

	raw, err := json.Marshal(map[string]interface{}{"matchmaker_matched": payload})
	require.NoError(t, err)
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	mm := decoded["matchmaker_matched"].(map[string]interface{})
	require.Equal(t, "t1", mm["ticket"])
	require.Equal(t, "tok-xyz", mm["token"])
	usersArr := mm["users"].([]interface{})
	require.Len(t, usersArr, 2)
	u0 := usersArr[0].(map[string]interface{})
	pres := u0["presence"].(map[string]interface{})
	require.Equal(t, "u1", pres["user_id"])
	require.Equal(t, "s1", pres["session_id"])
}

func TestBuildMatchmakerMatchedPayload_AuthoritativeMatchID(t *testing.T) {
	result := matchmaker.MatchResult{
		MatchID:       "uuid.node",
		Authoritative: true,
		MatchedUsers: []matchmaker.MatchedUser{
			{UserID: "u1", Username: "a", SessionID: "s1", TicketID: "t1"},
		},
	}
	users := buildMatchmakerMatchedUsers(result, nil)
	payload := buildMatchmakerMatchedPayload(result, users, 0)
	require.Equal(t, "uuid.node", payload["match_id"])
	_, hasToken := payload["token"]
	require.False(t, hasToken)
}
