package api

import (
	"ultimate-game-server/internal/matchmaker"
)

type matchmakerPresenceWire struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	SessionID string `json:"session_id"`
}

type matchmakerUserWire struct {
	Presence          matchmakerPresenceWire `json:"presence"`
	PartyID           string                 `json:"party_id,omitempty"`
	StringProperties  map[string]string      `json:"string_properties,omitempty"`
	NumericProperties map[string]float64     `json:"numeric_properties,omitempty"`
}

// buildMatchmakerMatchedUsers builds reference-shaped matchmaker_matched user entries.
func buildMatchmakerMatchedUsers(result matchmaker.MatchResult, resolveSession func(userID, sessionID string) string) []matchmakerUserWire {
	if resolveSession == nil {
		resolveSession = func(_, sessionID string) string { return sessionID }
	}
	users := make([]matchmakerUserWire, 0, len(result.MatchedUsers))
	if len(result.MatchedUsers) > 0 {
		for _, u := range result.MatchedUsers {
			users = append(users, matchmakerUserWire{
				Presence: matchmakerPresenceWire{
					UserID:    u.UserID,
					Username:  u.Username,
					SessionID: resolveSession(u.UserID, u.SessionID),
				},
				PartyID:           u.PartyID,
				StringProperties:  u.StringProperties,
				NumericProperties: u.NumericProperties,
			})
		}
		return users
	}
	if len(result.Users) > 0 {
		for _, u := range result.Users {
			users = append(users, matchmakerUserWire{
				Presence: matchmakerPresenceWire{
					UserID:    u.UserID,
					Username:  u.Username,
					SessionID: resolveSession(u.UserID, u.SessionID),
				},
			})
		}
		return users
	}
	for idx, pid := range result.PlayerIDs {
		uname := ""
		if idx < len(result.Usernames) {
			uname = result.Usernames[idx]
		}
		users = append(users, matchmakerUserWire{
			Presence: matchmakerPresenceWire{
				UserID:    pid,
				Username:  uname,
				SessionID: resolveSession(pid, ""),
			},
		})
	}
	return users
}

// buildMatchmakerMatchedPayload builds one player's matchmaker_matched payload (reference-shaped).
func buildMatchmakerMatchedPayload(result matchmaker.MatchResult, users []matchmakerUserWire, selfIdx int) map[string]interface{} {
	if selfIdx < 0 || selfIdx >= len(users) {
		return nil
	}
	u := users[selfIdx]
	ticketID := ""
	if len(result.MatchedUsers) > selfIdx && result.MatchedUsers[selfIdx].TicketID != "" {
		ticketID = result.MatchedUsers[selfIdx].TicketID
	} else if result.TicketIDs != nil {
		ticketID = result.TicketIDs[u.Presence.UserID]
	}
	payload := map[string]interface{}{
		"ticket": ticketID,
		"users":  users,
		"self":   u,
	}
	if result.Authoritative {
		payload["match_id"] = result.MatchID
	} else if result.MatchToken != "" {
		payload["token"] = result.MatchToken
	} else if result.MatchID != "" {
		payload["match_id"] = result.MatchID
	}
	return payload
}
