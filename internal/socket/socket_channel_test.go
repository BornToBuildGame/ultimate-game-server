package socket

import (
	"encoding/json"
	"testing"

	"github.com/BornToBuildGame/ultimate-game-server/internal/chat"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
)

func TestBuildChannelJoinAndSendRoom(t *testing.T) {
	gh := &GatewayHandler{
		channels:    make(map[string]map[string]*Session),
		channelMeta: make(map[string]map[string]channelMemberMeta),
	}
	gh.StreamTracker = presence.NewStreamTracker()

	s := &Session{
		ID:       "sess-1",
		UserID:   "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		Username: "alice",
		IsActive: true,
		Send:     make(chan []byte, 8),
	}

	persist := true
	gh.handleChannelJoin(s, "c1", &ChannelJoinPayload{
		Target:      "lobby",
		Type:        chat.ChannelJoinRoom,
		Persistence: &persist,
	})

	var joinResp map[string]interface{}
	select {
	case raw := <-s.Send:
		_ = json.Unmarshal(raw, &joinResp)
	default:
		t.Fatal("expected join response")
	}
	ch, ok := joinResp["channel"].(map[string]interface{})
	if !ok {
		t.Fatalf("joinResp=%v", joinResp)
	}
	channelID, _ := ch["id"].(string)
	if channelID != "2...lobby" {
		t.Fatalf("channelID=%q", channelID)
	}

	gh.handleChannelMessageSend(s, "c2", &ChannelMessageSendPayload{
		ChannelID: channelID,
		Content:   json.RawMessage(`{"text":"hi"}`),
	})

	gotMsg, gotAck := false, false
	for i := 0; i < 2; i++ {
		select {
		case raw := <-s.Send:
			var env map[string]interface{}
			_ = json.Unmarshal(raw, &env)
			if _, ok := env["channel_message"]; ok {
				gotMsg = true
			}
			if _, ok := env["channel_message_ack"]; ok {
				gotAck = true
			}
		default:
		}
	}
	if !gotMsg || !gotAck {
		t.Fatalf("gotMsg=%v gotAck=%v", gotMsg, gotAck)
	}

	gh.handleChannelLeave(s, "c3", &ChannelLeavePayload{ChannelID: channelID})
	if _, ok := gh.channels[channelID]; ok {
		t.Fatal("expected channel removed after last leave")
	}
}
