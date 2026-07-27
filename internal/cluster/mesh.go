package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	streamBroadcastChannel   = "uge:mesh:stream:all"
	presenceBroadcastChannel = "uge:mesh:presence:all"
	chatChannel              = "uge:mesh:chat"
	partyBroadcastChannel    = "uge:mesh:party:all"
)

// StreamSendMessage fans out stream payloads to peer nodes.
type StreamSendMessage struct {
	SourceNode  string   `json:"source_node"`
	Mode        int16    `json:"mode"`
	Subject     string   `json:"subject"`
	Subcontext  string   `json:"subcontext"`
	Label       string   `json:"label"`
	Data        string   `json:"data"`
	SessionIDs  []string `json:"session_ids,omitempty"`
}

// PresenceEventMessage carries cross-node status join/leave/update payloads.
type PresenceEventMessage struct {
	SourceNode string          `json:"source_node"`
	Kind       string          `json:"kind"` // join|leave|update
	Payload    json.RawMessage `json:"payload"`
}

// ChatMessage carries channel chat payloads across nodes.
type ChatMessage struct {
	SourceNode string          `json:"source_node"`
	ChannelID  string          `json:"channel_id"`
	Payload    json.RawMessage `json:"payload"`
}

// PartyMessage carries party events across nodes.
type PartyMessage struct {
	SourceNode string          `json:"source_node"`
	PartyID    string          `json:"party_id"`
	Payload    json.RawMessage `json:"payload"`
}

// Mesh routes realtime social payloads across nodes via Redis Pub/Sub.
type Mesh struct {
	rdb        *redis.Client
	nodeID     string
	logger     *zap.Logger
	onStream   func(StreamSendMessage)
	onPresence func(PresenceEventMessage)
	onChat     func(ChatMessage)
	onParty    func(PartyMessage)
	cancel     context.CancelFunc
}

// NewMesh creates a cluster mesh router.
func NewMesh(rdb *redis.Client, nodeID string, logger *zap.Logger) *Mesh {
	return &Mesh{rdb: rdb, nodeID: nodeID, logger: logger}
}

func (m *Mesh) SetStreamHandler(fn func(StreamSendMessage))   { m.onStream = fn }
func (m *Mesh) SetPresenceHandler(fn func(PresenceEventMessage)) { m.onPresence = fn }
func (m *Mesh) SetChatHandler(fn func(ChatMessage))           { m.onChat = fn }
func (m *Mesh) SetPartyHandler(fn func(PartyMessage))         { m.onParty = fn }

// Start subscribes to node-specific mesh channels.
func (m *Mesh) Start(ctx context.Context) {
	if m == nil || m.rdb == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	channels := []string{
		streamBroadcastChannel,
		presenceBroadcastChannel,
		chatChannel,
		partyBroadcastChannel,
	}
	sub := m.rdb.Subscribe(ctx, channels...)
	go func() {
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				_ = sub.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				m.dispatch(msg.Channel, []byte(msg.Payload))
			}
		}
	}()
}

func (m *Mesh) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

func (m *Mesh) dispatch(channel string, payload []byte) {
	switch channel {
	case streamBroadcastChannel:
		var msg StreamSendMessage
		if json.Unmarshal(payload, &msg) == nil && msg.SourceNode != m.nodeID && m.onStream != nil {
			m.onStream(msg)
		}
	case presenceBroadcastChannel:
		var msg PresenceEventMessage
		if json.Unmarshal(payload, &msg) == nil && msg.SourceNode != m.nodeID && m.onPresence != nil {
			m.onPresence(msg)
		}
	case chatChannel:
		var msg ChatMessage
		if json.Unmarshal(payload, &msg) == nil && msg.SourceNode != m.nodeID && m.onChat != nil {
			m.onChat(msg)
		}
	case partyBroadcastChannel:
		var msg PartyMessage
		if json.Unmarshal(payload, &msg) == nil && msg.SourceNode != m.nodeID && m.onParty != nil {
			m.onParty(msg)
		}
	}
}

func (m *Mesh) publish(ctx context.Context, channel string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return m.rdb.Publish(ctx, channel, b).Err()
}

// PublishStreamBroadcast publishes stream payloads to all peer nodes.
func (m *Mesh) PublishStreamBroadcast(ctx context.Context, msg StreamSendMessage) error {
	if m == nil || m.rdb == nil {
		return fmt.Errorf("mesh not configured")
	}
	msg.SourceNode = m.nodeID
	return m.publish(ctx, streamBroadcastChannel, msg)
}

// PublishStreamSend publishes a stream payload to a target node.
func (m *Mesh) PublishStreamSend(ctx context.Context, targetNode string, msg StreamSendMessage) error {
	_ = targetNode
	return m.PublishStreamBroadcast(ctx, msg)
}

// PublishPresence broadcasts a presence event to all peer nodes.
func (m *Mesh) PublishPresence(ctx context.Context, msg PresenceEventMessage) error {
	if m == nil || m.rdb == nil {
		return fmt.Errorf("mesh not configured")
	}
	msg.SourceNode = m.nodeID
	return m.publish(ctx, presenceBroadcastChannel, msg)
}

// PublishChat broadcasts chat to all nodes.
func (m *Mesh) PublishChat(ctx context.Context, msg ChatMessage) error {
	if m == nil || m.rdb == nil {
		return fmt.Errorf("mesh not configured")
	}
	msg.SourceNode = m.nodeID
	return m.publish(ctx, chatChannel, msg)
}

// PublishParty broadcasts a party event to all peer nodes.
func (m *Mesh) PublishParty(ctx context.Context, msg PartyMessage) error {
	if m == nil || m.rdb == nil {
		return fmt.Errorf("mesh not configured")
	}
	msg.SourceNode = m.nodeID
	return m.publish(ctx, partyBroadcastChannel, msg)
}
