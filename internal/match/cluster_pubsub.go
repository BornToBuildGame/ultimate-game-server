package match

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// Channel for authoritative MatchInput forwarded to a specific node.
	clusterInputChannelPrefix = "uge:match:input:"
	// Channel for authoritative MatchSignal forwarded to a specific node.
	clusterSignalChannelPrefix = "uge:match:signal:"
	// RelayFanoutChannel is the Redis Pub/Sub channel for client-relayed match fan-out.
	RelayFanoutChannel = "uge:match:relay"
)

// ClusterInputMessage is published when ForwardInput must reach a remote node.
type ClusterInputMessage struct {
	SourceNode string     `json:"source_node"`
	TargetNode string     `json:"target_node"`
	MatchID    string     `json:"match_id"`
	Input      MatchInput `json:"input"`
}

// ClusterSignalMessage is published when ForwardSignal must reach a remote node.
type ClusterSignalMessage struct {
	SourceNode string `json:"source_node"`
	TargetNode string `json:"target_node"`
	MatchID    string `json:"match_id"`
	Data       string `json:"data"`
}

// RelayFanoutMessage carries client-relayed match payloads (data / presence) across nodes.
type RelayFanoutMessage struct {
	SourceNode string          `json:"source_node"`
	MatchID    string          `json:"match_id"`
	Kind       string          `json:"kind"` // "match_data" | "match_presence"
	Payload    json.RawMessage `json:"payload"`
}

// ResolveNodeID returns a stable-ish node label for cluster metadata and match ID suffixes.
func ResolveNodeID() string {
	if v := os.Getenv("UGE_NODE_ID"); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "node-local"
}

// NewPubSubForwarder publishes MatchInput destined for peer nodes; peers subscribe via StartClusterInputListener.
func NewPubSubForwarder(rdb *redis.Client, sourceNode string, logger *zap.Logger) func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error {
	if rdb == nil {
		return nil
	}
	return func(ctx context.Context, targetNodeID, matchID string, input MatchInput) error {
		msg := ClusterInputMessage{
			SourceNode: sourceNode,
			TargetNode: targetNodeID,
			MatchID:    matchID,
			Input:      input,
		}
		b, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		channel := clusterInputChannelPrefix + targetNodeID
		if err := rdb.Publish(ctx, channel, b).Err(); err != nil {
			if logger != nil {
				logger.Warn("cluster input publish failed",
					zap.String("channel", channel),
					zap.String("match_id", matchID),
					zap.Error(err))
			}
			return fmt.Errorf("publish cluster input: %w", err)
		}
		return nil
	}
}

// NewPubSubSignalForwarder publishes MatchSignal payloads destined for peer nodes.
func NewPubSubSignalForwarder(rdb *redis.Client, sourceNode string, logger *zap.Logger) func(ctx context.Context, targetNodeID, matchID string, data string) error {
	if rdb == nil {
		return nil
	}
	return func(ctx context.Context, targetNodeID, matchID string, data string) error {
		msg := ClusterSignalMessage{
			SourceNode: sourceNode,
			TargetNode: targetNodeID,
			MatchID:    matchID,
			Data:       data,
		}
		b, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		channel := clusterSignalChannelPrefix + targetNodeID
		if err := rdb.Publish(ctx, channel, b).Err(); err != nil {
			if logger != nil {
				logger.Warn("cluster signal publish failed",
					zap.String("channel", channel),
					zap.String("match_id", matchID),
					zap.Error(err))
			}
			return fmt.Errorf("publish cluster signal: %w", err)
		}
		return nil
	}
}

// StartClusterInputListener subscribes to this node's input channel and submits to local match loops.
func (r *Router) StartClusterInputListener(ctx context.Context) {
	r.mu.RLock()
	rdb := r.rdb
	nodeID := r.nodeID
	logger := r.ZapLogger
	r.mu.RUnlock()

	if rdb == nil || nodeID == "" {
		return
	}

	channel := clusterInputChannelPrefix + nodeID
	pubsub := rdb.Subscribe(ctx, channel)
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var payload ClusterInputMessage
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
					if logger != nil {
						logger.Warn("invalid cluster input payload", zap.Error(err))
					}
					continue
				}
				if payload.TargetNode != "" && payload.TargetNode != nodeID {
					continue
				}
				if payload.SourceNode == nodeID {
					continue
				}
				r.mu.RLock()
				loop, exists := r.matches[payload.MatchID]
				r.mu.RUnlock()
				if !exists {
					if logger != nil {
						logger.Debug("cluster input for unknown local match",
							zap.String("match_id", payload.MatchID))
					}
					continue
				}
				loop.SubmitInput(payload.Input)
			}
		}
	}()
	if logger != nil {
		logger.Info("cluster input listener started", zap.String("channel", channel), zap.String("node", nodeID))
	}
}

// StartClusterSignalListener subscribes to this node's signal channel and submits to local match loops.
func (r *Router) StartClusterSignalListener(ctx context.Context) {
	r.mu.RLock()
	rdb := r.rdb
	nodeID := r.nodeID
	logger := r.ZapLogger
	r.mu.RUnlock()

	if rdb == nil || nodeID == "" {
		return
	}

	channel := clusterSignalChannelPrefix + nodeID
	pubsub := rdb.Subscribe(ctx, channel)
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var payload ClusterSignalMessage
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
					if logger != nil {
						logger.Warn("invalid cluster signal payload", zap.Error(err))
					}
					continue
				}
				if payload.TargetNode != "" && payload.TargetNode != nodeID {
					continue
				}
				if payload.SourceNode == nodeID {
					continue
				}
				r.mu.RLock()
				loop, exists := r.matches[payload.MatchID]
				r.mu.RUnlock()
				if !exists {
					if logger != nil {
						logger.Debug("cluster signal for unknown local match",
							zap.String("match_id", payload.MatchID))
					}
					continue
				}
				loop.EnqueueSignal(payload.Data)
			}
		}
	}()
	if logger != nil {
		logger.Info("cluster signal listener started", zap.String("channel", channel), zap.String("node", nodeID))
	}
}

const (
	defaultTickSoftTimeout = 50 * time.Millisecond
	maxMatchStateBytes     = 1 << 20 // 1 MiB
)

// PublishRelayFanout publishes a relayed match envelope for other nodes.
func PublishRelayFanout(ctx context.Context, rdb *redis.Client, sourceNode, matchID, kind string, envelope []byte) error {
	if rdb == nil {
		return nil
	}
	msg := RelayFanoutMessage{
		SourceNode: sourceNode,
		MatchID:    matchID,
		Kind:       kind,
		Payload:    envelope,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return rdb.Publish(ctx, RelayFanoutChannel, b).Err()
}
