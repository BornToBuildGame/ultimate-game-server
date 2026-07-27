package runtime

import (
	"context"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const cronLeaderKey = "uge:cron:leader"

// CronClusterLock ensures only one node runs a named leadership lease when Redis is configured.
type CronClusterLock struct {
	rdb      *redis.Client
	nodeID   string
	key      string
	leaseTTL time.Duration
}

// NewCronClusterLock creates a Redis lease helper for cron leadership.
func NewCronClusterLock(rdb *redis.Client, nodeID string) *CronClusterLock {
	return NewNamedClusterLock(rdb, nodeID, cronLeaderKey)
}

// NewNamedClusterLock creates a Redis SETNX lease for an arbitrary leadership key.
func NewNamedClusterLock(rdb *redis.Client, nodeID, key string) *CronClusterLock {
	if nodeID == "" {
		nodeID = os.Getenv("UGE_NODE_ID")
	}
	if key == "" {
		key = cronLeaderKey
	}
	return &CronClusterLock{rdb: rdb, nodeID: nodeID, key: key, leaseTTL: 15 * time.Second}
}

// TryAcquire returns true when this node holds (or acquired) the leadership lease.
func (l *CronClusterLock) TryAcquire(ctx context.Context) bool {
	if l == nil || l.rdb == nil || l.nodeID == "" {
		return true
	}
	ok, err := l.rdb.SetNX(ctx, l.key, l.nodeID, l.leaseTTL).Result()
	if err != nil {
		return true // fail open for single-node dev
	}
	if ok {
		return true
	}
	owner, err := l.rdb.Get(ctx, l.key).Result()
	if err != nil {
		return false
	}
	if owner == l.nodeID {
		_ = l.rdb.Expire(ctx, l.key, l.leaseTTL).Err()
		return true
	}
	return false
}

// SetClusterLock attaches a Redis leadership lock to the scheduler.
func (s *CronScheduler) SetClusterLock(lock *CronClusterLock) {
	if s == nil {
		return
	}
	s.clusterLock = lock
}
