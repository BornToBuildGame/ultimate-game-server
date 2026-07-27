package fleet

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Manager is the runtime FleetManager surface (reference-shaped).
type Manager interface {
	Get(ctx context.Context, id string) (*InstanceInfo, error)
	List(ctx context.Context, query string, limit int, previousCursor string) ([]*InstanceInfo, string, error)
	Create(ctx context.Context, maxPlayers int, userIds []string, latencies []FleetUserLatencies, metadata map[string]any, callback FmCreateCallbackFn) (map[string]string, error)
	Join(ctx context.Context, id string, userIds []string, metadata map[string]string) (*JoinInfo, error)
}

// Initializer is registered by game modules.
type Initializer interface {
	Manager
	Init(nk any, handler FmCallbackHandler) error
	Update(ctx context.Context, id string, playerCount int, metadata map[string]any) error
	Delete(ctx context.Context, id string) error
}

// LocalStub is a minimal in-process FleetManager for API parity.
type LocalStub struct {
	mu      sync.Mutex
	handler FmCallbackHandler
	byID    map[string]*InstanceInfo
}

// NewLocalStub creates an empty in-process fleet stub.
func NewLocalStub() *LocalStub {
	return &LocalStub{byID: make(map[string]*InstanceInfo)}
}

func (s *LocalStub) Init(_ any, handler FmCallbackHandler) error {
	s.handler = handler
	if s.byID == nil {
		s.byID = make(map[string]*InstanceInfo)
	}
	return nil
}

func (s *LocalStub) Get(_ context.Context, id string) (*InstanceInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("instance not found")
	}
	cp := *inst
	return &cp, nil
}

func (s *LocalStub) List(_ context.Context, _ string, limit int, _ string) ([]*InstanceInfo, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 10
	}
	out := make([]*InstanceInfo, 0, len(s.byID))
	for _, inst := range s.byID {
		cp := *inst
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, "", nil
}

func (s *LocalStub) Create(ctx context.Context, maxPlayers int, userIds []string, _ []FleetUserLatencies, metadata map[string]any, callback FmCreateCallbackFn) (map[string]string, error) {
	id := uuid.NewString()
	inst := &InstanceInfo{
		Id: id,
		ConnectionInfo: &ConnectionInfo{
			IpAddress: "127.0.0.1",
			DnsName:   "localhost",
			Port:      7350,
		},
		CreateTime:  time.Now().UTC(),
		PlayerCount: 0,
		Status:      "ready",
		Metadata:    metadata,
	}
	s.mu.Lock()
	if s.byID == nil {
		s.byID = make(map[string]*InstanceInfo)
	}
	s.byID[id] = inst
	s.mu.Unlock()

	var sessions []*SessionInfo
	for _, uid := range userIds {
		sessions = append(sessions, &SessionInfo{UserId: uid, SessionId: uuid.NewString()})
	}

	cbID := ""
	if s.handler != nil {
		cbID = s.handler.GenerateCallbackId()
		if callback != nil {
			s.handler.SetCallback(cbID, callback)
		}
	}
	go func() {
		if s.handler != nil && cbID != "" {
			s.handler.InvokeCallback(cbID, CreateSuccess, inst, sessions, metadata, nil)
			return
		}
		if callback != nil {
			callback(CreateSuccess, inst, sessions, metadata, nil)
		}
	}()

	_ = maxPlayers
	_ = ctx
	out := map[string]string{"id": id}
	if cbID != "" {
		out["callback_id"] = cbID
	}
	return out, nil
}

func (s *LocalStub) Join(_ context.Context, id string, userIds []string, _ map[string]string) (*JoinInfo, error) {
	inst, err := s.Get(context.Background(), id)
	if err != nil {
		return nil, err
	}
	sessions := make([]*SessionInfo, 0, len(userIds))
	for _, uid := range userIds {
		sessions = append(sessions, &SessionInfo{UserId: uid, SessionId: uuid.NewString()})
	}
	return &JoinInfo{InstanceInfo: inst, SessionInfo: sessions}, nil
}

func (s *LocalStub) Update(_ context.Context, id string, playerCount int, metadata map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("instance not found")
	}
	inst.PlayerCount = playerCount
	if metadata != nil {
		inst.Metadata = metadata
	}
	return nil
}

func (s *LocalStub) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byID, id)
	return nil
}
