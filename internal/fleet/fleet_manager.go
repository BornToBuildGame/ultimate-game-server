package fleet

import (
	"context"
	"fmt"
)

// Manager is the runtime FleetManager surface (v1 in-process stub).
type Manager interface {
	Get(ctx context.Context, id string) (map[string]interface{}, error)
	List(ctx context.Context, query string, limit int) ([]map[string]interface{}, error)
	Create(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
	Join(ctx context.Context, id string, input map[string]interface{}) (map[string]interface{}, error)
	Update(ctx context.Context, id string, input map[string]interface{}) error
	Delete(ctx context.Context, id string) error
}

// CallbackHandler receives async fleet create callbacks.
type CallbackHandler interface {
	Callback(ctx context.Context, payload map[string]interface{}) error
}

// Initializer is registered by game modules.
type Initializer interface {
	Manager
	Init(nk any, handler CallbackHandler) error
}

// LocalStub is a minimal in-process FleetManager for API parity.
type LocalStub struct {
	handler CallbackHandler
}

func (s *LocalStub) Init(_ any, handler CallbackHandler) error {
	s.handler = handler
	return nil
}

func (s *LocalStub) Get(_ context.Context, id string) (map[string]interface{}, error) {
	return map[string]interface{}{"id": id, "state": "ready"}, nil
}

func (s *LocalStub) List(_ context.Context, _ string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 {
		limit = 10
	}
	return []map[string]interface{}{}, nil
}

func (s *LocalStub) Create(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	out := map[string]interface{}{"id": fmt.Sprint(input["id"]), "state": "created"}
	if s.handler != nil {
		go func() { _ = s.handler.Callback(ctx, out) }()
	}
	return out, nil
}

func (s *LocalStub) Join(_ context.Context, id string, input map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{"id": id, "joined": true, "input": input}, nil
}

func (s *LocalStub) Update(_ context.Context, id string, input map[string]interface{}) error {
	_ = id
	_ = input
	return nil
}

func (s *LocalStub) Delete(_ context.Context, id string) error {
	_ = id
	return nil
}
