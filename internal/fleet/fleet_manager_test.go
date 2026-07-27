package fleet

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLocalStubCreateInvokesCallback(t *testing.T) {
	stub := NewLocalStub()
	handler := NewLocalFmCallbackHandler()
	if err := stub.Init(nil, handler); err != nil {
		t.Fatal(err)
	}

	var (
		mu     sync.Mutex
		status FmCreateStatus
		gotID  string
	)
	done := make(chan struct{}, 1)
	_, err := stub.Create(context.Background(), 4, []string{"u1"}, nil, map[string]any{"mode": "duel"},
		func(st FmCreateStatus, instanceInfo *InstanceInfo, sessionInfo []*SessionInfo, metadata map[string]any, err error) {
			mu.Lock()
			status = st
			if instanceInfo != nil {
				gotID = instanceInfo.Id
			}
			mu.Unlock()
			done <- struct{}{}
		})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for create callback")
	}
	mu.Lock()
	defer mu.Unlock()
	if status != CreateSuccess || gotID == "" {
		t.Fatalf("status=%v id=%q", status, gotID)
	}
	inst, err := stub.Get(context.Background(), gotID)
	if err != nil || inst.Id != gotID {
		t.Fatalf("get: %v %#v", err, inst)
	}
}
