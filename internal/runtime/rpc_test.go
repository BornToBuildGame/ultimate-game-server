package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestNewError_CodeBounds(t *testing.T) {
	e := NewError("bad", 3)
	if e.Code != codes.InvalidArgument {
		t.Fatalf("code=%v", e.Code)
	}
	e2 := NewError("x", 99)
	if e2.Code != codes.Internal {
		t.Fatalf("expected internal for invalid code, got %v", e2.Code)
	}
	if CodeFromError(e) != codes.InvalidArgument {
		t.Fatal("CodeFromError")
	}
	if CodeFromError(errors.New("plain")) != codes.Internal {
		t.Fatal("plain error should be internal")
	}
}

func TestDispatchRPC_GoPrecedenceAndLowercase(t *testing.T) {
	logger := &nopLogger{}
	m := NewGoRuntimeManager(logger, nil, nil)
	m.Registry().RegisterRPC("Echo", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		uid, _ := ctx.Value(CtxUserID).(string)
		return payload + ":" + uid, nil
	})
	m.Registry().RegisterLuaRPC("echo", "should_not_run")

	res, code, err := m.DispatchRPC(context.Background(), "ECHO", "hi", RPCDispatchOpts{
		UserID: "u1", ExecutionMode: "http",
	}, nil, nil, DefaultRPCConfig())
	if err != nil || code != codes.OK {
		t.Fatalf("err=%v code=%v", err, code)
	}
	if res != "hi:u1" {
		t.Fatalf("got %q", res)
	}
}

func TestDispatchRPC_NotFoundAndPayloadLimit(t *testing.T) {
	m := NewGoRuntimeManager(&nopLogger{}, nil, nil)
	_, code, err := m.DispatchRPC(context.Background(), "missing", "{}", RPCDispatchOpts{}, nil, nil, DefaultRPCConfig())
	if code != codes.NotFound || err == nil {
		t.Fatalf("expected not found, code=%v err=%v", code, err)
	}
	cfg := DefaultRPCConfig()
	cfg.MaxPayloadBytes = 4
	m.Registry().RegisterRPC("x", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return payload, nil
	})
	_, code, err = m.DispatchRPC(context.Background(), "x", "12345", RPCDispatchOpts{}, nil, nil, cfg)
	if code != codes.InvalidArgument || err == nil {
		t.Fatalf("expected invalid argument for large payload")
	}
}

func TestDispatchRPC_TypedError(t *testing.T) {
	m := NewGoRuntimeManager(&nopLogger{}, nil, nil)
	m.Registry().RegisterRPC("fail", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "", NewError("nope", int(codes.PermissionDenied))
	})
	_, code, err := m.DispatchRPC(context.Background(), "fail", "", RPCDispatchOpts{}, nil, nil, DefaultRPCConfig())
	if code != codes.PermissionDenied || err == nil {
		t.Fatalf("code=%v err=%v", code, err)
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("err=%v", err)
	}
}

type nopLogger struct{}

func (nopLogger) Debug(format string, args ...interface{}) {}
func (nopLogger) Info(format string, args ...interface{})  {}
func (nopLogger) Warn(format string, args ...interface{})  {}
func (nopLogger) Error(format string, args ...interface{}) {}
