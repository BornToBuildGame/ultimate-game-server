package runtime

import (
	"google.golang.org/grpc/codes"

	internalruntime "github.com/BornToBuildGame/ultimate-game-server/internal/runtime"
)

// Context keys for RPC / hook execution (string values match reference engine).
const (
	CtxUserID        = internalruntime.CtxUserID
	CtxUsername      = internalruntime.CtxUsername
	CtxVars          = internalruntime.CtxVars
	CtxClientIP      = internalruntime.CtxClientIP
	CtxEnv           = internalruntime.CtxEnv
	CtxQueryParams   = internalruntime.CtxQueryParams
	CtxHeaders       = internalruntime.CtxHeaders
	CtxExecutionMode = internalruntime.CtxExecutionMode

	// RUNTIME_CTX_USER_ID is a README-compat alias for CtxUserID.
	RUNTIME_CTX_USER_ID = CtxUserID
)

// NewError creates a typed runtime error with a gRPC status code (1–16).
func NewError(msg string, code int) *Error {
	return internalruntime.NewError(msg, code)
}

// CodeFromError extracts a gRPC code from err (default Internal).
func CodeFromError(err error) codes.Code {
	return internalruntime.CodeFromError(err)
}

// ErrBadRequest returns a typed InvalidArgument (3) runtime error.
func ErrBadRequest(msg string) error {
	return internalruntime.ErrBadRequest(msg)
}
