package engine

import (
	"context"
)

// Module represents a pluggable game feature module in Ultimate Game Engine.
type Module interface {
	// Name returns the unique identifier of the module.
	Name() string
	// Init initializes the module with the server instance.
	Init(ctx context.Context, srv *Server) error
}
