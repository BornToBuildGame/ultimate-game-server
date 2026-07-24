package api

import (
	"context"

	"ultimate-game-server/internal/api/apipb"

	"google.golang.org/protobuf/types/known/emptypb"
)

// SystemServer implements apipb.SystemServiceServer.
type SystemServer struct {
	apipb.UnimplementedSystemServiceServer
}

// NewSystemServer creates a SystemServer.
func NewSystemServer() *SystemServer {
	return &SystemServer{}
}

// Healthcheck returns OK when the process is serving.
func (s *SystemServer) Healthcheck(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
