package console

import (
	"context"
	"encoding/json"

	"github.com/BornToBuildGame/ultimate-game-server/internal/console/consolepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GRPCConsole implements consolepb.ConsoleServer.
// HTTP /v2/console is the primary gateway-equivalent surface; gRPC exposes core status and is extended over time.
type GRPCConsole struct {
	consolepb.UnimplementedConsoleServer
	Server *Server
}

func NewGRPCConsole(s *Server) *GRPCConsole {
	return &GRPCConsole{Server: s}
}

func (g *GRPCConsole) GetStatus(ctx context.Context, _ *emptypb.Empty) (*consolepb.Status, error) {
	if g.Server == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	m := map[string]interface{}{"ok": true}
	if g.Server.statusProvider != nil {
		m = g.Server.statusProvider.ConsoleStatus()
	}
	b, _ := json.Marshal(m)
	return &consolepb.Status{Json: string(b)}, nil
}

func (g *GRPCConsole) DeleteAllData(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if g.Server == nil || g.Server.pool == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	if err := DeleteAllData(ctx, g.Server.pool); err != nil {
		return nil, status.Errorf(codes.Internal, "delete all data: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (g *GRPCConsole) ImportAccount(ctx context.Context, in *consolepb.AccountImport) (*emptypb.Empty, error) {
	if g.Server == nil || g.Server.pool == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	if in == nil || in.GetPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "payload_json required")
	}
	var data AccountExport
	if err := json.Unmarshal([]byte(in.GetPayloadJson()), &data); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid payload_json: %v", err)
	}
	if id := in.GetId(); id != "" {
		data.UserID = id
	}
	if data.UserID == "" {
		return nil, status.Error(codes.InvalidArgument, "account id required")
	}
	if err := ImportAccount(ctx, g.Server.pool, &data); err != nil {
		return nil, status.Errorf(codes.Internal, "import account: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (g *GRPCConsole) ImportAccountFull(ctx context.Context, in *consolepb.AccountImport) (*emptypb.Empty, error) {
	if g.Server == nil || g.Server.pool == nil {
		return nil, status.Error(codes.Unavailable, "console not ready")
	}
	if in == nil || in.GetPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "payload_json required")
	}
	var data AccountExport
	if err := json.Unmarshal([]byte(in.GetPayloadJson()), &data); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid payload_json: %v", err)
	}
	if data.UserID == "" {
		data.UserID = in.GetId()
	}
	if data.UserID == "" {
		return nil, status.Error(codes.InvalidArgument, "account id required")
	}
	if err := ImportAccount(ctx, g.Server.pool, &data); err != nil {
		return nil, status.Errorf(codes.Internal, "import account: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (g *GRPCConsole) SatoriListTemplates(ctx context.Context, in *consolepb.SatoriListTemplatesRequest) (*consolepb.SatoriListTemplatesResponse, error) {
	if g.Server == nil || g.Server.satoriClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "Satori not configured")
	}
	limit := int(in.GetLimit())
	templates, cursor, err := g.Server.satoriClient.ConsoleMessageTemplatesList(ctx, limit, in.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "satori list templates: %v", err)
	}
	out := &consolepb.SatoriListTemplatesResponse{Cursor: cursor}
	for _, t := range templates {
		out.Templates = append(out.Templates, &consolepb.SatoriTemplate{
			Id: t.ID, Name: t.Name, Title: t.Title, Value: t.Value,
		})
	}
	return out, nil
}

func (g *GRPCConsole) SatoriSendDirectMessage(ctx context.Context, in *consolepb.SatoriSendDirectMessageRequest) (*consolepb.SatoriSendDirectMessageResponse, error) {
	if g.Server == nil || g.Server.satoriClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "Satori not configured")
	}
	if in.GetIdentityId() == "" {
		return nil, status.Error(codes.InvalidArgument, "identity_id required")
	}
	title, body := in.GetTitle(), in.GetBody()
	if in.GetTemplateId() != "" && title == "" {
		title = in.GetTemplateId()
	}
	if err := g.Server.satoriClient.ConsoleDirectMessageSendSimple(ctx, in.GetIdentityId(), title, body); err != nil {
		return &consolepb.SatoriSendDirectMessageResponse{Ok: false, Error: err.Error()}, nil
	}
	return &consolepb.SatoriSendDirectMessageResponse{Ok: true}, nil
}
