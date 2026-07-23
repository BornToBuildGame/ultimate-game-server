package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"ultimate-game-server/internal/api/storagepb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StorageServer struct {
	storagepb.UnimplementedStorageServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
}

func NewStorageServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager) *StorageServer {
	return &StorageServer{dbPool: dbPool, tokenMgr: tokenMgr}
}

func (s *StorageServer) authenticate(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(authHeaders[0], "Bearer ")
	claims, err := s.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims.UserID, nil
}

func mapStorageReject(err error) error {
	if errors.Is(err, storage.ErrStorageRejectedVersion) || errors.Is(err, storage.ErrStorageRejectedPermission) {
		return status.Error(codes.InvalidArgument, "Storage write rejected.")
	}
	if errors.Is(err, storage.ErrListCursorInvalid) {
		return status.Error(codes.InvalidArgument, "cursor is invalid")
	}
	return nil
}

func validateJSONObject(value string) error {
	trim := bytes.TrimSpace([]byte(value))
	if len(trim) == 0 || trim[0] != '{' {
		return errors.New("value must be a JSON-encoded object")
	}
	if !json.Valid(trim) {
		return errors.New("value must be valid JSON")
	}
	return nil
}

func toProtoStorageObject(o *storage.StorageObject) *storagepb.StorageObject {
	return &storagepb.StorageObject{
		Collection:      o.Collection,
		Key:             o.Key,
		UserId:          o.UserID,
		Value:           o.Value,
		Version:         o.Version,
		PermissionRead:  int32(o.Read),
		PermissionWrite: int32(o.Write),
		CreateTime:      timestamppb.New(o.CreateTime),
		UpdateTime:      timestamppb.New(o.UpdateTime),
	}
}

func (s *StorageServer) ReadStorageObjects(ctx context.Context, req *storagepb.ReadStorageObjectsRequest) (*storagepb.StorageObjects, error) {
	callerStr, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	caller, err := uuid.Parse(callerStr)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid user id")
	}
	reqs := make([]storage.ReadRequest, 0, len(req.GetObjectIds()))
	for _, r := range req.GetObjectIds() {
		if r.GetCollection() == "" || r.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "collection and key required")
		}
		uid := r.GetUserId()
		if uid != "" {
			parsed, err := uuid.Parse(uid)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid user_id")
			}
			if parsed == uuid.Nil {
				return nil, status.Error(codes.InvalidArgument, "user_id must not be nil UUID")
			}
		}
		reqs = append(reqs, storage.ReadRequest{Collection: r.GetCollection(), Key: r.GetKey(), UserID: uid})
	}
	objs, err := storage.ReadStorageObjects(ctx, s.dbPool, caller, reqs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read storage objects: %v", err)
	}
	res := make([]*storagepb.StorageObject, len(objs))
	for i, o := range objs {
		res[i] = toProtoStorageObject(o)
	}
	return &storagepb.StorageObjects{Objects: res}, nil
}

func (s *StorageServer) WriteStorageObjects(ctx context.Context, req *storagepb.WriteStorageObjectsRequest) (*storagepb.StorageObjectAcks, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	objs := make([]*storage.StorageObject, 0, len(req.GetObjects()))
	for _, w := range req.GetObjects() {
		if w.GetCollection() == "" || w.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "collection and key required")
		}
		if err := validateJSONObject(w.GetValue()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		read := int16(w.GetPermissionRead())
		write := int16(w.GetPermissionWrite())
		if read < 0 || read > 2 {
			return nil, status.Error(codes.InvalidArgument, "permission_read must be 0, 1, or 2")
		}
		if write < 0 || write > 1 {
			return nil, status.Error(codes.InvalidArgument, "permission_write must be 0 or 1")
		}
		// Proto int32 defaults to 0; treat both-zero as omitted → defaults 1 (reference wrappers).
		if w.GetPermissionRead() == 0 && w.GetPermissionWrite() == 0 {
			read, write = 1, 1
		}
		objs = append(objs, &storage.StorageObject{
			Collection: w.GetCollection(),
			Key:        w.GetKey(),
			UserID:     userID,
			Value:      w.GetValue(),
			Version:    w.GetVersion(),
			Read:       read,
			Write:      write,
		})
	}
	acks, err := storage.WriteStorageObjects(ctx, s.dbPool, false, objs)
	if err != nil {
		if mapped := mapStorageReject(err); mapped != nil {
			return nil, mapped
		}
		return nil, status.Errorf(codes.Internal, "failed to write storage objects: %v", err)
	}
	out := make([]*storagepb.StorageObjectAck, len(acks))
	for i, a := range acks {
		out[i] = &storagepb.StorageObjectAck{
			Collection: a.Collection,
			Key:        a.Key,
			UserId:     a.UserID,
			Version:    a.Version,
			CreateTime: timestamppb.New(a.CreateTime),
			UpdateTime: timestamppb.New(a.UpdateTime),
		}
	}
	return &storagepb.StorageObjectAcks{Acks: out}, nil
}

func (s *StorageServer) DeleteStorageObjects(ctx context.Context, req *storagepb.DeleteStorageObjectsRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	reqs := make([]storage.DeleteRequest, len(req.GetObjectIds()))
	for i, d := range req.GetObjectIds() {
		if d.GetCollection() == "" || d.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "collection and key required")
		}
		reqs[i] = storage.DeleteRequest{
			Collection: d.GetCollection(),
			Key:        d.GetKey(),
			UserID:     userID,
			Version:    d.GetVersion(),
		}
	}
	err = storage.DeleteStorageObjects(ctx, s.dbPool, false, reqs)
	if err != nil {
		if mapped := mapStorageReject(err); mapped != nil {
			return nil, mapped
		}
		return nil, status.Errorf(codes.Internal, "failed to delete storage objects: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *StorageServer) ListStorageObjects(ctx context.Context, req *storagepb.ListStorageObjectsRequest) (*storagepb.StorageObjectList, error) {
	callerStr, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	caller, err := uuid.Parse(callerStr)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid user id")
	}
	if req.GetCollection() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection required")
	}
	limit := int(req.GetLimit())
	var owner *uuid.UUID
	if uid := req.GetUserId(); uid != "" {
		parsed, err := uuid.Parse(uid)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid user_id")
		}
		owner = &parsed
	}
	list, err := storage.ListStorageObjects(ctx, s.dbPool, caller, owner, req.GetCollection(), limit, req.GetCursor())
	if err != nil {
		if mapped := mapStorageReject(err); mapped != nil {
			return nil, mapped
		}
		return nil, status.Errorf(codes.Internal, "failed to list storage objects: %v", err)
	}
	res := make([]*storagepb.StorageObject, len(list.Objects))
	for i, o := range list.Objects {
		res[i] = toProtoStorageObject(o)
	}
	return &storagepb.StorageObjectList{Objects: res, NextCursor: list.Cursor}, nil
}
