package api

import (
	"context"
	"errors"
	"strings"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/storage"
	"ultimate-game-server/internal/api/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StorageServer struct {
	storagepb.UnimplementedStorageServiceServer
	dbPool   *pgxpool.Pool
	tokenMgr *auth.TokenManager
}

func NewStorageServer(dbPool *pgxpool.Pool, tokenMgr *auth.TokenManager) *StorageServer {
	return &StorageServer{
		dbPool:   dbPool,
		tokenMgr: tokenMgr,
	}
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

func (s *StorageServer) ReadStorageObjects(ctx context.Context, req *storagepb.ReadStorageObjectsRequest) (*storagepb.StorageObjects, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	reqs := make([]storage.ReadRequest, len(req.GetObjectIds()))
	for i, r := range req.GetObjectIds() {
		reqs[i] = storage.ReadRequest{
			Collection: r.GetCollection(),
			Key:        r.GetKey(),
			UserID:     r.GetUserId(),
		}
	}
	objs, err := storage.ReadStorageObjects(ctx, s.dbPool, reqs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read storage objects: %v", err)
	}
	res := make([]*storagepb.StorageObject, len(objs))
	for i, o := range objs {
		res[i] = &storagepb.StorageObject{
			Collection:      o.Collection,
			Key:             o.Key,
			UserId:          o.UserID,
			Value:           o.Value,
			Version:         o.Version,
			PermissionRead:  int32(o.Read),
			PermissionWrite: int32(o.Write),
			CreateTime:      timestamppb.Now(),
			UpdateTime:      timestamppb.Now(),
		}
	}
	return &storagepb.StorageObjects{Objects: res}, nil
}

func (s *StorageServer) WriteStorageObjects(ctx context.Context, req *storagepb.WriteStorageObjectsRequest) (*storagepb.StorageObjectAcks, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	objs := make([]*storage.StorageObject, len(req.GetObjects()))
	for i, w := range req.GetObjects() {
		objs[i] = &storage.StorageObject{
			Collection: w.GetCollection(),
			Key:        w.GetKey(),
			UserID:     userID,
			Value:      w.GetValue(),
			Version:    w.GetVersion(),
			Read:       int16(w.GetPermissionRead()),
			Write:      int16(w.GetPermissionWrite()),
		}
	}
	err = storage.WriteStorageObjects(ctx, s.dbPool, objs)
	if err != nil {
		if errors.Is(err, storage.ErrOCCConflict) {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "failed to write storage objects: %v", err)
	}
	acks := make([]*storagepb.StorageObjectAck, len(objs))
	for i, o := range objs {
		acks[i] = &storagepb.StorageObjectAck{
			Collection: o.Collection,
			Key:        o.Key,
			UserId:     o.UserID,
			Version:    o.Version,
			CreateTime: timestamppb.Now(),
			UpdateTime: timestamppb.Now(),
		}
	}
	return &storagepb.StorageObjectAcks{Acks: acks}, nil
}

func (s *StorageServer) DeleteStorageObjects(ctx context.Context, req *storagepb.DeleteStorageObjectsRequest) (*emptypb.Empty, error) {
	userID, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	reqs := make([]storage.DeleteRequest, len(req.GetObjectIds()))
	for i, d := range req.GetObjectIds() {
		reqs[i] = storage.DeleteRequest{
			Collection: d.GetCollection(),
			Key:        d.GetKey(),
			UserID:     userID,
			Version:    d.GetVersion(),
		}
	}
	err = storage.DeleteStorageObjects(ctx, s.dbPool, reqs)
	if err != nil {
		if errors.Is(err, storage.ErrOCCConflict) {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "failed to delete storage objects: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *StorageServer) ListStorageObjects(ctx context.Context, req *storagepb.ListStorageObjectsRequest) (*storagepb.StorageObjectList, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	objs, nextCursor, err := storage.ListStorageObjects(ctx, s.dbPool, req.GetUserId(), req.GetCollection(), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list storage objects: %v", err)
	}
	res := make([]*storagepb.StorageObject, len(objs))
	for i, o := range objs {
		res[i] = &storagepb.StorageObject{
			Collection:      o.Collection,
			Key:             o.Key,
			UserId:          o.UserID,
			Value:           o.Value,
			Version:         o.Version,
			PermissionRead:  int32(o.Read),
			PermissionWrite: int32(o.Write),
			CreateTime:      timestamppb.Now(),
			UpdateTime:      timestamppb.Now(),
		}
	}
	return &storagepb.StorageObjectList{
		Objects:    res,
		NextCursor: nextCursor,
	}, nil
}
