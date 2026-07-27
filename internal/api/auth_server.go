package api

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"ultimate-game-server/internal/api/apipb"
	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AuthServer implements apipb.AuthenticationServiceServer.
type AuthServer struct {
	apipb.UnimplementedAuthenticationServiceServer
	api *Server
}

// NewAuthServer wraps the HTTP API server for shared session/DB state.
func NewAuthServer(api *Server) *AuthServer {
	return &AuthServer{api: api}
}

func (s *AuthServer) issue(user *auth.User, created bool) (*apipb.Session, error) {
	access, refresh, err := s.api.tokenMgr.GenerateSession(user.ID.String(), user.Username)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to issue session")
	}
	s.api.sessReg.RegisterSession(user.ID.String(), refresh, "")
	return &apipb.Session{
		Token: access, RefreshToken: refresh, Created: created,
		UserId: user.ID.String(), Username: user.Username,
	}, nil
}

func (s *AuthServer) bearerClaims(ctx context.Context) (*auth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	headers := md.Get("authorization")
	if len(headers) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}
	tokenStr := strings.TrimPrefix(headers[0], "Bearer ")
	claims, err := s.api.tokenMgr.VerifyToken(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	if claims.ID != "" {
		denied, _ := s.api.sessReg.Store().IsAccessJTIBlacklisted(ctx, claims.ID)
		if denied {
			return nil, status.Error(codes.Unauthenticated, "token revoked")
		}
	}
	return claims, nil
}

func (s *AuthServer) AuthenticateEmail(ctx context.Context, req *apipb.AuthenticateEmailRequest) (*apipb.Session, error) {
	var user *auth.User
	var err error
	created := false
	if req.GetCreate() {
		user, err = auth.RegisterEmail(ctx, s.api.dbPool, req.GetUsername(), req.GetEmail(), req.GetPassword(), req.GetDisplayName())
		created = true
	} else {
		user, err = auth.AuthenticateEmail(ctx, s.api.dbPool, req.GetEmail(), req.GetPassword())
	}
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) AuthenticateDevice(ctx context.Context, req *apipb.AuthenticateDeviceRequest) (*apipb.Session, error) {
	// Proto3 bool defaults to false; mirror REST default create=true for find-or-create.
	opts := auth.AuthOptions{Create: true, Username: req.GetUsername()}
	user, created, err := auth.AuthenticateDevice(ctx, s.api.dbPool, req.GetId(), opts)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) socialAuth(ctx context.Context, provider, token, username string, create bool) (*apipb.Session, error) {
	var verifyErr error
	var providerID string
	switch provider {
	case "apple":
		providerID, verifyErr = auth.VerifyAppleToken(ctx, token)
	case "google":
		providerID, verifyErr = auth.VerifyGoogleToken(ctx, token)
	case "facebook":
		providerID, verifyErr = auth.VerifyFacebookToken(ctx, token)
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported provider")
	}
	if verifyErr != nil {
		return nil, status.Error(codes.Unauthenticated, verifyErr.Error())
	}
	user, created, err := auth.AuthenticateSocialWithOpts(ctx, s.api.dbPool, provider, providerID, auth.AuthOptions{Create: create, Username: username})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) AuthenticateApple(ctx context.Context, req *apipb.AuthenticateAppleRequest) (*apipb.Session, error) {
	return s.socialAuth(ctx, "apple", req.GetToken(), req.GetUsername(), req.GetCreate())
}
func (s *AuthServer) AuthenticateGoogle(ctx context.Context, req *apipb.AuthenticateGoogleRequest) (*apipb.Session, error) {
	return s.socialAuth(ctx, "google", req.GetToken(), req.GetUsername(), req.GetCreate())
}
func (s *AuthServer) AuthenticateFacebook(ctx context.Context, req *apipb.AuthenticateFacebookRequest) (*apipb.Session, error) {
	return s.socialAuth(ctx, "facebook", req.GetToken(), req.GetUsername(), req.GetCreate())
}

func (s *AuthServer) AuthenticateSteam(ctx context.Context, req *apipb.AuthenticateSteamRequest) (*apipb.Session, error) {
	id, err := auth.VerifySteamTicket(ctx, string(req.GetTicket()))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	user, created, err := auth.AuthenticateSocialWithOpts(ctx, s.api.dbPool, "steam", id, auth.AuthOptions{Create: req.GetCreate(), Username: req.GetUsername()})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) AuthenticateGameCenter(ctx context.Context, req *apipb.AuthenticateGameCenterRequest) (*apipb.Session, error) {
	id, err := auth.VerifyGameCenterSignature(ctx, auth.GameCenterCredentials{
		PlayerID: req.GetPlayerId(), BundleID: req.GetBundleId(), Timestamp: req.GetTimestamp(),
		Salt: req.GetSalt(), Signature: req.GetSignature(), PublicKeyURL: req.GetPublicKeyUrl(),
	})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	user, created, err := auth.AuthenticateSocialWithOpts(ctx, s.api.dbPool, "gamecenter", id, auth.AuthOptions{Create: req.GetCreate(), Username: req.GetUsername()})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) AuthenticateFacebookInstantGame(ctx context.Context, req *apipb.AuthenticateFacebookInstantGameRequest) (*apipb.Session, error) {
	id, err := auth.VerifyFacebookInstantGame(ctx, req.GetSignedPlayerInfo())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	user, created, err := auth.AuthenticateSocialWithOpts(ctx, s.api.dbPool, "facebookinstantgame", id, auth.AuthOptions{Create: req.GetCreate(), Username: req.GetUsername()})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) AuthenticateCustom(ctx context.Context, req *apipb.AuthenticateCustomRequest) (*apipb.Session, error) {
	user, created, err := auth.AuthenticateCustomWithOpts(ctx, s.api.dbPool, req.GetId(), auth.AuthOptions{Create: req.GetCreate(), Username: req.GetUsername()})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(user, created)
}

func (s *AuthServer) SessionRefresh(ctx context.Context, req *apipb.SessionRefreshRequest) (*apipb.Session, error) {
	refresh := req.GetRefreshToken()
	if refresh == "" {
		refresh = req.GetToken()
	}
	userID, detected, err := s.api.sessReg.ValidateAndRotateSession(refresh)
	if err != nil {
		if detected {
			return nil, status.Error(codes.Aborted, "compromised token")
		}
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	var username string
	var disableTime time.Time
	if err := s.api.dbPool.QueryRow(ctx, `SELECT username, disable_time FROM users WHERE id = $1`, userID).Scan(&username, &disableTime); err != nil {
		return nil, status.Error(codes.Unauthenticated, "user not found")
	}
	if disableTime.After(time.Unix(0, 0)) {
		s.api.sessReg.RevokeAllSessions(userID)
		return nil, status.Error(codes.PermissionDenied, "account disabled")
	}
	access, newRefresh, err := s.api.tokenMgr.GenerateSession(userID, username)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to issue session")
	}
	s.api.sessReg.RegisterSession(userID, newRefresh, refresh)
	return &apipb.Session{Token: access, RefreshToken: newRefresh, UserId: userID, Username: username}, nil
}

func (s *AuthServer) SessionLogout(ctx context.Context, req *apipb.SessionLogoutRequest) (*emptypb.Empty, error) {
	access := req.GetToken()
	userID := ""
	if access != "" {
		if claims, err := s.api.tokenMgr.VerifyTokenIgnoreExpiry(access); err == nil && claims != nil {
			userID = claims.UserID
		}
	} else if claims, err := s.bearerClaims(ctx); err == nil {
		userID = claims.UserID
	}
	if req.GetRefreshToken() == "" && userID != "" {
		_ = auth.LogoutAll(ctx, s.api.sessReg.Store(), s.api.tokenMgr, userID, access)
	} else {
		_ = auth.Logout(ctx, s.api.sessReg.Store(), s.api.tokenMgr, access, req.GetRefreshToken())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) GetAccount(ctx context.Context, _ *emptypb.Empty) (*apipb.Account, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, err := uuid.Parse(claims.UserID)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid user")
	}
	acct, err := auth.GetAccount(ctx, s.api.dbPool, uid)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	email, custom := "", ""
	if acct.Email != nil {
		email = *acct.Email
	}
	if acct.CustomID != nil {
		custom = *acct.CustomID
	}
	devices := make([]*apipb.Device, 0, len(acct.Devices))
	for _, d := range acct.Devices {
		devices = append(devices, &apipb.Device{Id: d})
	}
	walletMap, _ := economy.GetWallet(ctx, s.api.dbPool, claims.UserID)
	walletJSON, _ := json.Marshal(walletMap)
	return &apipb.Account{
		User: &apipb.User{
			Id: acct.ID.String(), Username: acct.Username, DisplayName: acct.DisplayName,
			AvatarUrl: acct.AvatarURL, LangTag: acct.LangTag, Location: acct.Location,
			Timezone: acct.Timezone, Metadata: acct.Metadata,
			CreateTime: timestamppb.New(acct.CreateTime), UpdateTime: timestamppb.New(acct.UpdateTime),
		},
		Devices: devices, Email: email, CustomId: custom, Wallet: string(walletJSON),
	}, nil
}

func (s *AuthServer) UpdateAccount(ctx context.Context, req *apipb.UpdateAccountRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	upd := auth.AccountUpdate{}
	set := func(v string, dest **string) {
		if v != "" {
			x := v
			*dest = &x
		}
	}
	set(req.GetUsername(), &upd.Username)
	set(req.GetDisplayName(), &upd.DisplayName)
	set(req.GetAvatarUrl(), &upd.AvatarURL)
	set(req.GetLangTag(), &upd.LangTag)
	set(req.GetLocation(), &upd.Location)
	set(req.GetTimezone(), &upd.Timezone)
	set(req.GetMetadata(), &upd.Metadata)
	if err := auth.UpdateAccount(ctx, s.api.dbPool, uid, upd); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) DeleteAccount(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.SoftDeleteUser(ctx, s.api.dbPool, uid); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	_ = leaderboard.RecordsDeleteAll(ctx, s.api.dbPool, claims.UserID)
	s.api.sessReg.RevokeAllSessions(claims.UserID)
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) LinkEmail(ctx context.Context, req *apipb.AuthenticateEmailRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkEmail(ctx, s.api.dbPool, uid, req.GetEmail(), req.GetPassword()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) LinkDevice(ctx context.Context, req *apipb.AuthenticateDeviceRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.AddDevice(ctx, s.api.dbPool, uid, req.GetId(), "{}", nil); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) linkSocial(ctx context.Context, provider, token string) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	var providerID string
	switch provider {
	case "apple":
		providerID, err = auth.VerifyAppleToken(ctx, token)
	case "google":
		providerID, err = auth.VerifyGoogleToken(ctx, token)
	case "facebook":
		providerID, err = auth.VerifyFacebookToken(ctx, token)
	}
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkProvider(ctx, s.api.dbPool, uid, provider, providerID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) LinkApple(ctx context.Context, req *apipb.AuthenticateAppleRequest) (*emptypb.Empty, error) {
	return s.linkSocial(ctx, "apple", req.GetToken())
}
func (s *AuthServer) LinkGoogle(ctx context.Context, req *apipb.AuthenticateGoogleRequest) (*emptypb.Empty, error) {
	return s.linkSocial(ctx, "google", req.GetToken())
}
func (s *AuthServer) LinkFacebook(ctx context.Context, req *apipb.AuthenticateFacebookRequest) (*emptypb.Empty, error) {
	return s.linkSocial(ctx, "facebook", req.GetToken())
}
func (s *AuthServer) LinkSteam(ctx context.Context, req *apipb.AuthenticateSteamRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	id, err := auth.VerifySteamTicket(ctx, string(req.GetTicket()))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkProvider(ctx, s.api.dbPool, uid, "steam", id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}
func (s *AuthServer) LinkCustom(ctx context.Context, req *apipb.AuthenticateCustomRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkProvider(ctx, s.api.dbPool, uid, "custom", req.GetId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) LinkGameCenter(ctx context.Context, req *apipb.AuthenticateGameCenterRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	id, err := auth.VerifyGameCenterSignature(ctx, auth.GameCenterCredentials{
		PlayerID: req.GetPlayerId(), BundleID: req.GetBundleId(), Timestamp: req.GetTimestamp(),
		Salt: req.GetSalt(), Signature: req.GetSignature(), PublicKeyURL: req.GetPublicKeyUrl(),
	})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkProvider(ctx, s.api.dbPool, uid, "gamecenter", id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) LinkFacebookInstantGame(ctx context.Context, req *apipb.AuthenticateFacebookInstantGameRequest) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	id, err := auth.VerifyFacebookInstantGame(ctx, req.GetSignedPlayerInfo())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	uid, _ := uuid.Parse(claims.UserID)
	if err := auth.LinkProvider(ctx, s.api.dbPool, uid, "facebookinstantgame", id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) unlink(ctx context.Context, provider, deviceID string) (*emptypb.Empty, error) {
	claims, err := s.bearerClaims(ctx)
	if err != nil {
		return nil, err
	}
	uid, _ := uuid.Parse(claims.UserID)
	if provider == "device" {
		if err := auth.UnlinkDevice(ctx, s.api.dbPool, uid, deviceID); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return &emptypb.Empty{}, nil
	}
	if err := auth.UnlinkProvider(ctx, s.api.dbPool, uid, provider); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *AuthServer) UnlinkEmail(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "email", "")
}
func (s *AuthServer) UnlinkDevice(ctx context.Context, req *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "device", req.GetId())
}
func (s *AuthServer) UnlinkApple(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "apple", "")
}
func (s *AuthServer) UnlinkGoogle(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "google", "")
}
func (s *AuthServer) UnlinkFacebook(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "facebook", "")
}
func (s *AuthServer) UnlinkSteam(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "steam", "")
}
func (s *AuthServer) UnlinkCustom(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "custom", "")
}
func (s *AuthServer) UnlinkGameCenter(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "gamecenter", "")
}
func (s *AuthServer) UnlinkFacebookInstantGame(ctx context.Context, _ *apipb.UnlinkRequest) (*emptypb.Empty, error) {
	return s.unlink(ctx, "facebookinstantgame", "")
}
