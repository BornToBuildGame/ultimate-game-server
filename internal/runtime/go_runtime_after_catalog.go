package runtime

import (
	"context"
	"database/sql"
)

func registerAfterTyped[T any](i *goInitializer, hookID string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *T) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out, in interface{}) error {
		var req T
		if err := convertHookParam(in, &req); err != nil {
			return err
		}
		return fn(ctx, logger, db, nk, &req)
	}
	i.registry.RegisterAfter(hookID, wrapped)
	return nil
}

func registerAfterSessionTyped[T any](i *goInitializer, hookID string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *T) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out, in interface{}) error {
		var req T
		if err := convertHookParam(in, &req); err != nil {
			return err
		}
		var sess *Session
		if out != nil {
			if s, ok := out.(*Session); ok {
				sess = s
			} else {
				sess = &Session{}
				_ = convertHookParam(out, sess)
			}
		}
		return fn(ctx, logger, db, nk, sess, &req)
	}
	i.registry.RegisterAfter(hookID, wrapped)
	return nil
}

func (i *goInitializer) RegisterAfterAuthenticateDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateDeviceRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateDevice", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateCustomRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateCustom", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateAppleRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateApple", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateGoogleRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateGoogle", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateFacebookRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateFacebook", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateSteamRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateSteam", fn)
}
func (i *goInitializer) RegisterAfterSessionRefresh(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *SessionRefreshRequest) error) error {
	return registerAfterSessionTyped(i, "SessionRefresh", fn)
}

func (i *goInitializer) RegisterAfterReadStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ReadStorageObjectsRequest) error) error {
	return registerAfterTyped(i, "ReadStorageObjects", fn)
}
func (i *goInitializer) RegisterAfterDeleteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteStorageObjectsRequest) error) error {
	return registerAfterTyped(i, "DeleteStorageObjects", fn)
}
func (i *goInitializer) RegisterAfterDeleteFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteFriendsRequest) error) error {
	return registerAfterTyped(i, "DeleteFriends", fn)
}
func (i *goInitializer) RegisterAfterBlockFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BlockFriendsRequest) error) error {
	return registerAfterTyped(i, "BlockFriends", fn)
}
func (i *goInitializer) RegisterAfterWriteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteLeaderboardRecordRequest) error) error {
	return registerAfterTyped(i, "WriteLeaderboardRecord", fn)
}
func (i *goInitializer) RegisterAfterJoinTournament(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinTournamentRequest) error) error {
	return registerAfterTyped(i, "JoinTournament", fn)
}
func (i *goInitializer) RegisterAfterCreateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreateGroupRequest) error) error {
	return registerAfterTyped(i, "CreateGroup", fn)
}
func (i *goInitializer) RegisterAfterLeaveGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeaveGroupRequest) error) error {
	return registerAfterTyped(i, "LeaveGroup", fn)
}
func (i *goInitializer) RegisterAfterBanGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BanGroupUsersRequest) error) error {
	return registerAfterTyped(i, "BanGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterKickGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *KickGroupUsersRequest) error) error {
	return registerAfterTyped(i, "KickGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseAppleRequest) error) error {
	return registerAfterTyped(i, "ValidatePurchaseApple", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseGoogleRequest) error) error {
	return registerAfterTyped(i, "ValidatePurchaseGoogle", fn)
}
func (i *goInitializer) RegisterAfterLinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkAppleRequest) error) error {
	return registerAfterTyped(i, "LinkApple", fn)
}
func (i *goInitializer) RegisterAfterLinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGoogleRequest) error) error {
	return registerAfterTyped(i, "LinkGoogle", fn)
}
func (i *goInitializer) RegisterAfterUpdateAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateAccountRequest) error) error {
	return registerAfterTyped(i, "UpdateAccount", fn)
}
func (i *goInitializer) RegisterAfterDeleteNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteNotificationsRequest) error) error {
	return registerAfterTyped(i, "DeleteNotifications", fn)
}
