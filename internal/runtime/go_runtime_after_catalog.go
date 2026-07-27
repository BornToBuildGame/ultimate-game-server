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

func registerAfterOutTyped[In any, Out any](i *goInitializer, hookID string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Out, in *In) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out, in interface{}) error {
		var req In
		if err := convertHookParam(in, &req); err != nil {
			return err
		}
		var resp *Out
		if out != nil {
			if typed, ok := out.(*Out); ok {
				resp = typed
			} else {
				resp = new(Out)
				if err := convertHookParam(out, resp); err != nil {
					return err
				}
			}
		}
		return fn(ctx, logger, db, nk, resp, &req)
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

func (i *goInitializer) RegisterAfterReadStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectList, in *ReadStorageObjectsRequest) error) error {
	return registerAfterOutTyped(i, "ReadStorageObjects", fn)
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
func (i *goInitializer) RegisterAfterWriteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecord, in *WriteLeaderboardRecordRequest) error) error {
	return registerAfterOutTyped(i, "WriteLeaderboardRecord", fn)
}
func (i *goInitializer) RegisterAfterJoinTournament(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinTournamentRequest) error) error {
	return registerAfterTyped(i, "JoinTournament", fn)
}
func (i *goInitializer) RegisterAfterCreateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupView, in *CreateGroupRequest) error) error {
	return registerAfterOutTyped(i, "CreateGroup", fn)
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
func (i *goInitializer) RegisterAfterValidatePurchaseApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseAppleRequest) error) error {
	return registerAfterOutTyped(i, "ValidatePurchaseApple", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseGoogleRequest) error) error {
	return registerAfterOutTyped(i, "ValidatePurchaseGoogle", fn)
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

func (i *goInitializer) RegisterAfterAuthenticateGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateGameCenterRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateGameCenter", fn)
}
func (i *goInitializer) RegisterAfterAuthenticateFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateFacebookInstantGameRequest) error) error {
	return registerAfterSessionTyped(i, "AuthenticateFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterAfterLinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookRequest) error) error {
	return registerAfterTyped(i, "LinkFacebook", fn)
}
func (i *goInitializer) RegisterAfterLinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkSteamRequest) error) error {
	return registerAfterTyped(i, "LinkSteam", fn)
}
func (i *goInitializer) RegisterAfterLinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkDeviceRequest) error) error {
	return registerAfterTyped(i, "LinkDevice", fn)
}
func (i *goInitializer) RegisterAfterLinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkCustomRequest) error) error {
	return registerAfterTyped(i, "LinkCustom", fn)
}
func (i *goInitializer) RegisterAfterLinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkEmailRequest) error) error {
	return registerAfterTyped(i, "LinkEmail", fn)
}
func (i *goInitializer) RegisterAfterLinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGameCenterRequest) error) error {
	return registerAfterTyped(i, "LinkGameCenter", fn)
}
func (i *goInitializer) RegisterAfterLinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookInstantGameRequest) error) error {
	return registerAfterTyped(i, "LinkFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterAfterUnlinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkAppleRequest) error) error {
	return registerAfterTyped(i, "UnlinkApple", fn)
}
func (i *goInitializer) RegisterAfterUnlinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGoogleRequest) error) error {
	return registerAfterTyped(i, "UnlinkGoogle", fn)
}
func (i *goInitializer) RegisterAfterUnlinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookRequest) error) error {
	return registerAfterTyped(i, "UnlinkFacebook", fn)
}
func (i *goInitializer) RegisterAfterUnlinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkSteamRequest) error) error {
	return registerAfterTyped(i, "UnlinkSteam", fn)
}
func (i *goInitializer) RegisterAfterUnlinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkDeviceRequest) error) error {
	return registerAfterTyped(i, "UnlinkDevice", fn)
}
func (i *goInitializer) RegisterAfterUnlinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkCustomRequest) error) error {
	return registerAfterTyped(i, "UnlinkCustom", fn)
}
func (i *goInitializer) RegisterAfterUnlinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkEmailRequest) error) error {
	return registerAfterTyped(i, "UnlinkEmail", fn)
}
func (i *goInitializer) RegisterAfterUnlinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGameCenterRequest) error) error {
	return registerAfterTyped(i, "UnlinkGameCenter", fn)
}
func (i *goInitializer) RegisterAfterUnlinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookInstantGameRequest) error) error {
	return registerAfterTyped(i, "UnlinkFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterAfterListStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectList, in *ListStorageObjectsRequest) error) error {
	return registerAfterOutTyped(i, "ListStorageObjects", fn)
}
func (i *goInitializer) RegisterAfterListFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *FriendList, in *ListFriendsRequest) error) error {
	return registerAfterOutTyped(i, "ListFriends", fn)
}
func (i *goInitializer) RegisterAfterListFriendsOfFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *FriendsOfFriendsList, in *ListFriendsOfFriendsRequest) error) error {
	return registerAfterOutTyped(i, "ListFriendsOfFriends", fn)
}
func (i *goInitializer) RegisterAfterListLeaderboardRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListLeaderboardRecordsRequest) error) error {
	return registerAfterOutTyped(i, "ListLeaderboardRecords", fn)
}
func (i *goInitializer) RegisterAfterListLeaderboardRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListLeaderboardRecordsAroundOwnerRequest) error) error {
	return registerAfterOutTyped(i, "ListLeaderboardRecordsAroundOwner", fn)
}
func (i *goInitializer) RegisterAfterDeleteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteLeaderboardRecordRequest) error) error {
	return registerAfterTyped(i, "DeleteLeaderboardRecord", fn)
}
func (i *goInitializer) RegisterAfterWriteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecord, in *WriteTournamentRecordRequest) error) error {
	return registerAfterOutTyped(i, "WriteTournamentRecord", fn)
}
func (i *goInitializer) RegisterAfterListTournaments(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *TournamentList, in *ListTournamentsRequest) error) error {
	return registerAfterOutTyped(i, "ListTournaments", fn)
}
func (i *goInitializer) RegisterAfterListTournamentRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListTournamentRecordsRequest) error) error {
	return registerAfterOutTyped(i, "ListTournamentRecords", fn)
}
func (i *goInitializer) RegisterAfterListTournamentRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListTournamentRecordsAroundOwnerRequest) error) error {
	return registerAfterOutTyped(i, "ListTournamentRecordsAroundOwner", fn)
}
func (i *goInitializer) RegisterAfterDeleteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteTournamentRecordRequest) error) error {
	return registerAfterTyped(i, "DeleteTournamentRecord", fn)
}
func (i *goInitializer) RegisterAfterPromoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *PromoteGroupUsersRequest) error) error {
	return registerAfterTyped(i, "PromoteGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterDemoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DemoteGroupUsersRequest) error) error {
	return registerAfterTyped(i, "DemoteGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterAddGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddGroupUsersRequest) error) error {
	return registerAfterTyped(i, "AddGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterUpdateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateGroupRequest) error) error {
	return registerAfterTyped(i, "UpdateGroup", fn)
}
func (i *goInitializer) RegisterAfterDeleteGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteGroupRequest) error) error {
	return registerAfterTyped(i, "DeleteGroup", fn)
}
func (i *goInitializer) RegisterAfterListGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupUserList, in *ListGroupUsersRequest) error) error {
	return registerAfterOutTyped(i, "ListGroupUsers", fn)
}
func (i *goInitializer) RegisterAfterListUserGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *UserGroupList, in *ListUserGroupsRequest) error) error {
	return registerAfterOutTyped(i, "ListUserGroups", fn)
}
func (i *goInitializer) RegisterAfterListGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupList, in *ListGroupsRequest) error) error {
	return registerAfterOutTyped(i, "ListGroups", fn)
}
func (i *goInitializer) RegisterAfterListNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *NotificationList, in *ListNotificationsRequest) error) error {
	return registerAfterOutTyped(i, "ListNotifications", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseHuawei(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseHuaweiRequest) error) error {
	return registerAfterOutTyped(i, "ValidatePurchaseHuawei", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseFacebookInstant(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseFacebookInstantRequest) error) error {
	return registerAfterOutTyped(i, "ValidatePurchaseFacebookInstant", fn)
}
func (i *goInitializer) RegisterAfterValidatePurchaseSamsung(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseSamsungRequest) error) error {
	return registerAfterOutTyped(i, "ValidatePurchaseSamsung", fn)
}
func (i *goInitializer) RegisterAfterValidateSubscriptionApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidateSubscriptionResponse, in *ValidateSubscriptionAppleRequest) error) error {
	return registerAfterOutTyped(i, "ValidateSubscriptionApple", fn)
}
func (i *goInitializer) RegisterAfterValidateSubscriptionGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidateSubscriptionResponse, in *ValidateSubscriptionGoogleRequest) error) error {
	return registerAfterOutTyped(i, "ValidateSubscriptionGoogle", fn)
}
func (i *goInitializer) RegisterAfterGetSubscription(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatedSubscriptionView, in *GetSubscriptionRequest) error) error {
	return registerAfterOutTyped(i, "GetSubscription", fn)
}
func (i *goInitializer) RegisterAfterListSubscriptions(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *SubscriptionList, in *ListSubscriptionsRequest) error) error {
	return registerAfterOutTyped(i, "ListSubscriptions", fn)
}
func (i *goInitializer) RegisterAfterGetUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *UsersList, in *GetUsersRequest) error) error {
	return registerAfterOutTyped(i, "GetUsers", fn)
}
func (i *goInitializer) RegisterAfterGetAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Account, in *GetAccountRequest) error) error {
	return registerAfterOutTyped(i, "GetAccount", fn)
}
func (i *goInitializer) RegisterAfterDeleteAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteAccountRequest) error) error {
	return registerAfterTyped(i, "DeleteAccount", fn)
}
func (i *goInitializer) RegisterAfterSessionLogout(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionLogoutRequest) error) error {
	return registerAfterTyped(i, "SessionLogout", fn)
}
func (i *goInitializer) RegisterAfterImportFacebookFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportFacebookFriendsRequest) error) error {
	return registerAfterTyped(i, "ImportFacebookFriends", fn)
}
func (i *goInitializer) RegisterAfterImportSteamFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportSteamFriendsRequest) error) error {
	return registerAfterTyped(i, "ImportSteamFriends", fn)
}
func (i *goInitializer) RegisterAfterListMatches(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *MatchList, in *ListMatchesRequest) error) error {
	return registerAfterOutTyped(i, "ListMatches", fn)
}
func (i *goInitializer) RegisterAfterListChannelMessages(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ChannelMessageList, in *ListChannelMessagesRequest) error) error {
	return registerAfterOutTyped(i, "ListChannelMessages", fn)
}
func (i *goInitializer) RegisterAfterGetMatchmakerStats(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *MatchmakerStatsView, in *GetMatchmakerStatsRequest) error) error {
	return registerAfterOutTyped(i, "GetMatchmakerStats", fn)
}
func (i *goInitializer) RegisterAfterListParties(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *PartyListView, in *ListPartiesRequest) error) error {
	return registerAfterOutTyped(i, "ListParties", fn)
}
