package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Priority typed before-hook request shapes (HTTP/gRPC method-name IDs).
type AuthenticateDeviceRequest struct {
	ID       string            `json:"id"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type AuthenticateCustomRequest struct {
	ID       string            `json:"id"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type AuthenticateAppleRequest struct {
	Token    string            `json:"token"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type AuthenticateGoogleRequest struct {
	Token    string            `json:"token"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type AuthenticateFacebookRequest struct {
	Token    string            `json:"token"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type AuthenticateSteamRequest struct {
	Token    string            `json:"token"`
	Username string            `json:"username"`
	Create   *bool             `json:"create"`
	Vars     map[string]string `json:"vars"`
}

type SessionRefreshRequest struct {
	Token string            `json:"token"`
	Vars  map[string]string `json:"vars"`
}

type SessionLogoutRequest struct {
	Token        string `json:"token"`
	RefreshToken string `json:"refresh_token"`
}

type ReadStorageObjectsRequest struct {
	ObjectIDs []StorageRead `json:"object_ids"`
}

type DeleteStorageObjectsRequest struct {
	ObjectIDs []StorageDelete `json:"object_ids"`
}

type ListStorageObjectsRequest struct {
	UserID     string `json:"user_id"`
	Collection string `json:"collection"`
	Limit      int32  `json:"limit"`
	Cursor     string `json:"cursor"`
}

type DeleteFriendsRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
}

type ListFriendsRequest struct {
	Limit  int32  `json:"limit"`
	State  *int32 `json:"state"`
	Cursor string `json:"cursor"`
}

type BlockFriendsRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
}

type WriteLeaderboardRecordRequest struct {
	LeaderboardID string `json:"leaderboard_id"`
	Score         int64  `json:"score"`
	Subscore      int64  `json:"subscore"`
	Metadata      string `json:"metadata"`
}

type ListLeaderboardRecordsRequest struct {
	LeaderboardID string   `json:"leaderboard_id"`
	OwnerIDs      []string `json:"owner_ids"`
	Limit         int32    `json:"limit"`
	Cursor        string   `json:"cursor"`
	Expiry        int64    `json:"expiry"`
}

type JoinTournamentRequest struct {
	TournamentID string `json:"tournament_id"`
}

type WriteTournamentRecordRequest struct {
	TournamentID string `json:"tournament_id"`
	Score        int64  `json:"score"`
	Subscore     int64  `json:"subscore"`
	Metadata     string `json:"metadata"`
}

type CreateGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	LangTag     string `json:"lang_tag"`
	Metadata    string `json:"metadata"`
	AvatarURL   string `json:"avatar_url"`
	Open        bool   `json:"open"`
	MaxCount    int32  `json:"max_count"`
}

type LeaveGroupRequest struct {
	GroupID string `json:"group_id"`
}

type CreateMatchRequest struct {
	Name string `json:"name"`
}

type ListMatchesRequest struct {
	Limit         int32  `json:"limit"`
	Authoritative *bool  `json:"authoritative"`
	Label         string `json:"label"`
	MinSize       *int32 `json:"min_size"`
	MaxSize       *int32 `json:"max_size"`
}

type ListChannelMessagesRequest struct {
	ChannelID string `json:"channel_id"`
	Limit     int32  `json:"limit"`
	Forward   bool   `json:"forward"`
	Cursor    string `json:"cursor"`
}

type EventRequest struct {
	Name     string                 `json:"name"`
	Properties map[string]string    `json:"properties"`
	Timestamp int64                 `json:"timestamp"`
	External bool                   `json:"external"`
}

type GetAccountRequest struct{}

type UpdateAccountRequest struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
	LangTag     string `json:"lang_tag"`
	Location    string `json:"location"`
	Timezone    string `json:"timezone"`
	Metadata    string `json:"metadata"`
}

type DeleteAccountRequest struct{}

type GetWalletRequest struct{}

type ListWalletLedgerRequest struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}

func registerBeforeTyped[T any](i *goInitializer, hookID string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *T) (*T, error)) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		var req T
		if err := convertHookParam(in, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		return convertHookParamBack(res, in)
	}
	i.registry.RegisterBefore(hookID, wrapped)
	return nil
}

// registerBeforeTypedDual registers a typed before-hook under a REST/gRPC id and an RT envelope id.
// The RT adapter extracts envelope[rtKey], runs fn, and writes the result back into the envelope.
func registerBeforeTypedDual[T any](i *goInitializer, restID, rtKey string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *T) (*T, error)) error {
	if err := registerBeforeTyped(i, restID, fn); err != nil {
		return err
	}
	rtWrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		env, ok := in.(map[string]interface{})
		if !ok {
			if p, ok := in.(*map[string]interface{}); ok && p != nil {
				env = *p
			} else {
				return nil, fmt.Errorf("rt before %s: expected envelope map", rtKey)
			}
		}
		payload := env[rtKey]
		if payload == nil {
			payload = map[string]interface{}{}
		}
		var req T
		if err := convertHookParam(payload, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		if res != nil {
			b, err := json.Marshal(res)
			if err != nil {
				return nil, err
			}
			var outPayload interface{}
			if err := json.Unmarshal(b, &outPayload); err != nil {
				return nil, err
			}
			env[rtKey] = outPayload
		}
		return env, nil
	}
	i.registry.RegisterBefore(rtKey, rtWrapped)
	return nil
}

func (i *goInitializer) RegisterBeforeAuthenticateDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateDeviceRequest) (*AuthenticateDeviceRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateDevice", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateCustomRequest) (*AuthenticateCustomRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateCustom", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateAppleRequest) (*AuthenticateAppleRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateApple", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateGoogleRequest) (*AuthenticateGoogleRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateGoogle", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateFacebookRequest) (*AuthenticateFacebookRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateFacebook", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateSteamRequest) (*AuthenticateSteamRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateSteam", fn)
}
func (i *goInitializer) RegisterBeforeSessionRefresh(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionRefreshRequest) (*SessionRefreshRequest, error)) error {
	return registerBeforeTyped(i, "SessionRefresh", fn)
}
func (i *goInitializer) RegisterBeforeSessionLogout(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionLogoutRequest) (*SessionLogoutRequest, error)) error {
	return registerBeforeTyped(i, "SessionLogout", fn)
}
func (i *goInitializer) RegisterBeforeReadStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ReadStorageObjectsRequest) (*ReadStorageObjectsRequest, error)) error {
	return registerBeforeTyped(i, "ReadStorageObjects", fn)
}
func (i *goInitializer) RegisterBeforeDeleteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteStorageObjectsRequest) (*DeleteStorageObjectsRequest, error)) error {
	return registerBeforeTyped(i, "DeleteStorageObjects", fn)
}
func (i *goInitializer) RegisterBeforeListStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListStorageObjectsRequest) (*ListStorageObjectsRequest, error)) error {
	return registerBeforeTyped(i, "ListStorageObjects", fn)
}
func (i *goInitializer) RegisterBeforeDeleteFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteFriendsRequest) (*DeleteFriendsRequest, error)) error {
	return registerBeforeTyped(i, "DeleteFriends", fn)
}
func (i *goInitializer) RegisterBeforeListFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListFriendsRequest) (*ListFriendsRequest, error)) error {
	return registerBeforeTyped(i, "ListFriends", fn)
}
func (i *goInitializer) RegisterBeforeBlockFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BlockFriendsRequest) (*BlockFriendsRequest, error)) error {
	return registerBeforeTyped(i, "BlockFriends", fn)
}
func (i *goInitializer) RegisterBeforeWriteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteLeaderboardRecordRequest) (*WriteLeaderboardRecordRequest, error)) error {
	return registerBeforeTyped(i, "WriteLeaderboardRecord", fn)
}
func (i *goInitializer) RegisterBeforeListLeaderboardRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListLeaderboardRecordsRequest) (*ListLeaderboardRecordsRequest, error)) error {
	return registerBeforeTyped(i, "ListLeaderboardRecords", fn)
}
func (i *goInitializer) RegisterBeforeJoinTournament(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinTournamentRequest) (*JoinTournamentRequest, error)) error {
	return registerBeforeTyped(i, "JoinTournament", fn)
}
func (i *goInitializer) RegisterBeforeWriteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteTournamentRecordRequest) (*WriteTournamentRecordRequest, error)) error {
	return registerBeforeTyped(i, "WriteTournamentRecord", fn)
}
func (i *goInitializer) RegisterBeforeCreateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreateGroupRequest) (*CreateGroupRequest, error)) error {
	return registerBeforeTyped(i, "CreateGroup", fn)
}
func (i *goInitializer) RegisterBeforeLeaveGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeaveGroupRequest) (*LeaveGroupRequest, error)) error {
	return registerBeforeTyped(i, "LeaveGroup", fn)
}
func (i *goInitializer) RegisterBeforeCreateMatch(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreateMatchRequest) (*CreateMatchRequest, error)) error {
	return registerBeforeTypedDual(i, "CreateMatch", "match_create", fn)
}
func (i *goInitializer) RegisterBeforeListMatches(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListMatchesRequest) (*ListMatchesRequest, error)) error {
	return registerBeforeTyped(i, "ListMatches", fn)
}
func (i *goInitializer) RegisterBeforeListChannelMessages(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListChannelMessagesRequest) (*ListChannelMessagesRequest, error)) error {
	return registerBeforeTyped(i, "ListChannelMessages", fn)
}
func (i *goInitializer) RegisterBeforeEvent(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *EventRequest) (*EventRequest, error)) error {
	return registerBeforeTyped(i, "Event", fn)
}
func (i *goInitializer) RegisterBeforeGetAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetAccountRequest) (*GetAccountRequest, error)) error {
	return registerBeforeTyped(i, "GetAccount", fn)
}
func (i *goInitializer) RegisterBeforeUpdateAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateAccountRequest) (*UpdateAccountRequest, error)) error {
	return registerBeforeTyped(i, "UpdateAccount", fn)
}
func (i *goInitializer) RegisterBeforeDeleteAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteAccountRequest) (*DeleteAccountRequest, error)) error {
	return registerBeforeTyped(i, "DeleteAccount", fn)
}
func (i *goInitializer) RegisterBeforeGetWallet(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetWalletRequest) (*GetWalletRequest, error)) error {
	return registerBeforeTyped(i, "GetWallet", fn)
}
func (i *goInitializer) RegisterBeforeListWalletLedger(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListWalletLedgerRequest) (*ListWalletLedgerRequest, error)) error {
	return registerBeforeTyped(i, "ListWalletLedger", fn)
}

// --- Additional before-hook request shapes (link/IAP/group/notification/party) ---

type LinkAppleRequest struct {
	Token string            `json:"token"`
	Vars  map[string]string `json:"vars"`
}
type LinkGoogleRequest struct {
	Token string            `json:"token"`
	Vars  map[string]string `json:"vars"`
}
type LinkFacebookRequest struct {
	Token string            `json:"token"`
	Vars  map[string]string `json:"vars"`
}
type LinkSteamRequest struct {
	Token string            `json:"token"`
	Vars  map[string]string `json:"vars"`
}
type LinkDeviceRequest struct {
	ID   string            `json:"id"`
	Vars map[string]string `json:"vars"`
}
type LinkCustomRequest struct {
	ID   string            `json:"id"`
	Vars map[string]string `json:"vars"`
}
type LinkEmailRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}
type UnlinkAppleRequest struct{ Token string `json:"token"` }
type UnlinkGoogleRequest struct{ Token string `json:"token"` }
type UnlinkFacebookRequest struct{ Token string `json:"token"` }
type UnlinkSteamRequest struct{ Token string `json:"token"` }
type UnlinkDeviceRequest struct{ ID string `json:"id"` }
type UnlinkCustomRequest struct{ ID string `json:"id"` }
type UnlinkEmailRequest struct{ Email string `json:"email"` }

type ValidatePurchaseAppleRequest struct {
	Receipt string `json:"receipt"`
	Persist *bool  `json:"persist"`
}
type ValidatePurchaseGoogleRequest struct {
	ProductID     string `json:"product_id"`
	PurchaseToken string `json:"purchase_token"`
	Persist       *bool  `json:"persist"`
}
type ValidatePurchaseHuaweiRequest struct {
	Purchase  string `json:"purchase"`
	Signature string `json:"signature"`
	Persist   *bool  `json:"persist"`
}
type ValidateSubscriptionAppleRequest struct {
	Receipt string `json:"receipt"`
	Persist *bool  `json:"persist"`
}
type ValidateSubscriptionGoogleRequest struct {
	ProductID     string `json:"product_id"`
	PurchaseToken string `json:"purchase_token"`
	Persist       *bool  `json:"persist"`
}

type BanGroupUsersRequest struct {
	GroupID string   `json:"group_id"`
	UserIDs []string `json:"user_ids"`
}
type KickGroupUsersRequest struct {
	GroupID string   `json:"group_id"`
	UserIDs []string `json:"user_ids"`
}
type PromoteGroupUsersRequest struct {
	GroupID string   `json:"group_id"`
	UserIDs []string `json:"user_ids"`
}
type DemoteGroupUsersRequest struct {
	GroupID string   `json:"group_id"`
	UserIDs []string `json:"user_ids"`
}
type AddGroupUsersRequest struct {
	GroupID string   `json:"group_id"`
	UserIDs []string `json:"user_ids"`
}
type UpdateGroupRequest struct {
	GroupID     string `json:"group_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	AvatarURL   string `json:"avatar_url"`
	LangTag     string `json:"lang_tag"`
	Metadata    string `json:"metadata"`
	Open        *bool  `json:"open"`
}
type DeleteGroupRequest struct {
	GroupID string `json:"group_id"`
}
type ListGroupUsersRequest struct {
	GroupID string `json:"group_id"`
	Limit   int32  `json:"limit"`
	State   *int32 `json:"state"`
	Cursor  string `json:"cursor"`
}
type ListUserGroupsRequest struct {
	UserID string `json:"user_id"`
	Limit  int32  `json:"limit"`
	State  *int32 `json:"state"`
	Cursor string `json:"cursor"`
}

type ListNotificationsRequest struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}
type DeleteNotificationsRequest struct {
	IDs []string `json:"ids"`
}
type ListFriendsOfFriendsRequest struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}

type CreatePartyRequest struct {
	Open    bool  `json:"open"`
	MaxSize int32 `json:"max_size"`
}
type JoinPartyRequest struct {
	PartyID string `json:"party_id"`
}
type LeavePartyRequest struct {
	PartyID string `json:"party_id"`
}

type ListTournamentsRequest struct {
	CategoryStart int32  `json:"category_start"`
	CategoryEnd   int32  `json:"category_end"`
	StartTime     int32  `json:"start_time"`
	EndTime       int32  `json:"end_time"`
	Limit         int32  `json:"limit"`
	Cursor        string `json:"cursor"`
}

func (i *goInitializer) RegisterBeforeLinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkAppleRequest) (*LinkAppleRequest, error)) error {
	return registerBeforeTyped(i, "LinkApple", fn)
}
func (i *goInitializer) RegisterBeforeLinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGoogleRequest) (*LinkGoogleRequest, error)) error {
	return registerBeforeTyped(i, "LinkGoogle", fn)
}
func (i *goInitializer) RegisterBeforeLinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookRequest) (*LinkFacebookRequest, error)) error {
	return registerBeforeTyped(i, "LinkFacebook", fn)
}
func (i *goInitializer) RegisterBeforeLinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkSteamRequest) (*LinkSteamRequest, error)) error {
	return registerBeforeTyped(i, "LinkSteam", fn)
}
func (i *goInitializer) RegisterBeforeLinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkDeviceRequest) (*LinkDeviceRequest, error)) error {
	return registerBeforeTyped(i, "LinkDevice", fn)
}
func (i *goInitializer) RegisterBeforeLinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkCustomRequest) (*LinkCustomRequest, error)) error {
	return registerBeforeTyped(i, "LinkCustom", fn)
}
func (i *goInitializer) RegisterBeforeLinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkEmailRequest) (*LinkEmailRequest, error)) error {
	return registerBeforeTyped(i, "LinkEmail", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkAppleRequest) (*UnlinkAppleRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkApple", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGoogleRequest) (*UnlinkGoogleRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkGoogle", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookRequest) (*UnlinkFacebookRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkFacebook", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkSteamRequest) (*UnlinkSteamRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkSteam", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkDeviceRequest) (*UnlinkDeviceRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkDevice", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkCustomRequest) (*UnlinkCustomRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkCustom", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkEmailRequest) (*UnlinkEmailRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkEmail", fn)
}

func (i *goInitializer) RegisterBeforeValidatePurchaseApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseAppleRequest) (*ValidatePurchaseAppleRequest, error)) error {
	return registerBeforeTyped(i, "ValidatePurchaseApple", fn)
}
func (i *goInitializer) RegisterBeforeValidatePurchaseGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseGoogleRequest) (*ValidatePurchaseGoogleRequest, error)) error {
	return registerBeforeTyped(i, "ValidatePurchaseGoogle", fn)
}
func (i *goInitializer) RegisterBeforeValidatePurchaseHuawei(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseHuaweiRequest) (*ValidatePurchaseHuaweiRequest, error)) error {
	return registerBeforeTyped(i, "ValidatePurchaseHuawei", fn)
}
func (i *goInitializer) RegisterBeforeValidateSubscriptionApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidateSubscriptionAppleRequest) (*ValidateSubscriptionAppleRequest, error)) error {
	return registerBeforeTyped(i, "ValidateSubscriptionApple", fn)
}
func (i *goInitializer) RegisterBeforeValidateSubscriptionGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidateSubscriptionGoogleRequest) (*ValidateSubscriptionGoogleRequest, error)) error {
	return registerBeforeTyped(i, "ValidateSubscriptionGoogle", fn)
}

func (i *goInitializer) RegisterBeforeBanGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BanGroupUsersRequest) (*BanGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "BanGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforeKickGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *KickGroupUsersRequest) (*KickGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "KickGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforePromoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *PromoteGroupUsersRequest) (*PromoteGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "PromoteGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforeDemoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DemoteGroupUsersRequest) (*DemoteGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "DemoteGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforeAddGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddGroupUsersRequest) (*AddGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "AddGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforeUpdateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateGroupRequest) (*UpdateGroupRequest, error)) error {
	return registerBeforeTyped(i, "UpdateGroup", fn)
}
func (i *goInitializer) RegisterBeforeDeleteGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteGroupRequest) (*DeleteGroupRequest, error)) error {
	return registerBeforeTyped(i, "DeleteGroup", fn)
}
func (i *goInitializer) RegisterBeforeListGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListGroupUsersRequest) (*ListGroupUsersRequest, error)) error {
	return registerBeforeTyped(i, "ListGroupUsers", fn)
}
func (i *goInitializer) RegisterBeforeListUserGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListUserGroupsRequest) (*ListUserGroupsRequest, error)) error {
	return registerBeforeTyped(i, "ListUserGroups", fn)
}

func (i *goInitializer) RegisterBeforeListNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListNotificationsRequest) (*ListNotificationsRequest, error)) error {
	return registerBeforeTyped(i, "ListNotifications", fn)
}
func (i *goInitializer) RegisterBeforeDeleteNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteNotificationsRequest) (*DeleteNotificationsRequest, error)) error {
	return registerBeforeTyped(i, "DeleteNotifications", fn)
}
func (i *goInitializer) RegisterBeforeListFriendsOfFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListFriendsOfFriendsRequest) (*ListFriendsOfFriendsRequest, error)) error {
	return registerBeforeTyped(i, "ListFriendsOfFriends", fn)
}

func (i *goInitializer) RegisterBeforeCreateParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreatePartyRequest) (*CreatePartyRequest, error)) error {
	return registerBeforeTypedDual(i, "CreateParty", "party_create", fn)
}
func (i *goInitializer) RegisterBeforeJoinParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinPartyRequest) (*JoinPartyRequest, error)) error {
	return registerBeforeTypedDual(i, "JoinParty", "party_join", fn)
}
func (i *goInitializer) RegisterBeforeLeaveParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeavePartyRequest) (*LeavePartyRequest, error)) error {
	return registerBeforeTypedDual(i, "LeaveParty", "party_leave", fn)
}
func (i *goInitializer) RegisterBeforeListTournaments(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentsRequest) (*ListTournamentsRequest, error)) error {
	return registerBeforeTyped(i, "ListTournaments", fn)
}

// Remaining Before request shapes to reach nakama-common parity (~83).

type AuthenticateGameCenterRequest struct {
	PlayerID      string            `json:"player_id"`
	BundleID      string            `json:"bundle_id"`
	Timestamp     int64             `json:"timestamp_seconds"`
	Salt          string            `json:"salt"`
	Signature     string            `json:"signature"`
	PublicKeyURL  string            `json:"public_key_url"`
	Username      string            `json:"username"`
	Create        *bool             `json:"create"`
	Vars          map[string]string `json:"vars"`
}
type AuthenticateFacebookInstantGameRequest struct {
	SignedPlayerInfo string            `json:"signed_player_info"`
	Username         string            `json:"username"`
	Create           *bool             `json:"create"`
	Vars             map[string]string `json:"vars"`
}
type LinkGameCenterRequest struct {
	PlayerID     string `json:"player_id"`
	BundleID     string `json:"bundle_id"`
	Timestamp    int64  `json:"timestamp_seconds"`
	Salt         string `json:"salt"`
	Signature    string `json:"signature"`
	PublicKeyURL string `json:"public_key_url"`
}
type LinkFacebookInstantGameRequest struct {
	SignedPlayerInfo string `json:"signed_player_info"`
}
type UnlinkGameCenterRequest struct {
	PlayerID string `json:"player_id"`
}
type UnlinkFacebookInstantGameRequest struct {
	SignedPlayerInfo string `json:"signed_player_info"`
}

type ValidatePurchaseFacebookInstantRequest struct {
	SignedRequest string `json:"signed_request"`
	Persist       *bool  `json:"persist"`
}
type ValidatePurchaseSamsungRequest struct {
	PurchaseID string `json:"purchase"`
	Persist    *bool  `json:"persist"`
}
type GetSubscriptionRequest struct {
	ProductID string `json:"product_id"`
}
type ListSubscriptionsRequest struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}

type GetMatchmakerStatsRequest struct{}
type ListPartiesRequest struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
	Query  string `json:"query"`
	Open   *bool  `json:"open"`
}
type GetUsersRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
	FacebookIDs []string `json:"facebook_ids"`
}
type ListGroupsRequest struct {
	Name     string `json:"name"`
	LangTag  string `json:"lang_tag"`
	Members  *int32 `json:"members"`
	Open     *bool  `json:"open"`
	Limit    int32  `json:"limit"`
	Cursor   string `json:"cursor"`
}
type DeleteLeaderboardRecordRequest struct {
	LeaderboardID string `json:"leaderboard_id"`
}
type DeleteTournamentRecordRequest struct {
	TournamentID string `json:"tournament_id"`
}
type ListLeaderboardRecordsAroundOwnerRequest struct {
	LeaderboardID string `json:"leaderboard_id"`
	OwnerID       string `json:"owner_id"`
	Limit         int32  `json:"limit"`
	Expiry        int64  `json:"expiry"`
}
type ListTournamentRecordsRequest struct {
	TournamentID string   `json:"tournament_id"`
	OwnerIDs     []string `json:"owner_ids"`
	Limit        int32    `json:"limit"`
	Cursor       string   `json:"cursor"`
	Expiry       int64    `json:"expiry"`
}
type ListTournamentRecordsAroundOwnerRequest struct {
	TournamentID string `json:"tournament_id"`
	OwnerID      string `json:"owner_id"`
	Limit        int32  `json:"limit"`
	Expiry       int64  `json:"expiry"`
}
type ImportFacebookFriendsRequest struct {
	Token  string `json:"token"`
	Reset  *bool  `json:"reset"`
}
type ImportSteamFriendsRequest struct {
	Token string `json:"token"`
	Reset *bool  `json:"reset"`
}

func (i *goInitializer) RegisterBeforeAuthenticateGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateGameCenterRequest) (*AuthenticateGameCenterRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateGameCenter", fn)
}
func (i *goInitializer) RegisterBeforeAuthenticateFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateFacebookInstantGameRequest) (*AuthenticateFacebookInstantGameRequest, error)) error {
	return registerBeforeTyped(i, "AuthenticateFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterBeforeLinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGameCenterRequest) (*LinkGameCenterRequest, error)) error {
	return registerBeforeTyped(i, "LinkGameCenter", fn)
}
func (i *goInitializer) RegisterBeforeLinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookInstantGameRequest) (*LinkFacebookInstantGameRequest, error)) error {
	return registerBeforeTyped(i, "LinkFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGameCenterRequest) (*UnlinkGameCenterRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkGameCenter", fn)
}
func (i *goInitializer) RegisterBeforeUnlinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookInstantGameRequest) (*UnlinkFacebookInstantGameRequest, error)) error {
	return registerBeforeTyped(i, "UnlinkFacebookInstantGame", fn)
}
func (i *goInitializer) RegisterBeforeValidatePurchaseFacebookInstant(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseFacebookInstantRequest) (*ValidatePurchaseFacebookInstantRequest, error)) error {
	return registerBeforeTyped(i, "ValidatePurchaseFacebookInstant", fn)
}
func (i *goInitializer) RegisterBeforeValidatePurchaseSamsung(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseSamsungRequest) (*ValidatePurchaseSamsungRequest, error)) error {
	return registerBeforeTyped(i, "ValidatePurchaseSamsung", fn)
}
func (i *goInitializer) RegisterBeforeGetSubscription(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetSubscriptionRequest) (*GetSubscriptionRequest, error)) error {
	return registerBeforeTyped(i, "GetSubscription", fn)
}
func (i *goInitializer) RegisterBeforeListSubscriptions(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListSubscriptionsRequest) (*ListSubscriptionsRequest, error)) error {
	return registerBeforeTyped(i, "ListSubscriptions", fn)
}
func (i *goInitializer) RegisterBeforeGetMatchmakerStats(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetMatchmakerStatsRequest) (*GetMatchmakerStatsRequest, error)) error {
	return registerBeforeTyped(i, "GetMatchmakerStats", fn)
}
func (i *goInitializer) RegisterBeforeListParties(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListPartiesRequest) (*ListPartiesRequest, error)) error {
	return registerBeforeTyped(i, "ListParties", fn)
}
func (i *goInitializer) RegisterBeforeGetUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetUsersRequest) (*GetUsersRequest, error)) error {
	return registerBeforeTyped(i, "GetUsers", fn)
}
func (i *goInitializer) RegisterBeforeListGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListGroupsRequest) (*ListGroupsRequest, error)) error {
	return registerBeforeTyped(i, "ListGroups", fn)
}
func (i *goInitializer) RegisterBeforeDeleteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteLeaderboardRecordRequest) (*DeleteLeaderboardRecordRequest, error)) error {
	return registerBeforeTyped(i, "DeleteLeaderboardRecord", fn)
}
func (i *goInitializer) RegisterBeforeDeleteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteTournamentRecordRequest) (*DeleteTournamentRecordRequest, error)) error {
	return registerBeforeTyped(i, "DeleteTournamentRecord", fn)
}
func (i *goInitializer) RegisterBeforeListLeaderboardRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListLeaderboardRecordsAroundOwnerRequest) (*ListLeaderboardRecordsAroundOwnerRequest, error)) error {
	return registerBeforeTyped(i, "ListLeaderboardRecordsAroundOwner", fn)
}
func (i *goInitializer) RegisterBeforeListTournamentRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentRecordsRequest) (*ListTournamentRecordsRequest, error)) error {
	return registerBeforeTyped(i, "ListTournamentRecords", fn)
}
func (i *goInitializer) RegisterBeforeListTournamentRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentRecordsAroundOwnerRequest) (*ListTournamentRecordsAroundOwnerRequest, error)) error {
	return registerBeforeTyped(i, "ListTournamentRecordsAroundOwner", fn)
}
func (i *goInitializer) RegisterBeforeImportFacebookFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportFacebookFriendsRequest) (*ImportFacebookFriendsRequest, error)) error {
	return registerBeforeTyped(i, "ImportFacebookFriends", fn)
}
func (i *goInitializer) RegisterBeforeImportSteamFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportSteamFriendsRequest) (*ImportSteamFriendsRequest, error)) error {
	return registerBeforeTyped(i, "ImportSteamFriends", fn)
}
