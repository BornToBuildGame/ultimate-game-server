package runtime

import (
	"context"
	"database/sql"
)

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
