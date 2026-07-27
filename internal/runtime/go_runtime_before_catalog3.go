package runtime

import (
	"context"
	"database/sql"
)

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
