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
