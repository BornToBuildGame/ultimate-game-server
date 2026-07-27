package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"ultimate-game-server/internal/cronexpr"
	"ultimate-game-server/internal/fleet"
	"ultimate-game-server/internal/satori"
)

// Logger provides structured logging for runtime modules.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// RuntimeModule provides access to all server-side APIs from runtime code.
// This interface is injected into all Go native runtime handler invocations.
type RuntimeModule interface {
	// Storage operations
	StorageRead(ctx context.Context, reads []*StorageRead) ([]*StorageObject, error)
	StorageWrite(ctx context.Context, writes []*StorageWrite) ([]*StorageObjectAck, error)
	StorageDelete(ctx context.Context, deletes []*StorageDelete) error
	StorageList(ctx context.Context, callerID, userID, collection string, limit int, cursor string) ([]*StorageObject, string, error)
	StorageWriteRetry(ctx context.Context, reads []*StorageRead, updateFn func([]*StorageObject) ([]*StorageWrite, error), maxRetries int) ([]*StorageObjectAck, error)

	// Wallet operations
	WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (updated, previous map[string]int64, err error)
	WalletsUpdate(ctx context.Context, updates []*WalletUpdateParams, updateLedger bool) ([]*WalletUpdateResultView, error)
	WalletLedgerList(ctx context.Context, userID string, limit int, cursor string) ([]*WalletLedgerView, string, error)
	WalletLedgerUpdate(ctx context.Context, ledgerID, userID string, metadata map[string]interface{}) error

	// IAP operations
	PurchaseValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedPurchaseView, error)
	PurchaseValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedPurchaseView, error)
	PurchaseValidateHuawei(ctx context.Context, userID, purchaseData, signature string, persist bool) (*ValidatedPurchaseView, error)
	PurchaseValidateFacebookInstant(ctx context.Context, userID, signedRequest string, persist bool) (*ValidatedPurchaseView, error)
	PurchaseValidateSamsung(ctx context.Context, userID, purchaseID string, persist bool) (*ValidatedPurchaseView, error)
	PurchasesList(ctx context.Context, userID string, limit int) ([]*ValidatedPurchaseView, error)
	SubscriptionValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedSubscriptionView, error)
	SubscriptionValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedSubscriptionView, error)
	SubscriptionsList(ctx context.Context, userID string, limit int) ([]*ValidatedSubscriptionView, error)
	SubscriptionGetProductID(ctx context.Context, userID, productID string) (*ValidatedSubscriptionView, error)

	// Account operations
	AccountGetId(ctx context.Context, userID string) (*Account, error)
	UsersGetId(ctx context.Context, userIDs []string) ([]*UserView, error)
	UsersGetUsername(ctx context.Context, usernames []string) ([]*UserView, error)
	UsersGetRandom(ctx context.Context, count int) ([]*UserView, error)
	UsersBanId(ctx context.Context, userIDs []string) error
	UsersUnbanId(ctx context.Context, userIDs []string) error

	// Leaderboard operations
	LeaderboardCreate(ctx context.Context, id string, authoritative bool, sortOrder int, operator int, resetSchedule string, metadata map[string]interface{}, enableRanks bool) error
	LeaderboardDelete(ctx context.Context, id string) error
	LeaderboardList(ctx context.Context, limit int, cursor string) ([]*Leaderboard, string, error)
	LeaderboardsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error)
	LeaderboardRanksDisable(ctx context.Context, id string) error
	LeaderboardRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error)
	LeaderboardRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error)
	LeaderboardRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error)
	LeaderboardRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error)
	LeaderboardRecordsListCursorFromRank(ctx context.Context, leaderboardID string, rank, expiry int64) (string, error)
	LeaderboardRecordDelete(ctx context.Context, id, ownerID string) error

	// Tournament operations
	TournamentCreate(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule string, metadata map[string]interface{}, title, description string, category int, startTime, endTime int64, duration, maxSize, maxNumScore int, joinRequired, enableRanks bool) error
	TournamentDelete(ctx context.Context, id string) error
	TournamentList(ctx context.Context, categoryStart, categoryEnd int, startTime, endTime int64, limit int, cursor string, active bool) ([]*TournamentView, string, error)
	TournamentsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error)
	TournamentRanksDisable(ctx context.Context, id string) error
	TournamentJoin(ctx context.Context, id, ownerID, username string) error
	TournamentRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error)
	TournamentRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error)
	TournamentRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error)
	TournamentRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error)
	TournamentRecordDelete(ctx context.Context, id, ownerID string) error
	TournamentAddAttempt(ctx context.Context, id, ownerID string, count int) error

	// Notification operations
	NotificationSend(ctx context.Context, userID, subject string, content map[string]interface{}, code int, senderID string, persistent bool) error
	NotificationsSend(ctx context.Context, notifications []*NotificationSendParams) error
	NotificationSendAll(ctx context.Context, subject string, content map[string]interface{}, code int, persistent bool) error
	NotificationsList(ctx context.Context, userID string, limit int, cursor string) ([]*NotificationView, string, error)
	NotificationsDelete(ctx context.Context, userID string, ids []string) error
	NotificationsUpdate(ctx context.Context, updates []*NotificationUpdateParams) error
	NotificationsGetId(ctx context.Context, userID string, ids []string) ([]*NotificationView, error)
	NotificationsDeleteId(ctx context.Context, userID string, ids []string) error

	// Friends operations
	FriendsList(ctx context.Context, userID string, limit int, state *int, cursor string) ([]*FriendEdge, string, error)
	FriendsAdd(ctx context.Context, userID string, ids, usernames []string, metadata map[string]any) error
	FriendsDelete(ctx context.Context, userID string, ids, usernames []string) error
	FriendsBlock(ctx context.Context, userID string, ids, usernames []string) error
	FriendsOfFriendsList(ctx context.Context, userID string, limit int, cursor string) ([]*FriendOfFriendEdge, string, error)
	UsersGetFriendStatus(ctx context.Context, userID string, friendIDs []string) (map[string]int, error)
	FriendMetadataUpdate(ctx context.Context, userID, friendID string, metadata map[string]any) error

	// Party operations
	PartyList(ctx context.Context, limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error)

	// Group operations
	GroupsGetId(ctx context.Context, groupIDs []string) ([]*GroupView, error)
	GroupCreate(ctx context.Context, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) (*GroupView, error)
	GroupUpdate(ctx context.Context, groupID, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) error
	GroupDelete(ctx context.Context, groupID, userID string) error
	GroupUsersAdd(ctx context.Context, groupID, callerID string, userIDs []string) error
	GroupUsersBan(ctx context.Context, groupID, callerID string, userIDs []string) error
	GroupUsersKick(ctx context.Context, groupID, callerID string, userIDs []string) error
	GroupUsersPromote(ctx context.Context, groupID, callerID string, userIDs []string) error
	GroupUsersDemote(ctx context.Context, groupID, callerID string, userIDs []string) error
	GroupUsersList(ctx context.Context, groupID string, limit int, cursor string) ([]*GroupUserView, string, error)
	GroupsList(ctx context.Context, name, langTag string, open *bool, members, limit int, cursor string) ([]*GroupView, string, error)
	UserGroupsList(ctx context.Context, userID string, limit int, cursor string) ([]*UserGroupView, string, error)
	GroupsGetRandom(ctx context.Context, count int) ([]*GroupView, error)

	// Channel / chat operations
	ChannelIdBuild(ctx context.Context, userID, target string, chanType int) (string, error)
	ChannelMessageSend(ctx context.Context, channelID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error)
	ChannelMessageUpdate(ctx context.Context, channelID, messageID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error)
	ChannelMessageRemove(ctx context.Context, channelID, messageID, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error)
	ChannelMessagesList(ctx context.Context, channelID string, limit int, forward bool, cursor string) ([]*ChannelMessageView, string, string, string, error)

	// Match operations
	MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error)
	MatchList(ctx context.Context, limit int, authoritative bool, label string, minSize, maxSize int) ([]*MatchInfo, error)
	MatchGet(ctx context.Context, matchID string) (*MatchInfo, error)
	MatchSignal(ctx context.Context, matchID, data string) (string, error)

	// Status presence
	StatusFollow(sessionID string, userIDs []string) error
	StatusUnfollow(sessionID string, userIDs []string) error

	// Stream Tracker (ADR-0020)
	StreamUserList(mode int16, subject, subcontext, label string, includeHidden, includeNotHidden bool) ([]StreamPresenceView, error)
	StreamUserGet(mode int16, subject, subcontext, label, userID, sessionID string) (*StreamPresenceView, error)
	StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error)
	StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error
	StreamClose(mode int16, subject, subcontext, label string) error
	StreamCount(mode int16, subject, subcontext, label string) (int, error)
	StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error
	SessionDisconnect(sessionID string) error

	// RPC
	RpcCall(ctx context.Context, id, payload string) (string, error)

	// Atomic multi-update (account + storage + wallet)
	MultiUpdate(ctx context.Context, accountUpdates []*AccountUpdateParams, storageWrites []*StorageWrite, storageDeletes []*StorageDelete, walletUpdates []*WalletUpdateParams, updateLedger bool) ([]*StorageObjectAck, []*WalletUpdateResultView, error)
	StorageIndexList(ctx context.Context, callerID, indexName, query string, limit int, order []string, cursor string) ([]*StorageObject, string, error)
	GetSatori() satori.Satori

	// Cron utilities (UTC, ADR-0025)
	CronNext(expression string, timestamp int64) (int64, error)
	CronPrev(expression string, timestamp int64) (int64, error)
}

type StorageRead struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	UserID     string `json:"user_id"`
}

type StorageWrite struct {
	Collection      string `json:"collection"`
	Key             string `json:"key"`
	UserID          string `json:"user_id"`
	Value           string `json:"value"`
	Version         string `json:"version"`
	PermissionRead  int32  `json:"permission_read"`
	PermissionWrite int32  `json:"permission_write"`
}

type StorageDelete struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	UserID     string `json:"user_id"`
	Version    string `json:"version"`
}

// MatchInfo is metadata for an active match (not full opaque state).
type MatchInfo struct {
	MatchID       string `json:"match_id"`
	Authoritative bool   `json:"authoritative"`
	Label         string `json:"label"`
	Size          int    `json:"size"`
	MaxSize       int    `json:"max_size"`
	HandlerName   string `json:"handler_name,omitempty"`
}

// AccountUpdateParams updates account fields in MultiUpdate.
type AccountUpdateParams struct {
	UserID      string  `json:"user_id"`
	Username    *string `json:"username,omitempty"`
	DisplayName *string `json:"display_name,omitempty"`
	AvatarURL   *string `json:"avatar_url,omitempty"`
	LangTag     *string `json:"lang_tag,omitempty"`
	Location    *string `json:"location,omitempty"`
	Timezone    *string `json:"timezone,omitempty"`
	Metadata    *string `json:"metadata,omitempty"`
}

// WalletUpdateParams is a batch wallet mutation for runtime WalletsUpdate.
type WalletUpdateParams struct {
	UserID    string                 `json:"user_id"`
	Changeset map[string]int64       `json:"changeset"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// WalletUpdateResultView is returned from WalletsUpdate.
type WalletUpdateResultView struct {
	UserID   string           `json:"user_id"`
	Updated  map[string]int64 `json:"updated"`
	Previous map[string]int64 `json:"previous"`
}

// WalletLedgerView is a ledger row for runtime.
type WalletLedgerView struct {
	ID         string                 `json:"id"`
	UserID     string                 `json:"user_id"`
	Changeset  map[string]int64       `json:"changeset"`
	Metadata   map[string]interface{} `json:"metadata"`
	CreateTime time.Time              `json:"create_time"`
	UpdateTime time.Time              `json:"update_time"`
}

// ValidatedPurchaseView is a runtime IAP purchase result.
type ValidatedPurchaseView struct {
	UserID        string    `json:"user_id"`
	ProductID     string    `json:"product_id"`
	TransactionID string    `json:"transaction_id"`
	Store         int       `json:"store"`
	PurchaseTime  time.Time `json:"purchase_time"`
	SeenBefore    bool      `json:"seen_before"`
	Environment   int       `json:"environment"`
}

// ValidatedSubscriptionView is a runtime IAP subscription result.
type ValidatedSubscriptionView struct {
	UserID                string    `json:"user_id"`
	ProductID             string    `json:"product_id"`
	OriginalTransactionID string    `json:"original_transaction_id"`
	Store                 int       `json:"store"`
	PurchaseTime          time.Time `json:"purchase_time"`
	ExpireTime            time.Time `json:"expire_time"`
	Active                bool      `json:"active"`
	SeenBefore            bool      `json:"seen_before"`
	Environment           int       `json:"environment"`
}

type StorageObject struct {
	Collection      string    `json:"collection"`
	Key             string    `json:"key"`
	UserID          string    `json:"user_id"`
	Value           string    `json:"value"`
	Version         string    `json:"version"`
	PermissionRead  int32     `json:"permission_read"`
	PermissionWrite int32     `json:"permission_write"`
	CreateTime      time.Time `json:"create_time"`
	UpdateTime      time.Time `json:"update_time"`
}

type StorageObjectAck struct {
	Collection string    `json:"collection"`
	Key        string    `json:"key"`
	UserID     string    `json:"user_id"`
	Version    string    `json:"version"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
}

type StorageObjectList struct {
	Objects []*StorageObject `json:"objects"`
	Cursor  string           `json:"cursor,omitempty"`
}

type Account struct {
	ID         string    `json:"id"`
	Username   string    `json:"username"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
}

type UserView struct {
	ID          string    `json:"id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	AvatarURL   string    `json:"avatar_url"`
	LangTag     string    `json:"lang_tag"`
	Location    string    `json:"location"`
	Timezone    string    `json:"timezone"`
	Metadata    string    `json:"metadata"`
	Online      bool      `json:"online"`
	EdgeCount   int       `json:"edge_count"`
	CreateTime  time.Time `json:"create_time"`
	UpdateTime  time.Time `json:"update_time"`
}

type UsersList struct {
	Users []*UserView `json:"users"`
}

type LeaderboardRecord struct {
	LeaderboardID string    `json:"leaderboard_id"`
	OwnerID       string    `json:"owner_id"`
	Username      string    `json:"username"`
	Score         int64     `json:"score"`
	Subscore      int64     `json:"subscore"`
	NumScore      int       `json:"num_score"`
	MaxNumScore   int       `json:"max_num_score"`
	Metadata      string    `json:"metadata"`
	CreateTime    time.Time `json:"create_time"`
	UpdateTime    time.Time `json:"update_time"`
	ExpiryTime    time.Time `json:"expiry_time"`
	Rank          int64     `json:"rank"`
}

// Leaderboard is a runtime-facing leaderboard/tournament config snapshot.
type Leaderboard struct {
	ID            string    `json:"id"`
	Authoritative bool      `json:"authoritative"`
	SortOrder     int       `json:"sort_order"`
	Operator      int       `json:"operator"`
	ResetSchedule string    `json:"reset_schedule"`
	Metadata      string    `json:"metadata"`
	CreateTime    time.Time `json:"create_time"`
	Category      int       `json:"category"`
	Description   string    `json:"description"`
	Duration      int       `json:"duration"`
	EndTime       time.Time `json:"end_time"`
	JoinRequired  bool      `json:"join_required"`
	MaxSize       int       `json:"max_size"`
	MaxNumScore   int       `json:"max_num_score"`
	Title         string    `json:"title"`
	Size          int       `json:"size"`
	StartTime     time.Time `json:"start_time"`
	EnableRanks   bool      `json:"enable_ranks"`
}

// TournamentView is a runtime-facing tournament listing entry.
type TournamentView struct {
	*Leaderboard
	CanEnter    bool  `json:"can_enter"`
	StartActive int64 `json:"start_active"`
	EndActive   int64 `json:"end_active"`
	PrevReset   int64 `json:"prev_reset"`
	NextReset   int64 `json:"next_reset"`
}

type LeaderboardRecordList struct {
	Records      []*LeaderboardRecord `json:"records,omitempty"`
	OwnerRecords []*LeaderboardRecord `json:"owner_records,omitempty"`
	NextCursor   string               `json:"next_cursor,omitempty"`
	PrevCursor   string               `json:"prev_cursor,omitempty"`
}

type TournamentList struct {
	Tournaments []*TournamentView `json:"tournaments"`
	NextCursor  string            `json:"next_cursor,omitempty"`
}

type AuthenticateEmailRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Register    bool   `json:"register"`
}

// Auth session/account hook targets (also registrable via RegisterBeforeRt / RegisterAfterRt):
// AuthenticateEmail, AuthenticateCustom, AuthenticateDevice, AuthenticateApple,
// AuthenticateGoogle, AuthenticateFacebook, AuthenticateSteam, AuthenticateGameCenter,
// SessionRefresh, SessionLogout, GetAccount, UpdateAccount, DeleteAccount,
// Link*, Unlink*.

type WriteStorageObjectsRequest struct {
	Objects []*StorageWrite `json:"objects"`
}

type AddFriendsRequest struct {
	IDs       []string `json:"ids"`
	Usernames []string `json:"usernames"`
}

// FriendEdge is a runtime representation of a friend list entry.
type FriendEdge struct {
	UserID      string    `json:"user_id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	State       int       `json:"state"`
	UpdateTime  time.Time `json:"update_time"`
	Metadata    string    `json:"metadata"`
}

type Friend struct {
	User       *UserView `json:"user"`
	State      int       `json:"state"`
	UpdateTime time.Time `json:"update_time"`
	Metadata   string    `json:"metadata"`
}

type FriendList struct {
	Friends    []*Friend `json:"friends"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// FriendOfFriendEdge is a runtime FoF entry.
type FriendOfFriendEdge struct {
	Referrer string `json:"referrer"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

type FriendOfFriend struct {
	Referrer string    `json:"referrer"`
	User     *UserView `json:"user"`
}

type FriendsOfFriendsList struct {
	FriendsOfFriends []*FriendOfFriend `json:"friends_of_friends"`
	Cursor           string            `json:"cursor,omitempty"`
}

// PartyListEntry is a discoverable party for runtime PartyList.
type PartyListEntry struct {
	ID      string `json:"id"`
	Open    bool   `json:"open"`
	Hidden  bool   `json:"hidden"`
	MaxSize int    `json:"max_size"`
	Label   string `json:"label"`
}

type PartyListView struct {
	Parties []*PartyListEntry `json:"parties"`
	Cursor  string            `json:"cursor,omitempty"`
}

// ChannelMessageAckView is returned by runtime channel send/update/remove.
type ChannelMessageAckView struct {
	ChannelID  string    `json:"channel_id"`
	MessageID  string    `json:"message_id"`
	Code       int16     `json:"code"`
	Username   string    `json:"username"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
	Persistent bool      `json:"persistent"`
	RoomName   string    `json:"room_name,omitempty"`
	GroupID    string    `json:"group_id,omitempty"`
	UserIDOne  string    `json:"user_id_one,omitempty"`
	UserIDTwo  string    `json:"user_id_two,omitempty"`
}

// ChannelMessageView is a listed channel message.
type ChannelMessageView struct {
	ChannelID  string    `json:"channel_id"`
	MessageID  string    `json:"message_id"`
	Code       int16     `json:"code"`
	SenderID   string    `json:"sender_id"`
	Username   string    `json:"username"`
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
	Persistent bool      `json:"persistent"`
	RoomName   string    `json:"room_name,omitempty"`
	GroupID    string    `json:"group_id,omitempty"`
	UserIDOne  string    `json:"user_id_one,omitempty"`
	UserIDTwo  string    `json:"user_id_two,omitempty"`
}

type ChannelMessageList struct {
	Messages        []*ChannelMessageView `json:"messages"`
	NextCursor      string                `json:"next_cursor,omitempty"`
	PrevCursor      string                `json:"prev_cursor,omitempty"`
	CacheableCursor string                `json:"cacheable_cursor,omitempty"`
}

// NotificationView is a runtime notification record.
type NotificationView struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	Subject    string    `json:"subject"`
	Content    string    `json:"content"`
	Code       int16     `json:"code"`
	SenderID   string    `json:"sender_id"`
	CreateTime time.Time `json:"create_time"`
	Persistent bool      `json:"persistent"`
}

type NotificationList struct {
	Notifications   []*NotificationView `json:"notifications"`
	CacheableCursor string              `json:"cacheable_cursor,omitempty"`
}

// NotificationSendParams is a batch send entry.
type NotificationSendParams struct {
	UserID     string
	Subject    string
	Content    map[string]interface{}
	Code       int
	SenderID   string
	Persistent bool
}

// NotificationUpdateParams is a partial notification update.
type NotificationUpdateParams struct {
	ID       string
	Subject  *string
	Content  *string
	SenderID *string
}

// GroupView is a runtime group record.
type GroupView struct {
	ID          string `json:"id"`
	CreatorID   string `json:"creator_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	AvatarURL   string `json:"avatar_url"`
	LangTag     string `json:"lang_tag"`
	Metadata    string `json:"metadata"`
	Open        bool   `json:"open"`
	EdgeCount   int    `json:"edge_count"`
	MaxCount    int    `json:"max_count"`
}

type GroupList struct {
	Groups     []*GroupView `json:"groups"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// GroupUserView is a runtime group member row.
type GroupUserView struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	State    int    `json:"state"`
}

type GroupUser struct {
	User  *UserView `json:"user"`
	State int       `json:"state"`
}

type GroupUserList struct {
	GroupUsers []*GroupUser `json:"group_users"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// UserGroupView is a runtime user→group relation.
type UserGroupView struct {
	Group *GroupView `json:"group"`
	State int        `json:"state"`
}

type UserGroupList struct {
	UserGroups []*UserGroupView `json:"user_groups"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type ValidatePurchaseResponse struct {
	ValidatedPurchases []*ValidatedPurchaseView `json:"validated_purchases"`
}

type ValidateSubscriptionResponse struct {
	ValidatedSubscription *ValidatedSubscriptionView `json:"validated_subscription"`
}

type SubscriptionList struct {
	ValidatedSubscriptions []*ValidatedSubscriptionView `json:"validated_subscriptions"`
	Cursor                 string                       `json:"cursor,omitempty"`
	PrevCursor             string                       `json:"prev_cursor,omitempty"`
}

type MatchList struct {
	Matches []*MatchInfo `json:"matches"`
}

type MatchmakerStatsView struct {
	TicketCount            int    `json:"ticket_count"`
	OldestTicketCreateTime string `json:"oldest_ticket_create_time"`
	CompletionCount        int    `json:"completion_count"`
}

type JoinGroupRequest struct {
	GroupID string `json:"group_id"`
}

type Session struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
}

type StorageObjectAcks struct {
	Acks []*StorageObjectAck `json:"acks"`
}

// RPCHandler represents a custom client-callable RPC endpoint handler.
// Go native handlers receive logger, db, and nk for full server API access.
type RPCHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error)

// BeforeHook represents an HTTP/gRPC before request interceptor.
// Go native before hooks receive logger, db, and nk for full server API access.
type BeforeHook func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error)

// AfterHook represents an HTTP/gRPC after request interceptor.
// Go native after hooks receive logger, db, and nk for full server API access.
type AfterHook func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error

// EventHandler represents an asynchronous event handler.
type EventHandler func(ctx context.Context, logger Logger, evt *Event)

// Event represents a server-side event (session start/end, matchmaker match, etc.).
type Event struct {
	Name       string
	Properties map[string]string
	Timestamp  int64
}

// MatchHandlerFactory creates a new match handler instance.
type MatchHandlerFactory func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) (Match, error)

// Presence represents a client presence within a match.
type Presence interface {
	GetUserId() string
	GetSessionId() string
	GetNodeId() string
	GetUsername() string
}

// MatchData represents match data messages passed into MatchLoop.
type MatchData interface {
	Presence
	GetOpCode() int64
	GetData() []byte
	GetReliable() bool
	GetReceiveTime() int64
}

// Match represents an authoritative match handler.
type Match interface {
	MatchInit(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, params map[string]interface{}) (interface{}, int, string)
	MatchJoinAttempt(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presence Presence, metadata map[string]string) (interface{}, bool, string)
	MatchJoin(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []Presence) interface{}
	MatchLeave(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []Presence) interface{}
	MatchLoop(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []MatchData) interface{}
	MatchTerminate(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{}
	MatchSignal(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, data string) (interface{}, string)
}

// MatchmakerMatchedHandler handles matchmaker match events.
type MatchmakerMatchedHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, entries []interface{}) (string, error)

// MatchmakerProcessorHandler replaces default Process pairing. Tickets are *matchmaker.Ticket values.
type MatchmakerProcessorHandler func(ctx context.Context, tickets []interface{}) [][]interface{}

// MatchmakerOverrideHandler rewrites candidate match groups after default Process pairing.
type MatchmakerOverrideHandler func(ctx context.Context, matches [][]interface{}) [][]interface{}

// LeaderboardResetHandler handles leaderboard reset events.
type LeaderboardResetHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, leaderboardID string, reset int64) error

// TournamentEndHandler handles tournament end events.
type TournamentEndHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, tournamentID string, end int64, reset int64) error

// TournamentResetHandler handles tournament reset events.
type TournamentResetHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, tournamentID string, end int64, reset int64) error

// ShutdownHandler runs once when the server receives a termination signal.
type ShutdownHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule)

// RuntimeHTTPHandler is a custom HTTP route registered by a Go runtime module.
type RuntimeHTTPHandler struct {
	PathPattern string
	Handler     func(http.ResponseWriter, *http.Request)
	Methods     []string
}

// CronJob represents a scheduled event job (UGE extension; ADR-0025).
type CronJob struct {
	Schedule string
	Handler  func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error
	Expr     *cronexpr.Expression // parsed at RegisterCron
}

// Initializer provides registration methods during module initialization.
// Available only during InitModule execution — do not store or cache globally.
type Initializer interface {
	RegisterRpc(id string, fn RPCHandler) error
	RegisterBeforeRt(id string, fn BeforeHook) error
	RegisterAfterRt(id string, fn AfterHook) error
	RegisterMatch(name string, fn MatchHandlerFactory) error
	RegisterMatchmakerMatched(fn MatchmakerMatchedHandler) error
	RegisterMatchmakerOverride(fn MatchmakerOverrideHandler) error
	RegisterMatchmakerProcessor(fn MatchmakerProcessorHandler) error
	RegisterLeaderboardReset(fn LeaderboardResetHandler) error
	RegisterTournamentEnd(fn TournamentEndHandler) error
	RegisterTournamentReset(fn TournamentResetHandler) error
	RegisterEvent(fn EventHandler) error
	RegisterEventSessionStart(fn EventHandler) error
	RegisterEventSessionEnd(fn EventHandler) error
	RegisterShutdown(fn ShutdownHandler) error
	RegisterHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) error
	RegisterConsoleHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) error
	RegisterCron(name, schedule string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error) error

	// Specific type-safe before hooks
	RegisterBeforeAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateEmailRequest) (*AuthenticateEmailRequest, error)) error
	RegisterBeforeAuthenticateDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateDeviceRequest) (*AuthenticateDeviceRequest, error)) error
	RegisterBeforeAuthenticateCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateCustomRequest) (*AuthenticateCustomRequest, error)) error
	RegisterBeforeAuthenticateApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateAppleRequest) (*AuthenticateAppleRequest, error)) error
	RegisterBeforeAuthenticateGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateGoogleRequest) (*AuthenticateGoogleRequest, error)) error
	RegisterBeforeAuthenticateFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateFacebookRequest) (*AuthenticateFacebookRequest, error)) error
	RegisterBeforeAuthenticateSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateSteamRequest) (*AuthenticateSteamRequest, error)) error
	RegisterBeforeSessionRefresh(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionRefreshRequest) (*SessionRefreshRequest, error)) error
	RegisterBeforeSessionLogout(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionLogoutRequest) (*SessionLogoutRequest, error)) error
	RegisterBeforeWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteStorageObjectsRequest) (*WriteStorageObjectsRequest, error)) error
	RegisterBeforeReadStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ReadStorageObjectsRequest) (*ReadStorageObjectsRequest, error)) error
	RegisterBeforeDeleteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteStorageObjectsRequest) (*DeleteStorageObjectsRequest, error)) error
	RegisterBeforeListStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListStorageObjectsRequest) (*ListStorageObjectsRequest, error)) error
	RegisterBeforeAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) (*AddFriendsRequest, error)) error
	RegisterBeforeDeleteFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteFriendsRequest) (*DeleteFriendsRequest, error)) error
	RegisterBeforeListFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListFriendsRequest) (*ListFriendsRequest, error)) error
	RegisterBeforeBlockFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BlockFriendsRequest) (*BlockFriendsRequest, error)) error
	RegisterBeforeWriteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteLeaderboardRecordRequest) (*WriteLeaderboardRecordRequest, error)) error
	RegisterBeforeListLeaderboardRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListLeaderboardRecordsRequest) (*ListLeaderboardRecordsRequest, error)) error
	RegisterBeforeJoinTournament(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinTournamentRequest) (*JoinTournamentRequest, error)) error
	RegisterBeforeWriteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteTournamentRecordRequest) (*WriteTournamentRecordRequest, error)) error
	RegisterBeforeJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) (*JoinGroupRequest, error)) error
	RegisterBeforeCreateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreateGroupRequest) (*CreateGroupRequest, error)) error
	RegisterBeforeLeaveGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeaveGroupRequest) (*LeaveGroupRequest, error)) error
	RegisterBeforeCreateMatch(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreateMatchRequest) (*CreateMatchRequest, error)) error
	RegisterBeforeListMatches(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListMatchesRequest) (*ListMatchesRequest, error)) error
	RegisterBeforeListChannelMessages(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListChannelMessagesRequest) (*ListChannelMessagesRequest, error)) error
	RegisterBeforeEvent(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *EventRequest) (*EventRequest, error)) error
	RegisterBeforeGetAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetAccountRequest) (*GetAccountRequest, error)) error
	RegisterBeforeUpdateAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateAccountRequest) (*UpdateAccountRequest, error)) error
	RegisterBeforeDeleteAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteAccountRequest) (*DeleteAccountRequest, error)) error
	RegisterBeforeGetWallet(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetWalletRequest) (*GetWalletRequest, error)) error
	RegisterBeforeListWalletLedger(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListWalletLedgerRequest) (*ListWalletLedgerRequest, error)) error

	RegisterBeforeLinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkAppleRequest) (*LinkAppleRequest, error)) error
	RegisterBeforeLinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGoogleRequest) (*LinkGoogleRequest, error)) error
	RegisterBeforeLinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookRequest) (*LinkFacebookRequest, error)) error
	RegisterBeforeLinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkSteamRequest) (*LinkSteamRequest, error)) error
	RegisterBeforeLinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkDeviceRequest) (*LinkDeviceRequest, error)) error
	RegisterBeforeLinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkCustomRequest) (*LinkCustomRequest, error)) error
	RegisterBeforeLinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkEmailRequest) (*LinkEmailRequest, error)) error
	RegisterBeforeUnlinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkAppleRequest) (*UnlinkAppleRequest, error)) error
	RegisterBeforeUnlinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGoogleRequest) (*UnlinkGoogleRequest, error)) error
	RegisterBeforeUnlinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookRequest) (*UnlinkFacebookRequest, error)) error
	RegisterBeforeUnlinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkSteamRequest) (*UnlinkSteamRequest, error)) error
	RegisterBeforeUnlinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkDeviceRequest) (*UnlinkDeviceRequest, error)) error
	RegisterBeforeUnlinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkCustomRequest) (*UnlinkCustomRequest, error)) error
	RegisterBeforeUnlinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkEmailRequest) (*UnlinkEmailRequest, error)) error
	RegisterBeforeValidatePurchaseApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseAppleRequest) (*ValidatePurchaseAppleRequest, error)) error
	RegisterBeforeValidatePurchaseGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseGoogleRequest) (*ValidatePurchaseGoogleRequest, error)) error
	RegisterBeforeValidatePurchaseHuawei(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseHuaweiRequest) (*ValidatePurchaseHuaweiRequest, error)) error
	RegisterBeforeValidateSubscriptionApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidateSubscriptionAppleRequest) (*ValidateSubscriptionAppleRequest, error)) error
	RegisterBeforeValidateSubscriptionGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidateSubscriptionGoogleRequest) (*ValidateSubscriptionGoogleRequest, error)) error
	RegisterBeforeBanGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BanGroupUsersRequest) (*BanGroupUsersRequest, error)) error
	RegisterBeforeKickGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *KickGroupUsersRequest) (*KickGroupUsersRequest, error)) error
	RegisterBeforePromoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *PromoteGroupUsersRequest) (*PromoteGroupUsersRequest, error)) error
	RegisterBeforeDemoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DemoteGroupUsersRequest) (*DemoteGroupUsersRequest, error)) error
	RegisterBeforeAddGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddGroupUsersRequest) (*AddGroupUsersRequest, error)) error
	RegisterBeforeUpdateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateGroupRequest) (*UpdateGroupRequest, error)) error
	RegisterBeforeDeleteGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteGroupRequest) (*DeleteGroupRequest, error)) error
	RegisterBeforeListGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListGroupUsersRequest) (*ListGroupUsersRequest, error)) error
	RegisterBeforeListUserGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListUserGroupsRequest) (*ListUserGroupsRequest, error)) error
	RegisterBeforeListNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListNotificationsRequest) (*ListNotificationsRequest, error)) error
	RegisterBeforeDeleteNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteNotificationsRequest) (*DeleteNotificationsRequest, error)) error
	RegisterBeforeListFriendsOfFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListFriendsOfFriendsRequest) (*ListFriendsOfFriendsRequest, error)) error
	RegisterBeforeCreateParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *CreatePartyRequest) (*CreatePartyRequest, error)) error
	RegisterBeforeJoinParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinPartyRequest) (*JoinPartyRequest, error)) error
	RegisterBeforeLeaveParty(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeavePartyRequest) (*LeavePartyRequest, error)) error
	RegisterBeforeListTournaments(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentsRequest) (*ListTournamentsRequest, error)) error

	RegisterBeforeAuthenticateGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateGameCenterRequest) (*AuthenticateGameCenterRequest, error)) error
	RegisterBeforeAuthenticateFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateFacebookInstantGameRequest) (*AuthenticateFacebookInstantGameRequest, error)) error
	RegisterBeforeLinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGameCenterRequest) (*LinkGameCenterRequest, error)) error
	RegisterBeforeLinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookInstantGameRequest) (*LinkFacebookInstantGameRequest, error)) error
	RegisterBeforeUnlinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGameCenterRequest) (*UnlinkGameCenterRequest, error)) error
	RegisterBeforeUnlinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookInstantGameRequest) (*UnlinkFacebookInstantGameRequest, error)) error
	RegisterBeforeValidatePurchaseFacebookInstant(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseFacebookInstantRequest) (*ValidatePurchaseFacebookInstantRequest, error)) error
	RegisterBeforeValidatePurchaseSamsung(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ValidatePurchaseSamsungRequest) (*ValidatePurchaseSamsungRequest, error)) error
	RegisterBeforeGetSubscription(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetSubscriptionRequest) (*GetSubscriptionRequest, error)) error
	RegisterBeforeListSubscriptions(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListSubscriptionsRequest) (*ListSubscriptionsRequest, error)) error
	RegisterBeforeGetMatchmakerStats(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetMatchmakerStatsRequest) (*GetMatchmakerStatsRequest, error)) error
	RegisterBeforeListParties(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListPartiesRequest) (*ListPartiesRequest, error)) error
	RegisterBeforeGetUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *GetUsersRequest) (*GetUsersRequest, error)) error
	RegisterBeforeListGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListGroupsRequest) (*ListGroupsRequest, error)) error
	RegisterBeforeDeleteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteLeaderboardRecordRequest) (*DeleteLeaderboardRecordRequest, error)) error
	RegisterBeforeDeleteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteTournamentRecordRequest) (*DeleteTournamentRecordRequest, error)) error
	RegisterBeforeListLeaderboardRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListLeaderboardRecordsAroundOwnerRequest) (*ListLeaderboardRecordsAroundOwnerRequest, error)) error
	RegisterBeforeListTournamentRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentRecordsRequest) (*ListTournamentRecordsRequest, error)) error
	RegisterBeforeListTournamentRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ListTournamentRecordsAroundOwnerRequest) (*ListTournamentRecordsAroundOwnerRequest, error)) error
	RegisterBeforeImportFacebookFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportFacebookFriendsRequest) (*ImportFacebookFriendsRequest, error)) error
	RegisterBeforeImportSteamFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportSteamFriendsRequest) (*ImportSteamFriendsRequest, error)) error

	// Specific type-safe after hooks
	RegisterAfterAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateEmailRequest) error) error
	RegisterAfterWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectAcks, in *WriteStorageObjectsRequest) error) error
	RegisterAfterAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) error) error
	RegisterAfterJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) error) error
	RegisterAfterAuthenticateDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateDeviceRequest) error) error
	RegisterAfterAuthenticateCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateCustomRequest) error) error
	RegisterAfterAuthenticateApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateAppleRequest) error) error
	RegisterAfterAuthenticateGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateGoogleRequest) error) error
	RegisterAfterAuthenticateFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateFacebookRequest) error) error
	RegisterAfterAuthenticateSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateSteamRequest) error) error
	RegisterAfterSessionRefresh(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *SessionRefreshRequest) error) error
	RegisterAfterReadStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectList, in *ReadStorageObjectsRequest) error) error
	RegisterAfterDeleteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteStorageObjectsRequest) error) error
	RegisterAfterDeleteFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteFriendsRequest) error) error
	RegisterAfterBlockFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BlockFriendsRequest) error) error
	RegisterAfterWriteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecord, in *WriteLeaderboardRecordRequest) error) error
	RegisterAfterJoinTournament(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinTournamentRequest) error) error
	RegisterAfterCreateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupView, in *CreateGroupRequest) error) error
	RegisterAfterLeaveGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LeaveGroupRequest) error) error
	RegisterAfterBanGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *BanGroupUsersRequest) error) error
	RegisterAfterKickGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *KickGroupUsersRequest) error) error
	RegisterAfterValidatePurchaseApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseAppleRequest) error) error
	RegisterAfterValidatePurchaseGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseGoogleRequest) error) error
	RegisterAfterLinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkAppleRequest) error) error
	RegisterAfterLinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGoogleRequest) error) error
	RegisterAfterUpdateAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateAccountRequest) error) error
	RegisterAfterDeleteNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteNotificationsRequest) error) error
	RegisterAfterAuthenticateGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateGameCenterRequest) error) error
	RegisterAfterAuthenticateFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateFacebookInstantGameRequest) error) error
	RegisterAfterLinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookRequest) error) error
	RegisterAfterLinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkSteamRequest) error) error
	RegisterAfterLinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkDeviceRequest) error) error
	RegisterAfterLinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkCustomRequest) error) error
	RegisterAfterLinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkEmailRequest) error) error
	RegisterAfterLinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkGameCenterRequest) error) error
	RegisterAfterLinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *LinkFacebookInstantGameRequest) error) error
	RegisterAfterUnlinkApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkAppleRequest) error) error
	RegisterAfterUnlinkGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGoogleRequest) error) error
	RegisterAfterUnlinkFacebook(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookRequest) error) error
	RegisterAfterUnlinkSteam(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkSteamRequest) error) error
	RegisterAfterUnlinkDevice(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkDeviceRequest) error) error
	RegisterAfterUnlinkCustom(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkCustomRequest) error) error
	RegisterAfterUnlinkEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkEmailRequest) error) error
	RegisterAfterUnlinkGameCenter(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkGameCenterRequest) error) error
	RegisterAfterUnlinkFacebookInstantGame(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UnlinkFacebookInstantGameRequest) error) error
	RegisterAfterListStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectList, in *ListStorageObjectsRequest) error) error
	RegisterAfterListFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *FriendList, in *ListFriendsRequest) error) error
	RegisterAfterListFriendsOfFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *FriendsOfFriendsList, in *ListFriendsOfFriendsRequest) error) error
	RegisterAfterListLeaderboardRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListLeaderboardRecordsRequest) error) error
	RegisterAfterListLeaderboardRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListLeaderboardRecordsAroundOwnerRequest) error) error
	RegisterAfterDeleteLeaderboardRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteLeaderboardRecordRequest) error) error
	RegisterAfterWriteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecord, in *WriteTournamentRecordRequest) error) error
	RegisterAfterListTournaments(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *TournamentList, in *ListTournamentsRequest) error) error
	RegisterAfterListTournamentRecords(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListTournamentRecordsRequest) error) error
	RegisterAfterListTournamentRecordsAroundOwner(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *LeaderboardRecordList, in *ListTournamentRecordsAroundOwnerRequest) error) error
	RegisterAfterDeleteTournamentRecord(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteTournamentRecordRequest) error) error
	RegisterAfterPromoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *PromoteGroupUsersRequest) error) error
	RegisterAfterDemoteGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DemoteGroupUsersRequest) error) error
	RegisterAfterAddGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddGroupUsersRequest) error) error
	RegisterAfterUpdateGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *UpdateGroupRequest) error) error
	RegisterAfterDeleteGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteGroupRequest) error) error
	RegisterAfterListGroupUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupUserList, in *ListGroupUsersRequest) error) error
	RegisterAfterListUserGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *UserGroupList, in *ListUserGroupsRequest) error) error
	RegisterAfterListGroups(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *GroupList, in *ListGroupsRequest) error) error
	RegisterAfterListNotifications(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *NotificationList, in *ListNotificationsRequest) error) error
	RegisterAfterValidatePurchaseHuawei(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseHuaweiRequest) error) error
	RegisterAfterValidatePurchaseFacebookInstant(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseFacebookInstantRequest) error) error
	RegisterAfterValidatePurchaseSamsung(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatePurchaseResponse, in *ValidatePurchaseSamsungRequest) error) error
	RegisterAfterValidateSubscriptionApple(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidateSubscriptionResponse, in *ValidateSubscriptionAppleRequest) error) error
	RegisterAfterValidateSubscriptionGoogle(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidateSubscriptionResponse, in *ValidateSubscriptionGoogleRequest) error) error
	RegisterAfterGetSubscription(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ValidatedSubscriptionView, in *GetSubscriptionRequest) error) error
	RegisterAfterListSubscriptions(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *SubscriptionList, in *ListSubscriptionsRequest) error) error
	RegisterAfterGetUsers(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *UsersList, in *GetUsersRequest) error) error
	RegisterAfterGetAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Account, in *GetAccountRequest) error) error
	RegisterAfterDeleteAccount(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *DeleteAccountRequest) error) error
	RegisterAfterSessionLogout(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *SessionLogoutRequest) error) error
	RegisterAfterImportFacebookFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportFacebookFriendsRequest) error) error
	RegisterAfterImportSteamFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *ImportSteamFriendsRequest) error) error
	RegisterAfterListMatches(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *MatchList, in *ListMatchesRequest) error) error
	RegisterAfterListChannelMessages(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *ChannelMessageList, in *ListChannelMessagesRequest) error) error
	RegisterAfterGetMatchmakerStats(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *MatchmakerStatsView, in *GetMatchmakerStatsRequest) error) error
	RegisterAfterListParties(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *PartyListView, in *ListPartiesRequest) error) error

	RegisterStorageIndex(name, collection, key string, fields, sortableFields []string, maxEntries int, indexOnly bool) error
	RegisterStorageIndexFilter(indexName string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, write *StorageWrite) bool) error
	RegisterFleetManager(fm fleet.Manager) error

	RegisterPurchaseNotificationApple(fn PurchaseNotificationAppleHandler) error
	RegisterPurchaseNotificationGoogle(fn PurchaseNotificationGoogleHandler) error
	RegisterSubscriptionNotificationApple(fn SubscriptionNotificationAppleHandler) error
	RegisterSubscriptionNotificationGoogle(fn SubscriptionNotificationGoogleHandler) error
}

// PurchaseNotificationAppleHandler is invoked for Apple purchase RTDN events.
type PurchaseNotificationAppleHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, notificationType int, purchase *ValidatedPurchaseView, rawPayload string) error

// PurchaseNotificationGoogleHandler is invoked for Google purchase RTDN events.
type PurchaseNotificationGoogleHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, notificationType int, purchase *ValidatedPurchaseView, rawPayload string) error

// SubscriptionNotificationAppleHandler is invoked for Apple subscription RTDN events.
type SubscriptionNotificationAppleHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, notificationType int, subscription *ValidatedSubscriptionView, rawPayload string) error

// SubscriptionNotificationGoogleHandler is invoked for Google subscription RTDN events.
type SubscriptionNotificationGoogleHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, notificationType int, subscription *ValidatedSubscriptionView, rawPayload string) error

// HookRegistry stores registered custom RPCs, before/after hooks, and cron jobs.
type HookRegistry struct {
	mu                             sync.RWMutex
	beforeHooks                    map[string]BeforeHook
	afterHooks                     map[string]AfterHook
	rpcHooks                       map[string]RPCHandler
	luaBeforeHooks                 map[string]string
	luaAfterHooks                  map[string]string
	luaRpcHooks                    map[string]string
	jsBeforeHooks                  map[string]string
	jsAfterHooks                   map[string]string
	jsRpcHooks                     map[string]string
	eventHandlers                  []EventHandler
	sessionStartHandlers           []EventHandler
	sessionEndHandlers             []EventHandler
	shutdownHandlers               []ShutdownHandler
	httpHandlers                   []*RuntimeHTTPHandler
	consoleHTTPHandlers            []*RuntimeHTTPHandler
	matchHandlers                  map[string]MatchHandlerFactory
	cronJobs                       map[string]*CronJob
	matchmakerMatchedHandler       MatchmakerMatchedHandler
	matchmakerOverrideHandler      MatchmakerOverrideHandler
	matchmakerProcessorHandler     MatchmakerProcessorHandler
	leaderboardResetHandler        LeaderboardResetHandler
	tournamentEndHandler           TournamentEndHandler
	tournamentResetHandler         TournamentResetHandler
	purchaseNotificationApple      PurchaseNotificationAppleHandler
	purchaseNotificationGoogle     PurchaseNotificationGoogleHandler
	subscriptionNotificationApple  SubscriptionNotificationAppleHandler
	subscriptionNotificationGoogle SubscriptionNotificationGoogleHandler
}

// NewHookRegistry creates a new instance of HookRegistry.
func NewHookRegistry() *HookRegistry {
	return &HookRegistry{
		beforeHooks:    make(map[string]BeforeHook),
		afterHooks:     make(map[string]AfterHook),
		rpcHooks:       make(map[string]RPCHandler),
		luaBeforeHooks: make(map[string]string),
		luaAfterHooks:  make(map[string]string),
		luaRpcHooks:    make(map[string]string),
		jsBeforeHooks:  make(map[string]string),
		jsAfterHooks:   make(map[string]string),
		jsRpcHooks:     make(map[string]string),
		eventHandlers:  make([]EventHandler, 0),
		matchHandlers:  make(map[string]MatchHandlerFactory),
		cronJobs:       make(map[string]*CronJob),
	}
}

// RegisterBefore registers a request before hook.
func (hr *HookRegistry) RegisterBefore(name string, hook BeforeHook) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.beforeHooks[name] = hook
}

// GetBefore retrieves a before hook.
func (hr *HookRegistry) GetBefore(name string) (BeforeHook, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	hook, ok := hr.beforeHooks[name]
	return hook, ok
}

// RegisterAfter registers a response after hook.
func (hr *HookRegistry) RegisterAfter(name string, hook AfterHook) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.afterHooks[name] = hook
}

// GetAfter retrieves an after hook.
func (hr *HookRegistry) GetAfter(name string) (AfterHook, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	hook, ok := hr.afterHooks[name]
	return hook, ok
}

// RegisterRPC registers a custom RPC endpoint handler.
func (hr *HookRegistry) RegisterRPC(rpcName string, handler RPCHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.rpcHooks[strings.ToLower(rpcName)] = handler
}

// GetRPC retrieves an RPC handler.
func (hr *HookRegistry) GetRPC(rpcName string) (RPCHandler, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	handler, ok := hr.rpcHooks[strings.ToLower(rpcName)]
	return handler, ok
}

// RegisterEvent registers an event handler.
func (hr *HookRegistry) RegisterEvent(handler EventHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.eventHandlers = append(hr.eventHandlers, handler)
}

// EventHandlers returns a snapshot of registered event handlers.
func (hr *HookRegistry) EventHandlers() []EventHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]EventHandler, len(hr.eventHandlers))
	copy(out, hr.eventHandlers)
	return out
}

// DispatchEvent fans out to all registered event handlers asynchronously.
func (hr *HookRegistry) DispatchEvent(ctx context.Context, logger Logger, evt *Event) {
	if hr == nil || evt == nil {
		return
	}
	handlers := hr.EventHandlers()
	for _, h := range handlers {
		h := h
		go func() {
			defer func() { _ = recover() }()
			h(ctx, logger, evt)
		}()
	}
	if evt.Name == "session_start" {
		for _, h := range hr.SessionStartHandlers() {
			h := h
			go func() {
				defer func() { _ = recover() }()
				h(ctx, logger, evt)
			}()
		}
	}
	if evt.Name == "session_end" {
		for _, h := range hr.SessionEndHandlers() {
			h := h
			go func() {
				defer func() { _ = recover() }()
				h(ctx, logger, evt)
			}()
		}
	}
}

// RegisterEventSessionStart registers a session_start-specific handler.
func (hr *HookRegistry) RegisterEventSessionStart(handler EventHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.sessionStartHandlers = append(hr.sessionStartHandlers, handler)
}

// SessionStartHandlers returns a snapshot of session_start handlers.
func (hr *HookRegistry) SessionStartHandlers() []EventHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]EventHandler, len(hr.sessionStartHandlers))
	copy(out, hr.sessionStartHandlers)
	return out
}

// RegisterEventSessionEnd registers a session_end-specific handler.
func (hr *HookRegistry) RegisterEventSessionEnd(handler EventHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.sessionEndHandlers = append(hr.sessionEndHandlers, handler)
}

// SessionEndHandlers returns a snapshot of session_end handlers.
func (hr *HookRegistry) SessionEndHandlers() []EventHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]EventHandler, len(hr.sessionEndHandlers))
	copy(out, hr.sessionEndHandlers)
	return out
}

// RegisterShutdown registers a shutdown handler.
func (hr *HookRegistry) RegisterShutdown(fn ShutdownHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.shutdownHandlers = append(hr.shutdownHandlers, fn)
}

// ShutdownHandlers returns a snapshot of shutdown handlers.
func (hr *HookRegistry) ShutdownHandlers() []ShutdownHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]ShutdownHandler, len(hr.shutdownHandlers))
	copy(out, hr.shutdownHandlers)
	return out
}

// InvokeShutdown runs all shutdown handlers synchronously with panic recovery.
func (hr *HookRegistry) InvokeShutdown(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) {
	if hr == nil {
		return
	}
	for _, fn := range hr.ShutdownHandlers() {
		func() {
			defer func() { _ = recover() }()
			fn(ctx, logger, db, nk)
		}()
	}
}

// RegisterHttp stores a custom client-API HTTP handler.
func (hr *HookRegistry) RegisterHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.httpHandlers = append(hr.httpHandlers, &RuntimeHTTPHandler{
		PathPattern: pathPattern,
		Handler:     handler,
		Methods:     methods,
	})
}

// HTTPHandlers returns a snapshot of client-API custom HTTP handlers.
func (hr *HookRegistry) HTTPHandlers() []*RuntimeHTTPHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]*RuntimeHTTPHandler, len(hr.httpHandlers))
	copy(out, hr.httpHandlers)
	return out
}

// RegisterConsoleHttp stores a custom console HTTP handler.
func (hr *HookRegistry) RegisterConsoleHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.consoleHTTPHandlers = append(hr.consoleHTTPHandlers, &RuntimeHTTPHandler{
		PathPattern: pathPattern,
		Handler:     handler,
		Methods:     methods,
	})
}

// ConsoleHTTPHandlers returns a snapshot of console custom HTTP handlers.
func (hr *HookRegistry) ConsoleHTTPHandlers() []*RuntimeHTTPHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make([]*RuntimeHTTPHandler, len(hr.consoleHTTPHandlers))
	copy(out, hr.consoleHTTPHandlers)
	return out
}

// MountHTTPHandlers registers custom handlers onto mux.
func MountHTTPHandlers(mux *http.ServeMux, handlers []*RuntimeHTTPHandler) {
	for _, h := range handlers {
		if h == nil || h.Handler == nil || h.PathPattern == "" {
			continue
		}
		handler := h.Handler
		methods := h.Methods
		if len(methods) == 0 {
			mux.HandleFunc(h.PathPattern, handler)
			continue
		}
		allowed := make(map[string]struct{}, len(methods))
		for _, m := range methods {
			allowed[strings.ToUpper(m)] = struct{}{}
		}
		mux.HandleFunc(h.PathPattern, func(w http.ResponseWriter, r *http.Request) {
			if _, ok := allowed[r.Method]; !ok {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			handler(w, r)
		})
	}
}

// RegisterMatch registers a match handler factory.
func (hr *HookRegistry) RegisterMatch(name string, factory MatchHandlerFactory) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if _, exists := hr.matchHandlers[name]; exists {
		return fmt.Errorf("match handler %q already registered", name)
	}
	hr.matchHandlers[name] = factory
	return nil
}

// GetMatch retrieves a registered match handler factory.
func (hr *HookRegistry) GetMatch(name string) (MatchHandlerFactory, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	factory, ok := hr.matchHandlers[name]
	return factory, ok
}

// RegisterCron registers a scheduled background cron job.
// Schedule must be a valid cronexpr (5–7 fields). Invalid schedules are rejected.
func (hr *HookRegistry) RegisterCron(jobName string, cron *CronJob) error {
	if !RegisterCronEnabled() {
		return fmt.Errorf("RegisterCron is disabled; set UGE_ENABLE_REGISTER_CRON=true to enable")
	}
	if jobName == "" {
		return fmt.Errorf("cron job name must not be empty")
	}
	if cron == nil || cron.Handler == nil {
		return fmt.Errorf("cron job %q requires a handler", jobName)
	}
	if strings.TrimSpace(cron.Schedule) == "" {
		return fmt.Errorf("cron job %q requires a schedule", jobName)
	}
	expr, err := cronexpr.Parse(cron.Schedule)
	if err != nil {
		return fmt.Errorf("cron job %q invalid schedule: %w", jobName, err)
	}
	cron.Expr = expr

	hr.mu.Lock()
	defer hr.mu.Unlock()
	if _, exists := hr.cronJobs[jobName]; exists {
		return fmt.Errorf("cron job %q already registered", jobName)
	}
	hr.cronJobs[jobName] = cron
	return nil
}

// ListCronJobs returns a snapshot of registered cron jobs.
func (hr *HookRegistry) ListCronJobs() map[string]*CronJob {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	out := make(map[string]*CronJob, len(hr.cronJobs))
	for k, v := range hr.cronJobs {
		out[k] = v
	}
	return out
}

// GetBeforeHook retrieves a before hook, returning runtime type and script function name if script-based.
func (hr *HookRegistry) GetBeforeHook(name string) (BeforeHook, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	// 1. Go Native Hook (highest precedence)
	if hook, ok := hr.beforeHooks[name]; ok {
		return hook, "go", "", true
	}
	// 2. Gopher-Lua Hook
	if fnName, ok := hr.luaBeforeHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	// 3. Goja JS Hook
	if fnName, ok := hr.jsBeforeHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

// GetAfterHook retrieves an after hook.
func (hr *HookRegistry) GetAfterHook(name string) (AfterHook, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if hook, ok := hr.afterHooks[name]; ok {
		return hook, "go", "", true
	}
	if fnName, ok := hr.luaAfterHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	if fnName, ok := hr.jsAfterHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

// GetRPCHook retrieves an RPC handler.
func (hr *HookRegistry) GetRPCHook(name string) (RPCHandler, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	name = strings.ToLower(name)

	if hook, ok := hr.rpcHooks[name]; ok {
		return hook, "go", "", true
	}
	if fnName, ok := hr.luaRpcHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	if fnName, ok := hr.jsRpcHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

func (hr *HookRegistry) RegisterLuaBefore(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaBeforeHooks[name] = fnName
}

func (hr *HookRegistry) RegisterLuaAfter(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaAfterHooks[name] = fnName
}

func (hr *HookRegistry) RegisterLuaRPC(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaRpcHooks[strings.ToLower(name)] = fnName
}

func (hr *HookRegistry) RegisterJSBefore(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsBeforeHooks[name] = fnName
}

func (hr *HookRegistry) RegisterJSAfter(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsAfterHooks[name] = fnName
}

func (hr *HookRegistry) RegisterJSRPC(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsRpcHooks[strings.ToLower(name)] = fnName
}

func (hr *HookRegistry) RegisterMatchmakerMatched(fn MatchmakerMatchedHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.matchmakerMatchedHandler = fn
}

func (hr *HookRegistry) GetMatchmakerMatched() MatchmakerMatchedHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.matchmakerMatchedHandler
}

func (hr *HookRegistry) RegisterMatchmakerOverride(fn MatchmakerOverrideHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.matchmakerOverrideHandler = fn
}

func (hr *HookRegistry) GetMatchmakerOverride() MatchmakerOverrideHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.matchmakerOverrideHandler
}

func (hr *HookRegistry) RegisterMatchmakerProcessor(fn MatchmakerProcessorHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.matchmakerProcessorHandler = fn
}

func (hr *HookRegistry) GetMatchmakerProcessor() MatchmakerProcessorHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.matchmakerProcessorHandler
}

func (hr *HookRegistry) RegisterLeaderboardReset(fn LeaderboardResetHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.leaderboardResetHandler = fn
}

func (hr *HookRegistry) GetLeaderboardReset() LeaderboardResetHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.leaderboardResetHandler
}

func (hr *HookRegistry) RegisterTournamentEnd(fn TournamentEndHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.tournamentEndHandler = fn
}

func (hr *HookRegistry) GetTournamentEnd() TournamentEndHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.tournamentEndHandler
}

func (hr *HookRegistry) RegisterTournamentReset(fn TournamentResetHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.tournamentResetHandler = fn
}

func (hr *HookRegistry) GetTournamentReset() TournamentResetHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.tournamentResetHandler
}

func (hr *HookRegistry) RegisterPurchaseNotificationApple(fn PurchaseNotificationAppleHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.purchaseNotificationApple = fn
}
func (hr *HookRegistry) GetPurchaseNotificationApple() PurchaseNotificationAppleHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.purchaseNotificationApple
}
func (hr *HookRegistry) RegisterPurchaseNotificationGoogle(fn PurchaseNotificationGoogleHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.purchaseNotificationGoogle = fn
}
func (hr *HookRegistry) GetPurchaseNotificationGoogle() PurchaseNotificationGoogleHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.purchaseNotificationGoogle
}
func (hr *HookRegistry) RegisterSubscriptionNotificationApple(fn SubscriptionNotificationAppleHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.subscriptionNotificationApple = fn
}
func (hr *HookRegistry) GetSubscriptionNotificationApple() SubscriptionNotificationAppleHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.subscriptionNotificationApple
}
func (hr *HookRegistry) RegisterSubscriptionNotificationGoogle(fn SubscriptionNotificationGoogleHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.subscriptionNotificationGoogle = fn
}
func (hr *HookRegistry) GetSubscriptionNotificationGoogle() SubscriptionNotificationGoogleHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.subscriptionNotificationGoogle
}
