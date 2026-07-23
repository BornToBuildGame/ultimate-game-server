package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
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

type Account struct {
	ID         string    `json:"id"`
	Username   string    `json:"username"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
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
	UserID     string    `json:"user_id"`
	Username   string    `json:"username"`
	DisplayName string   `json:"display_name"`
	State      int       `json:"state"`
	UpdateTime time.Time `json:"update_time"`
	Metadata   string    `json:"metadata"`
}

// FriendOfFriendEdge is a runtime FoF entry.
type FriendOfFriendEdge struct {
	Referrer string `json:"referrer"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
}

// PartyListEntry is a discoverable party for runtime PartyList.
type PartyListEntry struct {
	ID      string `json:"id"`
	Open    bool   `json:"open"`
	Hidden  bool   `json:"hidden"`
	MaxSize int    `json:"max_size"`
	Label   string `json:"label"`
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

// GroupUserView is a runtime group member row.
type GroupUserView struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	State    int    `json:"state"`
}

// UserGroupView is a runtime user→group relation.
type UserGroupView struct {
	Group *GroupView `json:"group"`
	State int        `json:"state"`
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

// CronJob represents a scheduled event job.
type CronJob struct {
	Schedule string
	Handler  func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error
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

	// Specific type-safe before hooks
	RegisterBeforeAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateEmailRequest) (*AuthenticateEmailRequest, error)) error
	RegisterBeforeWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteStorageObjectsRequest) (*WriteStorageObjectsRequest, error)) error
	RegisterBeforeAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) (*AddFriendsRequest, error)) error
	RegisterBeforeJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) (*JoinGroupRequest, error)) error

	// Specific type-safe after hooks
	RegisterAfterAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateEmailRequest) error) error
	RegisterAfterWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectAcks, in *WriteStorageObjectsRequest) error) error
	RegisterAfterAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) error) error
	RegisterAfterJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) error) error
}

// HookRegistry stores registered custom RPCs, before/after hooks, and cron jobs.
type HookRegistry struct {
	mu                       sync.RWMutex
	beforeHooks              map[string]BeforeHook
	afterHooks               map[string]AfterHook
	rpcHooks                 map[string]RPCHandler
	luaBeforeHooks           map[string]string
	luaAfterHooks            map[string]string
	luaRpcHooks              map[string]string
	jsBeforeHooks            map[string]string
	jsAfterHooks             map[string]string
	jsRpcHooks               map[string]string
	eventHandlers            []EventHandler
	matchHandlers            map[string]MatchHandlerFactory
	cronJobs                 map[string]*CronJob
	matchmakerMatchedHandler   MatchmakerMatchedHandler
	matchmakerOverrideHandler  MatchmakerOverrideHandler
	matchmakerProcessorHandler MatchmakerProcessorHandler
	leaderboardResetHandler    LeaderboardResetHandler
	tournamentEndHandler       TournamentEndHandler
	tournamentResetHandler     TournamentResetHandler
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
	hr.rpcHooks[rpcName] = handler
}

// GetRPC retrieves an RPC handler.
func (hr *HookRegistry) GetRPC(rpcName string) (RPCHandler, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	handler, ok := hr.rpcHooks[rpcName]
	return handler, ok
}

// RegisterEvent registers an event handler.
func (hr *HookRegistry) RegisterEvent(handler EventHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.eventHandlers = append(hr.eventHandlers, handler)
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
func (hr *HookRegistry) RegisterCron(jobName string, cron *CronJob) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if _, exists := hr.cronJobs[jobName]; exists {
		return fmt.Errorf("cron job %q already registered", jobName)
	}
	hr.cronJobs[jobName] = cron
	return nil
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
	hr.luaRpcHooks[name] = fnName
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
	hr.jsRpcHooks[name] = fnName
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
