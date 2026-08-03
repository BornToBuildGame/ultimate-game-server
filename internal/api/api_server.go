package api

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api/apipb"

	"google.golang.org/protobuf/types/known/emptypb"
)

// ApiServer implements apipb.UltimateGameEngineServer by delegating RPC requests to domain sub-servers.
type ApiServer struct {
	apipb.UnimplementedUltimateGameEngineServer

	authServer         *AuthServer
	storageServer      *StorageServer
	leaderboardServer  *LeaderboardServer
	tournamentServer   *TournamentServer
	friendsServer      *FriendsServer
	groupServer        *GroupServer
	matchmakerServer   *MatchmakerServer
	realtimeServer     *RealtimeServer
	partyServer        *PartyServer
	channelServer      *ChannelServer
	notificationServer *NotificationServer
	iapServer          *IAPServer
	rpcServer          *RpcServer
	userServer         *UserServer
	systemServer       *SystemServer
	eventServer        *EventServer
}

func NewApiServer(
	authServer *AuthServer,
	storageServer *StorageServer,
	leaderboardServer *LeaderboardServer,
	tournamentServer *TournamentServer,
	friendsServer *FriendsServer,
	groupServer *GroupServer,
	matchmakerServer *MatchmakerServer,
	realtimeServer *RealtimeServer,
	partyServer *PartyServer,
	channelServer *ChannelServer,
	notificationServer *NotificationServer,
	iapServer *IAPServer,
	rpcServer *RpcServer,
	userServer *UserServer,
	systemServer *SystemServer,
	eventServer *EventServer,
) *ApiServer {
	return &ApiServer{
		authServer:         authServer,
		storageServer:      storageServer,
		leaderboardServer:  leaderboardServer,
		tournamentServer:   tournamentServer,
		friendsServer:      friendsServer,
		groupServer:        groupServer,
		matchmakerServer:   matchmakerServer,
		realtimeServer:     realtimeServer,
		partyServer:        partyServer,
		channelServer:      channelServer,
		notificationServer: notificationServer,
		iapServer:          iapServer,
		rpcServer:          rpcServer,
		userServer:         userServer,
		systemServer:       systemServer,
		eventServer:        eventServer,
	}
}

func (s *ApiServer) AddFriends(ctx context.Context, req *apipb.AddFriendsRequest) (*emptypb.Empty, error) {
	return s.friendsServer.AddFriends(ctx, req)
}

func (s *ApiServer) AddGroupUsers(ctx context.Context, req *apipb.AddGroupUsersRequest) (*emptypb.Empty, error) {
	return s.groupServer.AddGroupUsers(ctx, req)
}

func (s *ApiServer) SessionRefresh(ctx context.Context, req *apipb.SessionRefreshRequest) (*apipb.Session, error) {
	return s.authServer.SessionRefresh(ctx, req)
}

func (s *ApiServer) SessionLogout(ctx context.Context, req *apipb.SessionLogoutRequest) (*emptypb.Empty, error) {
	return s.authServer.SessionLogout(ctx, req)
}

func (s *ApiServer) AuthenticateApple(ctx context.Context, req *apipb.AuthenticateAppleRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateApple(ctx, req)
}

func (s *ApiServer) AuthenticateCustom(ctx context.Context, req *apipb.AuthenticateCustomRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateCustom(ctx, req)
}

func (s *ApiServer) AuthenticateDevice(ctx context.Context, req *apipb.AuthenticateDeviceRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateDevice(ctx, req)
}

func (s *ApiServer) AuthenticateEmail(ctx context.Context, req *apipb.AuthenticateEmailRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateEmail(ctx, req)
}

func (s *ApiServer) AuthenticateFacebook(ctx context.Context, req *apipb.AuthenticateFacebookRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateFacebook(ctx, req)
}

func (s *ApiServer) AuthenticateFacebookInstantGame(ctx context.Context, req *apipb.AuthenticateFacebookInstantGameRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateFacebookInstantGame(ctx, req)
}

func (s *ApiServer) AuthenticateGameCenter(ctx context.Context, req *apipb.AuthenticateGameCenterRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateGameCenter(ctx, req)
}

func (s *ApiServer) AuthenticateGoogle(ctx context.Context, req *apipb.AuthenticateGoogleRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateGoogle(ctx, req)
}

func (s *ApiServer) AuthenticateSteam(ctx context.Context, req *apipb.AuthenticateSteamRequest) (*apipb.Session, error) {
	return s.authServer.AuthenticateSteam(ctx, req)
}

func (s *ApiServer) BanGroupUsers(ctx context.Context, req *apipb.BanGroupUsersRequest) (*emptypb.Empty, error) {
	return s.groupServer.BanGroupUsers(ctx, req)
}

func (s *ApiServer) BlockFriends(ctx context.Context, req *apipb.BlockFriendsRequest) (*emptypb.Empty, error) {
	return s.friendsServer.BlockFriends(ctx, req)
}

func (s *ApiServer) CreateGroup(ctx context.Context, req *apipb.CreateGroupRequest) (*apipb.Group, error) {
	return s.groupServer.CreateGroup(ctx, req)
}

func (s *ApiServer) DeleteAccount(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return s.authServer.DeleteAccount(ctx, req)
}

func (s *ApiServer) DeleteFriends(ctx context.Context, req *apipb.DeleteFriendsRequest) (*emptypb.Empty, error) {
	return s.friendsServer.DeleteFriends(ctx, req)
}

func (s *ApiServer) DeleteGroup(ctx context.Context, req *apipb.DeleteGroupRequest) (*emptypb.Empty, error) {
	return s.groupServer.DeleteGroup(ctx, req)
}

func (s *ApiServer) DeleteLeaderboardRecord(ctx context.Context, req *apipb.DeleteLeaderboardRecordRequest) (*emptypb.Empty, error) {
	return s.leaderboardServer.DeleteLeaderboardRecord(ctx, req)
}

func (s *ApiServer) DeleteNotifications(ctx context.Context, req *apipb.DeleteNotificationsRequest) (*emptypb.Empty, error) {
	return s.notificationServer.DeleteNotifications(ctx, req)
}

func (s *ApiServer) DeleteTournamentRecord(ctx context.Context, req *apipb.DeleteTournamentRecordRequest) (*emptypb.Empty, error) {
	return s.tournamentServer.DeleteTournamentRecord(ctx, req)
}

func (s *ApiServer) DeleteStorageObjects(ctx context.Context, req *apipb.DeleteStorageObjectsRequest) (*emptypb.Empty, error) {
	return s.storageServer.DeleteStorageObjects(ctx, req)
}

func (s *ApiServer) DemoteGroupUsers(ctx context.Context, req *apipb.DemoteGroupUsersRequest) (*emptypb.Empty, error) {
	return s.groupServer.DemoteGroupUsers(ctx, req)
}

func (s *ApiServer) Event(ctx context.Context, req *apipb.Event) (*emptypb.Empty, error) {
	return s.eventServer.Event(ctx, req)
}

func (s *ApiServer) GetAccount(ctx context.Context, req *emptypb.Empty) (*apipb.Account, error) {
	return s.authServer.GetAccount(ctx, req)
}

func (s *ApiServer) GetUsers(ctx context.Context, req *apipb.GetUsersRequest) (*apipb.Users, error) {
	return s.userServer.GetUsers(ctx, req)
}

func (s *ApiServer) GetSubscription(ctx context.Context, req *apipb.GetSubscriptionRequest) (*apipb.ValidatedSubscription, error) {
	return s.iapServer.GetSubscription(ctx, req)
}

func (s *ApiServer) GetMatchmakerStats(ctx context.Context, req *emptypb.Empty) (*apipb.MatchmakerStats, error) {
	return s.matchmakerServer.GetMatchmakerStats(ctx, req)
}

func (s *ApiServer) Healthcheck(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return s.systemServer.Healthcheck(ctx, req)
}

func (s *ApiServer) ImportFacebookFriends(ctx context.Context, req *apipb.ImportFacebookFriendsRequest) (*emptypb.Empty, error) {
	return s.friendsServer.ImportFacebookFriends(ctx, req)
}

func (s *ApiServer) ImportSteamFriends(ctx context.Context, req *apipb.ImportSteamFriendsRequest) (*emptypb.Empty, error) {
	return s.friendsServer.ImportSteamFriends(ctx, req)
}

func (s *ApiServer) JoinGroup(ctx context.Context, req *apipb.JoinGroupRequest) (*emptypb.Empty, error) {
	return s.groupServer.JoinGroup(ctx, req)
}

func (s *ApiServer) JoinTournament(ctx context.Context, req *apipb.JoinTournamentRequest) (*emptypb.Empty, error) {
	return s.tournamentServer.JoinTournament(ctx, req)
}

func (s *ApiServer) KickGroupUsers(ctx context.Context, req *apipb.KickGroupUsersRequest) (*emptypb.Empty, error) {
	return s.groupServer.KickGroupUsers(ctx, req)
}

func (s *ApiServer) LeaveGroup(ctx context.Context, req *apipb.LeaveGroupRequest) (*emptypb.Empty, error) {
	return s.groupServer.LeaveGroup(ctx, req)
}

func (s *ApiServer) LinkApple(ctx context.Context, req *apipb.AccountApple) (*emptypb.Empty, error) {
	return s.authServer.LinkApple(ctx, req)
}

func (s *ApiServer) LinkCustom(ctx context.Context, req *apipb.AccountCustom) (*emptypb.Empty, error) {
	return s.authServer.LinkCustom(ctx, req)
}

func (s *ApiServer) LinkDevice(ctx context.Context, req *apipb.AccountDevice) (*emptypb.Empty, error) {
	return s.authServer.LinkDevice(ctx, req)
}

func (s *ApiServer) LinkEmail(ctx context.Context, req *apipb.AccountEmail) (*emptypb.Empty, error) {
	return s.authServer.LinkEmail(ctx, req)
}

func (s *ApiServer) LinkFacebook(ctx context.Context, req *apipb.LinkFacebookRequest) (*emptypb.Empty, error) {
	return s.authServer.LinkFacebook(ctx, req)
}

func (s *ApiServer) LinkFacebookInstantGame(ctx context.Context, req *apipb.AccountFacebookInstantGame) (*emptypb.Empty, error) {
	return s.authServer.LinkFacebookInstantGame(ctx, req)
}

func (s *ApiServer) LinkGameCenter(ctx context.Context, req *apipb.AccountGameCenter) (*emptypb.Empty, error) {
	return s.authServer.LinkGameCenter(ctx, req)
}

func (s *ApiServer) LinkGoogle(ctx context.Context, req *apipb.AccountGoogle) (*emptypb.Empty, error) {
	return s.authServer.LinkGoogle(ctx, req)
}

func (s *ApiServer) LinkSteam(ctx context.Context, req *apipb.LinkSteamRequest) (*emptypb.Empty, error) {
	return s.authServer.LinkSteam(ctx, req)
}

func (s *ApiServer) ListChannelMessages(ctx context.Context, req *apipb.ListChannelMessagesRequest) (*apipb.ChannelMessageList, error) {
	return s.channelServer.ListChannelMessages(ctx, req)
}

func (s *ApiServer) ListFriends(ctx context.Context, req *apipb.ListFriendsRequest) (*apipb.FriendList, error) {
	return s.friendsServer.ListFriends(ctx, req)
}

func (s *ApiServer) ListFriendsOfFriends(ctx context.Context, req *apipb.ListFriendsOfFriendsRequest) (*apipb.FriendsOfFriendsList, error) {
	return s.friendsServer.ListFriendsOfFriends(ctx, req)
}

func (s *ApiServer) ListGroupUsers(ctx context.Context, req *apipb.ListGroupUsersRequest) (*apipb.GroupUserList, error) {
	return s.groupServer.ListGroupUsers(ctx, req)
}

func (s *ApiServer) ListGroups(ctx context.Context, req *apipb.ListGroupsRequest) (*apipb.GroupList, error) {
	return s.groupServer.ListGroups(ctx, req)
}

func (s *ApiServer) ListLeaderboardRecords(ctx context.Context, req *apipb.ListLeaderboardRecordsRequest) (*apipb.LeaderboardRecordList, error) {
	return s.leaderboardServer.ListLeaderboardRecords(ctx, req)
}

func (s *ApiServer) ListLeaderboardRecordsAroundOwner(ctx context.Context, req *apipb.ListLeaderboardRecordsAroundOwnerRequest) (*apipb.LeaderboardRecordList, error) {
	return s.leaderboardServer.ListLeaderboardRecordsAroundOwner(ctx, req)
}

func (s *ApiServer) ListMatches(ctx context.Context, req *apipb.ListMatchesRequest) (*apipb.MatchList, error) {
	return s.realtimeServer.ListMatches(ctx, req)
}

func (s *ApiServer) ListParties(ctx context.Context, req *apipb.ListPartiesRequest) (*apipb.PartyList, error) {
	return s.partyServer.ListParties(ctx, req)
}

func (s *ApiServer) ListNotifications(ctx context.Context, req *apipb.ListNotificationsRequest) (*apipb.NotificationList, error) {
	return s.notificationServer.ListNotifications(ctx, req)
}

func (s *ApiServer) ListStorageObjects(ctx context.Context, req *apipb.ListStorageObjectsRequest) (*apipb.StorageObjectList, error) {
	return s.storageServer.ListStorageObjects(ctx, req)
}

func (s *ApiServer) ListSubscriptions(ctx context.Context, req *apipb.ListSubscriptionsRequest) (*apipb.SubscriptionList, error) {
	return s.iapServer.ListSubscriptions(ctx, req)
}

func (s *ApiServer) ListTournaments(ctx context.Context, req *apipb.ListTournamentsRequest) (*apipb.TournamentList, error) {
	return s.tournamentServer.ListTournaments(ctx, req)
}

func (s *ApiServer) ListTournamentRecords(ctx context.Context, req *apipb.ListTournamentRecordsRequest) (*apipb.TournamentRecordList, error) {
	return s.tournamentServer.ListTournamentRecords(ctx, req)
}

func (s *ApiServer) ListTournamentRecordsAroundOwner(ctx context.Context, req *apipb.ListTournamentRecordsAroundOwnerRequest) (*apipb.TournamentRecordList, error) {
	return s.tournamentServer.ListTournamentRecordsAroundOwner(ctx, req)
}

func (s *ApiServer) ListUserGroups(ctx context.Context, req *apipb.ListUserGroupsRequest) (*apipb.UserGroupList, error) {
	return s.groupServer.ListUserGroups(ctx, req)
}

func (s *ApiServer) PromoteGroupUsers(ctx context.Context, req *apipb.PromoteGroupUsersRequest) (*emptypb.Empty, error) {
	return s.groupServer.PromoteGroupUsers(ctx, req)
}

func (s *ApiServer) ReadStorageObjects(ctx context.Context, req *apipb.ReadStorageObjectsRequest) (*apipb.StorageObjects, error) {
	return s.storageServer.ReadStorageObjects(ctx, req)
}

func (s *ApiServer) RpcFunc(ctx context.Context, req *apipb.Rpc) (*apipb.Rpc, error) {
	return s.rpcServer.RpcFunc(ctx, req)
}

func (s *ApiServer) UnlinkApple(ctx context.Context, req *apipb.AccountApple) (*emptypb.Empty, error) {
	return s.authServer.UnlinkApple(ctx, req)
}

func (s *ApiServer) UnlinkCustom(ctx context.Context, req *apipb.AccountCustom) (*emptypb.Empty, error) {
	return s.authServer.UnlinkCustom(ctx, req)
}

func (s *ApiServer) UnlinkDevice(ctx context.Context, req *apipb.AccountDevice) (*emptypb.Empty, error) {
	return s.authServer.UnlinkDevice(ctx, req)
}

func (s *ApiServer) UnlinkEmail(ctx context.Context, req *apipb.AccountEmail) (*emptypb.Empty, error) {
	return s.authServer.UnlinkEmail(ctx, req)
}

func (s *ApiServer) UnlinkFacebook(ctx context.Context, req *apipb.AccountFacebook) (*emptypb.Empty, error) {
	return s.authServer.UnlinkFacebook(ctx, req)
}

func (s *ApiServer) UnlinkFacebookInstantGame(ctx context.Context, req *apipb.AccountFacebookInstantGame) (*emptypb.Empty, error) {
	return s.authServer.UnlinkFacebookInstantGame(ctx, req)
}

func (s *ApiServer) UnlinkGameCenter(ctx context.Context, req *apipb.AccountGameCenter) (*emptypb.Empty, error) {
	return s.authServer.UnlinkGameCenter(ctx, req)
}

func (s *ApiServer) UnlinkGoogle(ctx context.Context, req *apipb.AccountGoogle) (*emptypb.Empty, error) {
	return s.authServer.UnlinkGoogle(ctx, req)
}

func (s *ApiServer) UnlinkSteam(ctx context.Context, req *apipb.AccountSteam) (*emptypb.Empty, error) {
	return s.authServer.UnlinkSteam(ctx, req)
}

func (s *ApiServer) UpdateAccount(ctx context.Context, req *apipb.UpdateAccountRequest) (*emptypb.Empty, error) {
	return s.authServer.UpdateAccount(ctx, req)
}

func (s *ApiServer) UpdateGroup(ctx context.Context, req *apipb.UpdateGroupRequest) (*emptypb.Empty, error) {
	return s.groupServer.UpdateGroup(ctx, req)
}

func (s *ApiServer) ValidatePurchaseApple(ctx context.Context, req *apipb.ValidatePurchaseAppleRequest) (*apipb.ValidatePurchaseResponse, error) {
	return s.iapServer.ValidatePurchaseApple(ctx, req)
}

func (s *ApiServer) ValidatePurchaseGoogle(ctx context.Context, req *apipb.ValidatePurchaseGoogleRequest) (*apipb.ValidatePurchaseResponse, error) {
	return s.iapServer.ValidatePurchaseGoogle(ctx, req)
}

func (s *ApiServer) ValidatePurchaseHuawei(ctx context.Context, req *apipb.ValidatePurchaseHuaweiRequest) (*apipb.ValidatePurchaseResponse, error) {
	return s.iapServer.ValidatePurchaseHuawei(ctx, req)
}

func (s *ApiServer) ValidatePurchaseFacebookInstant(ctx context.Context, req *apipb.ValidatePurchaseFacebookInstantRequest) (*apipb.ValidatePurchaseResponse, error) {
	return s.iapServer.ValidatePurchaseFacebookInstant(ctx, req)
}

func (s *ApiServer) ValidatePurchaseSamsung(ctx context.Context, req *apipb.ValidatePurchaseSamsungRequest) (*apipb.ValidatePurchaseResponse, error) {
	return s.iapServer.ValidatePurchaseSamsung(ctx, req)
}

func (s *ApiServer) ValidateSubscriptionApple(ctx context.Context, req *apipb.ValidateSubscriptionAppleRequest) (*apipb.ValidateSubscriptionResponse, error) {
	return s.iapServer.ValidateSubscriptionApple(ctx, req)
}

func (s *ApiServer) ValidateSubscriptionGoogle(ctx context.Context, req *apipb.ValidateSubscriptionGoogleRequest) (*apipb.ValidateSubscriptionResponse, error) {
	return s.iapServer.ValidateSubscriptionGoogle(ctx, req)
}

func (s *ApiServer) WriteLeaderboardRecord(ctx context.Context, req *apipb.WriteLeaderboardRecordRequest) (*apipb.LeaderboardRecord, error) {
	return s.leaderboardServer.WriteLeaderboardRecord(ctx, req)
}

func (s *ApiServer) WriteStorageObjects(ctx context.Context, req *apipb.WriteStorageObjectsRequest) (*apipb.StorageObjectAcks, error) {
	return s.storageServer.WriteStorageObjects(ctx, req)
}

func (s *ApiServer) WriteTournamentRecord(ctx context.Context, req *apipb.WriteTournamentRecordRequest) (*apipb.LeaderboardRecord, error) {
	return s.tournamentServer.WriteTournamentRecord(ctx, req)
}
