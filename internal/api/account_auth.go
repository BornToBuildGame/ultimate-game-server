package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type authDeviceRequest struct {
	ID       string            `json:"id"`
	Create   *bool             `json:"create"`
	Username string            `json:"username"`
	Vars     map[string]string `json:"vars"`
}

type authSteamRequest struct {
	Account struct {
		Ticket string `json:"ticket"`
	} `json:"account"`
	Ticket   string            `json:"ticket"`
	Create   *bool             `json:"create"`
	Username string            `json:"username"`
	Vars     map[string]string `json:"vars"`
}

type authGameCenterRequest struct {
	Account struct {
		PlayerID     string `json:"player_id"`
		BundleID     string `json:"bundle_id"`
		Timestamp    int64  `json:"timestamp"`
		Salt         string `json:"salt"`
		Signature    string `json:"signature"`
		PublicKeyURL string `json:"public_key_url"`
	} `json:"account"`
	Create   *bool             `json:"create"`
	Username string            `json:"username"`
	Vars     map[string]string `json:"vars"`
}

type logoutRequest struct {
	Token        string `json:"token"`
	RefreshToken string `json:"refresh_token"`
}

type accountUpdateRequest struct {
	Username    *string `json:"username"`
	DisplayName *string `json:"display_name"`
	AvatarURL   *string `json:"avatar_url"`
	LangTag     *string `json:"lang_tag"`
	Location    *string `json:"location"`
	Timezone    *string `json:"timezone"`
	Metadata    *string `json:"metadata"`
}

type unlinkRequest struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

func createFlag(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

func (s *Server) clientIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.RemoteAddr
		if idx := strings.LastIndex(ip, ":"); idx != -1 {
			ip = ip[:idx]
		}
		return ip
	}
	if idx := strings.Index(ip, ","); idx != -1 {
		ip = strings.TrimSpace(ip[:idx])
	}
	return ip
}

func (s *Server) issueSession(w http.ResponseWriter, user *auth.User, vars map[string]string, created bool) {
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		UserID:       user.ID.String(),
		Username:     user.Username,
		Created:      created,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) invokeBefore(ctx context.Context, name string, in interface{}) (interface{}, error) {
	if s.RuntimeManager == nil || !s.RuntimeManager.HasBeforeHook(name) {
		return in, nil
	}
	return s.RuntimeManager.InvokeBeforeHook(ctx, name, in)
}

func (s *Server) invokeAfter(name string, out, in interface{}) {
	if s.RuntimeManager == nil || !s.RuntimeManager.HasAfterHook(name) {
		return
	}
	go func() {
		if err := s.RuntimeManager.InvokeAfterHook(context.Background(), name, out, in); err != nil {
			s.logger.Error("After hook failed", zap.String("hook", name), zap.Error(err))
		}
	}()
}

func (s *Server) handleAuthenticateDevice(w http.ResponseWriter, r *http.Request) {
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	in, err := s.invokeBefore(r.Context(), "AuthenticateDevice", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*authDeviceRequest); ok {
		req = *casted
	}
	opts := auth.AuthOptions{Create: createFlag(req.Create), Username: req.Username, Vars: req.Vars}
	user, created, err := auth.AuthenticateDevice(r.Context(), s.dbPool, req.ID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{
		AccessToken: accessToken, RefreshToken: refreshToken,
		UserID: user.ID.String(), Username: user.Username, Created: created,
	}
	s.invokeAfter("AuthenticateDevice", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateSteam(w http.ResponseWriter, r *http.Request) {
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authSteamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ticket := req.Ticket
	if ticket == "" {
		ticket = req.Account.Ticket
	}
	providerID, err := auth.VerifySteamTicket(r.Context(), ticket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	opts := auth.AuthOptions{Create: createFlag(req.Create), Username: req.Username, Vars: req.Vars}
	user, created, err := auth.AuthenticateSocialWithOpts(r.Context(), s.dbPool, "steam", providerID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{AccessToken: accessToken, RefreshToken: refreshToken, UserID: user.ID.String(), Username: user.Username, Created: created}
	s.invokeAfter("AuthenticateSteam", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAuthenticateGameCenter(w http.ResponseWriter, r *http.Request) {
	if !s.authRateLimit.Allow(s.clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var req authGameCenterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cred := auth.GameCenterCredentials{
		PlayerID: req.Account.PlayerID, BundleID: req.Account.BundleID,
		Timestamp: req.Account.Timestamp, Salt: req.Account.Salt,
		Signature: req.Account.Signature, PublicKeyURL: req.Account.PublicKeyURL,
	}
	providerID, err := auth.VerifyGameCenterSignature(r.Context(), cred)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	opts := auth.AuthOptions{Create: createFlag(req.Create), Username: req.Username, Vars: req.Vars}
	user, created, err := auth.AuthenticateSocialWithOpts(r.Context(), s.dbPool, "gamecenter", providerID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	accessToken, refreshToken, err := s.tokenMgr.GenerateSessionWithVars(user.ID.String(), user.Username, req.Vars)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.sessReg.RegisterSession(user.ID.String(), refreshToken, "")
	resp := authResponse{AccessToken: accessToken, RefreshToken: refreshToken, UserID: user.ID.String(), Username: user.Username, Created: created}
	s.invokeAfter("AuthenticateGameCenter", &resp, &req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	var req logoutRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	access := req.Token
	if access == "" {
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			access = strings.TrimPrefix(h, "Bearer ")
		}
	}
	userID := ""
	if claims, err := s.tokenMgr.VerifyTokenIgnoreExpiry(access); err == nil && claims != nil {
		userID = claims.UserID
	}
	in, err := s.invokeBefore(r.Context(), "SessionLogout", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*logoutRequest); ok {
		req = *casted
	}
	store := s.sessReg.Store()
	if req.RefreshToken == "" && access != "" && userID != "" {
		_ = auth.LogoutAll(r.Context(), store, s.tokenMgr, userID, access)
	} else {
		_ = auth.Logout(r.Context(), store, s.tokenMgr, access, req.RefreshToken)
	}
	s.invokeAfter("SessionLogout", nil, &req)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		http.Error(w, "invalid user", http.StatusUnauthorized)
		return
	}
	if _, err := s.invokeBefore(r.Context(), "GetAccount", &struct{}{}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	acct, err := auth.GetAccount(r.Context(), s.dbPool, uid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	email := ""
	if acct.Email != nil {
		email = *acct.Email
	}
	custom := ""
	if acct.CustomID != nil {
		custom = *acct.CustomID
	}
	devices := make([]map[string]string, 0, len(acct.Devices))
	for _, d := range acct.Devices {
		devices = append(devices, map[string]string{"id": d})
	}
	lbRecords, _ := leaderboard.RecordsReadAll(r.Context(), s.dbPool, userID)
	walletMap, _ := economy.GetWallet(r.Context(), s.dbPool, userID)
	walletJSON, _ := json.Marshal(walletMap)
	resp := map[string]interface{}{
		"user": map[string]interface{}{
			"id": acct.ID.String(), "username": acct.Username, "display_name": acct.DisplayName,
			"avatar_url": acct.AvatarURL, "lang_tag": acct.LangTag, "location": acct.Location,
			"timezone": acct.Timezone, "metadata": acct.Metadata,
			"create_time": acct.CreateTime, "update_time": acct.UpdateTime,
		},
		"devices":             devices,
		"email":               email,
		"custom_id":           custom,
		"wallet":              string(walletJSON),
		"leaderboard_records": lbRecords,
	}
	s.invokeAfter("GetAccount", resp, nil)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleUpdateAccount(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	uid, _ := uuid.Parse(userID)
	var req accountUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	in, err := s.invokeBefore(r.Context(), "UpdateAccount", &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if casted, ok := in.(*accountUpdateRequest); ok {
		req = *casted
	}
	if err := auth.UpdateAccount(r.Context(), s.dbPool, uid, auth.AccountUpdate{
		Username: req.Username, DisplayName: req.DisplayName, AvatarURL: req.AvatarURL,
		LangTag: req.LangTag, Location: req.Location, Timezone: req.Timezone, Metadata: req.Metadata,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.invokeAfter("UpdateAccount", nil, &req)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	uid, _ := uuid.Parse(userID)
	if _, err := s.invokeBefore(r.Context(), "DeleteAccount", &struct{}{}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := auth.SoftDeleteUser(r.Context(), s.dbPool, uid); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = leaderboard.RecordsDeleteAll(r.Context(), s.dbPool, userID)
	s.sessReg.RevokeAllSessions(userID)
	access := ""
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		access = strings.TrimPrefix(h, "Bearer ")
	}
	_ = auth.LogoutAll(r.Context(), s.sessReg.Store(), s.tokenMgr, userID, access)
	s.invokeAfter("DeleteAccount", nil, nil)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) requireUser(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	userID, err := s.authenticateREST(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return uuid.Nil, false
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		http.Error(w, "invalid user", http.StatusUnauthorized)
		return uuid.Nil, false
	}
	return uid, true
}

func (s *Server) handleLinkEmail(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req authEmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := auth.LinkEmail(r.Context(), s.dbPool, uid, req.Email, req.Password); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleLinkDevice(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req authDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := auth.AddDevice(r.Context(), s.dbPool, uid, req.ID, "{}", nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleLinkCustom(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req authCustomRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := auth.LinkProvider(r.Context(), s.dbPool, uid, "custom", req.CustomID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) linkSocial(w http.ResponseWriter, r *http.Request, provider string, verify func(context.Context, string) (string, error)) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req authSocialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	providerID, err := verify(r.Context(), req.Account.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := auth.LinkProvider(r.Context(), s.dbPool, uid, provider, providerID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleLinkApple(w http.ResponseWriter, r *http.Request) {
	s.linkSocial(w, r, "apple", auth.VerifyAppleToken)
}
func (s *Server) handleLinkGoogle(w http.ResponseWriter, r *http.Request) {
	s.linkSocial(w, r, "google", auth.VerifyGoogleToken)
}
func (s *Server) handleLinkFacebook(w http.ResponseWriter, r *http.Request) {
	s.linkSocial(w, r, "facebook", auth.VerifyFacebookToken)
}
func (s *Server) handleLinkSteam(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req authSteamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ticket := req.Ticket
	if ticket == "" {
		ticket = req.Account.Ticket
	}
	id, err := auth.VerifySteamTicket(r.Context(), ticket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := auth.LinkProvider(r.Context(), s.dbPool, uid, "steam", id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUnlinkProvider(provider string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uid, ok := s.requireUser(w, r)
		if !ok {
			return
		}
		var req unlinkRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		hookName := unlinkHookName(provider)
		if _, err := s.invokeBefore(r.Context(), hookName, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := auth.UnlinkProvider(r.Context(), s.dbPool, uid, provider); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleUnlinkDevice(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var req unlinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := auth.UnlinkDevice(r.Context(), s.dbPool, uid, req.ID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func unlinkHookName(provider string) string {
	switch strings.ToLower(provider) {
	case "email":
		return "UnlinkEmail"
	case "device":
		return "UnlinkDevice"
	case "apple":
		return "UnlinkApple"
	case "google":
		return "UnlinkGoogle"
	case "facebook":
		return "UnlinkFacebook"
	case "steam":
		return "UnlinkSteam"
	case "custom":
		return "UnlinkCustom"
	default:
		return "UnlinkProvider"
	}
}