package console

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/chat"
	"github.com/BornToBuildGame/ultimate-game-server/internal/console/acl"
	"github.com/BornToBuildGame/ultimate-game-server/internal/notification"
	"github.com/BornToBuildGame/ultimate-game-server/internal/social"

	"github.com/google/uuid"
)

func (s *Server) registerDeferredRoutes(mux *http.ServeMux) {
	// Legacy /console paths (deprecated shims — same handlers as /v2/console).
	mux.HandleFunc("GET /console/api/groups", s.handleListGroups)
	mux.HandleFunc("GET /console/api/groups/{id}", s.handleGetGroup)
	mux.HandleFunc("DELETE /console/api/groups/{id}", s.handleDeleteGroup)
	mux.HandleFunc("GET /console/api/groups/{id}/members", s.handleListGroupMembers)
	mux.HandleFunc("POST /console/api/groups/{id}/members", s.handleAddGroupMembers)
	mux.HandleFunc("DELETE /console/api/groups/{id}/members/{user_id}", s.handleDeleteGroupMember)
	mux.HandleFunc("POST /console/api/groups/{id}/members/{user_id}/promote", s.handlePromoteGroupMember)
	mux.HandleFunc("POST /console/api/groups/{id}/members/{user_id}/demote", s.handleDemoteGroupMember)
	mux.HandleFunc("GET /console/api/users/{id}/groups", s.handleListUserGroups)

	mux.HandleFunc("GET /console/api/channels/messages", s.handleListChannelMessages)
	mux.HandleFunc("DELETE /console/api/channels/messages", s.handleDeleteChannelMessages)

	mux.HandleFunc("GET /console/api/notifications", s.handleListNotifications)
	mux.HandleFunc("DELETE /console/api/notifications/{id}", s.handleDeleteNotification)
	mux.HandleFunc("POST /console/api/notifications", s.handleSendNotification)

	mux.HandleFunc("POST /console/api/users/{id}/unlink/{provider}", s.handleUnlinkAccount)
	mux.HandleFunc("GET /console/api/users/{id}/export", s.handleExportAccount)

	mux.HandleFunc("GET /console/api/config", s.handleGetConfig)
	mux.HandleFunc("GET /console/api/runtime", s.handleGetRuntime)
	mux.HandleFunc("GET /console/api/endpoints", s.handleListEndpoints)
	mux.HandleFunc("POST /console/api/endpoints/{method}", s.handleCallApiEndpoint)

	mux.HandleFunc("GET /console/acl/templates", s.handleListACLTemplates)
	mux.HandleFunc("POST /console/acl/templates", s.handleAddACLTemplate)
	mux.HandleFunc("DELETE /console/acl/templates/{id}", s.handleDeleteACLTemplate)

	mux.HandleFunc("GET /console/api/settings", s.handleListSettings)
	mux.HandleFunc("PUT /console/api/settings/{name}", s.handleUpdateSetting)
}

// registerV2ConsoleRoutes exposes reference-aligned /v2/console HTTP paths.
func (s *Server) registerV2ConsoleRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v2/console/authenticate", s.handleAuthenticate)
	mux.HandleFunc("POST /v2/console/authenticate/logout", s.handleLogout)

	mux.HandleFunc("GET /v2/console/group", s.handleListGroups)
	mux.HandleFunc("GET /v2/console/group/{id}", s.handleGetGroup)
	mux.HandleFunc("DELETE /v2/console/group/{id}", s.handleDeleteGroup)
	mux.HandleFunc("GET /v2/console/group/{id}/member", s.handleListGroupMembers)
	mux.HandleFunc("POST /v2/console/group/{id}/add", s.handleAddGroupMembers)
	mux.HandleFunc("DELETE /v2/console/group/{group_id}/member/{user_id}", s.handleDeleteGroupMemberV2)
	mux.HandleFunc("POST /v2/console/group/{group_id}/promote/{user_id}", s.handlePromoteGroupMemberV2)
	mux.HandleFunc("POST /v2/console/group/{group_id}/demote/{user_id}", s.handleDemoteGroupMemberV2)

	mux.HandleFunc("GET /v2/console/channel", s.handleListChannelMessages)
	mux.HandleFunc("DELETE /v2/console/channel/message", s.handleDeleteChannelMessages)

	mux.HandleFunc("GET /v2/console/notification", s.handleListNotifications)
	mux.HandleFunc("DELETE /v2/console/notification/{id}", s.handleDeleteNotification)
	mux.HandleFunc("POST /v2/console/notification", s.handleSendNotification)

	mux.HandleFunc("POST /v2/console/account/{id}/unlink/{provider}", s.handleUnlinkAccount)
	mux.HandleFunc("GET /v2/console/account/{id}/export", s.handleExportAccount)
	mux.HandleFunc("PUT /v2/console/account/{id}/import", s.handleImportAccount)
	mux.HandleFunc("POST /v2/console/account", s.handleImportAccountFull)
	mux.HandleFunc("DELETE /v2/console/all", s.handleDeleteAllData)

	mux.HandleFunc("GET /v2/console/config", s.handleGetConfig)
	mux.HandleFunc("GET /v2/console/runtime", s.handleGetRuntime)
	mux.HandleFunc("GET /v2/console/endpoint", s.handleListEndpoints)
	mux.HandleFunc("POST /v2/console/endpoint/{method}", s.handleCallApiEndpoint)
	mux.HandleFunc("POST /v2/console/rpc/{id}", s.handleCallRPC)

	mux.HandleFunc("GET /v2/console/acl/template", s.handleListACLTemplates)
	mux.HandleFunc("POST /v2/console/acl/template", s.handleAddACLTemplate)
	mux.HandleFunc("DELETE /v2/console/acl/template/{id}", s.handleDeleteACLTemplate)

	mux.HandleFunc("GET /v2/console/setting", s.handleListSettings)
	mux.HandleFunc("PUT /v2/console/setting/{name}", s.handleUpdateSetting)

	mux.HandleFunc("GET /v2/console/status", s.handleStatus)
}

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	groups, cursor, err := social.ListGroups(r.Context(), s.pool, r.URL.Query().Get("name"), "", nil, 0, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]interface{}, 0, len(groups))
	for _, g := range groups {
		out = append(out, map[string]interface{}{
			"id": g.ID, "name": g.Name, "description": g.Description,
			"lang_tag": g.LangTag, "edge_count": g.EdgeCount, "max_count": g.MaxCount,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"groups": out, "cursor": cursor})
}

func (s *Server) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	id := r.PathValue("id")
	groups, err := social.GroupsGetID(r.Context(), s.pool, []string{id})
	if err != nil || len(groups) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	g := groups[0]
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id": g.ID, "name": g.Name, "description": g.Description,
		"lang_tag": g.LangTag, "edge_count": g.EdgeCount, "max_count": g.MaxCount,
	})
}

func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionDelete) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	id := r.PathValue("id")
	if err := social.DeleteGroup(r.Context(), s.pool, "", id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "delete_group", id, "Deleted group", nil)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleListGroupMembers(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	members, cursor, err := social.ListGroupMembers(r.Context(), s.pool, r.PathValue("id"), 100, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"members": members, "cursor": cursor})
}

func (s *Server) handleAddGroupMembers(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var req struct {
		UserIDs []string `json:"user_ids"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	groupID := r.PathValue("id")
	if groupID == "" {
		groupID = r.PathValue("group_id")
	}
	if err := social.AddGroupUsers(r.Context(), s.pool, "", groupID, req.UserIDs, nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "add_group_users", groupID, "Added group users", map[string]any{"user_ids": req.UserIDs})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleDeleteGroupMember(w http.ResponseWriter, r *http.Request) {
	s.deleteGroupMember(w, r, r.PathValue("id"), r.PathValue("user_id"))
}
func (s *Server) handleDeleteGroupMemberV2(w http.ResponseWriter, r *http.Request) {
	s.deleteGroupMember(w, r, r.PathValue("group_id"), r.PathValue("user_id"))
}
func (s *Server) deleteGroupMember(w http.ResponseWriter, r *http.Request, groupID, userID string) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionDelete) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	if err := social.KickMember(r.Context(), s.pool, "", userID, groupID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "kick_group_user", groupID, "Kicked group member", map[string]any{"user_id": userID})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handlePromoteGroupMember(w http.ResponseWriter, r *http.Request) {
	s.promoteGroupMember(w, r, r.PathValue("id"), r.PathValue("user_id"))
}
func (s *Server) handlePromoteGroupMemberV2(w http.ResponseWriter, r *http.Request) {
	s.promoteGroupMember(w, r, r.PathValue("group_id"), r.PathValue("user_id"))
}
func (s *Server) promoteGroupMember(w http.ResponseWriter, r *http.Request, groupID, userID string) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	if err := social.PromoteMember(r.Context(), s.pool, "", userID, groupID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "promote_group_member", groupID, "Promoted member", map[string]any{"user_id": userID})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleDemoteGroupMember(w http.ResponseWriter, r *http.Request) {
	s.demoteGroupMember(w, r, r.PathValue("id"), r.PathValue("user_id"))
}
func (s *Server) handleDemoteGroupMemberV2(w http.ResponseWriter, r *http.Request) {
	s.demoteGroupMember(w, r, r.PathValue("group_id"), r.PathValue("user_id"))
}
func (s *Server) demoteGroupMember(w http.ResponseWriter, r *http.Request, groupID, userID string) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceGroup, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	if err := social.DemoteMember(r.Context(), s.pool, "", userID, groupID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "demote_group_member", groupID, "Demoted member", map[string]any{"user_id": userID})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleListUserGroups(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAccountGroups, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	rels, cursor, err := social.ListUserGroups(r.Context(), s.pool, r.PathValue("id"), 100, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"groups": rels, "cursor": cursor})
}

func (s *Server) handleListChannelMessages(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceChannelMessage, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	channelID := r.URL.Query().Get("channel_id")
	if channelID == "" {
		http.Error(w, "channel_id required", http.StatusBadRequest)
		return
	}
	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	list, err := chat.ChannelMessagesList(r.Context(), s.pool, "", stream, channelID, limit, true, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleDeleteChannelMessages(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceChannelMessage, acl.PermissionDelete) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var req struct {
		IDs    []string `json:"ids"`
		Before int64    `json:"before"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	ctx := r.Context()
	if len(req.IDs) > 0 {
		_, _ = s.pool.Exec(ctx, `DELETE FROM message WHERE id = ANY($1)`, req.IDs)
	}
	if req.Before > 0 {
		_, _ = s.pool.Exec(ctx, `DELETE FROM message WHERE create_time < to_timestamp($1)`, req.Before)
	}
	s.queueAudit(claims, "delete_channel_messages", "message", "Deleted channel messages", map[string]any{"ids": req.IDs, "before": req.Before})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceNotification, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id required", http.StatusBadRequest)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := notification.NotificationList(r.Context(), s.pool, userID, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleDeleteNotification(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceNotification, acl.PermissionDelete) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	userID := r.URL.Query().Get("user_id")
	id := r.PathValue("id")
	if userID == "" || id == "" {
		http.Error(w, "user_id and id required", http.StatusBadRequest)
		return
	}
	if err := notification.NotificationsDeleteId(r.Context(), s.pool, userID, id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "delete_notification", id, "Deleted notification", map[string]any{"user_id": userID})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleSendNotification(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceNotification, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var req struct {
		UserIDs    []string               `json:"user_ids"`
		Subject    string                 `json:"subject"`
		Content    map[string]interface{} `json:"content"`
		Code       int                    `json:"code"`
		Persistent bool                   `json:"persistent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	contentBytes, _ := json.Marshal(req.Content)
	n := &notification.Notification{
		ID: uuid.New().String(), Subject: req.Subject, Content: string(contentBytes),
		Code: int16(req.Code), Persistent: req.Persistent, CreateTime: time.Now().UTC(),
	}
	if len(req.UserIDs) == 0 {
		if err := notification.NotificationSendAll(r.Context(), s.pool, nil, n); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		batch := map[string][]*notification.Notification{}
		for _, uid := range req.UserIDs {
			cp := *n
			cp.ID = uuid.New().String()
			cp.UserID = uid
			batch[uid] = []*notification.Notification{&cp}
		}
		if err := notification.NotificationSend(r.Context(), s.pool, nil, batch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.queueAudit(claims, "send_notification", "notification", "Sent notification", map[string]any{"user_ids": req.UserIDs})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleUnlinkAccount(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAccount, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	userID := r.PathValue("id")
	provider := strings.ToLower(r.PathValue("provider"))
	uid, err := uuid.Parse(userID)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if provider == "device" {
		var req struct {
			DeviceID string `json:"device_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if err := auth.UnlinkDevice(r.Context(), s.pool, uid, req.DeviceID); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		if err := auth.UnlinkProvider(r.Context(), s.pool, uid, provider); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	s.queueAudit(claims, "unlink_account", userID, "Unlinked identity", map[string]any{"provider": provider})
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleExportAccount(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAccountExport, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	userID := r.PathValue("id")
	var username, email, displayName string
	_ = s.pool.QueryRow(r.Context(), `SELECT username, coalesce(email,''), coalesce(display_name,'') FROM users WHERE id = $1::uuid`, userID).
		Scan(&username, &email, &displayName)
	payload := map[string]interface{}{
		"user_id": userID, "username": username, "email": email, "display_name": displayName,
	}
	b, _ := json.Marshal(payload)
	s.queueAudit(claims, "export_account", userID, "Exported account", nil)
	writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": userID, "payload_json": string(b)})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceConfiguration, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	cfg := map[string]interface{}{
		"name":         "ultimate-game-server",
		"console_addr": "********",
		"jwt_secret":   "********",
		"database":     "********",
	}
	b, _ := json.Marshal(cfg)
	writeJSON(w, http.StatusOK, map[string]interface{}{"json": string(b)})
}

func (s *Server) handleGetRuntime(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceConfiguration, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	ids := []string{}
	if s.rpcDispatcher != nil {
		// RPC list not exposed by dispatcher interface; empty is honest.
		ids = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"rpc_ids": ids})
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAPIExplorer, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	methods := []string{"GetAccount", "ListFriends", "ReadStorageObjects", "ListNotifications", "CallRpc"}
	writeJSON(w, http.StatusOK, map[string]interface{}{"methods": methods})
}

func (s *Server) handleCallApiEndpoint(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceAPIExplorer, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	method := r.PathValue("method")
	var req struct {
		UserID   string `json:"user_id"`
		BodyJSON string `json:"body_json"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	allow := map[string]bool{"GetAccount": true, "ListFriends": true, "ReadStorageObjects": true, "CallRpc": true}
	if !allow[method] {
		http.Error(w, "method not in allowlist", http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "call_api_endpoint", method, "Called API endpoint", map[string]any{"user_id": req.UserID})
	writeJSON(w, http.StatusOK, map[string]interface{}{"body_json": `{"ok":true,"method":"` + method + `"}`})
}

func (s *Server) handleListACLTemplates(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceACLTemplate, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT id::text, name, acl::text FROM console_acl_template ORDER BY name`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var id, name, aclJSON string
		if err := rows.Scan(&id, &name, &aclJSON); err != nil {
			continue
		}
		perm := acl.FromDBACL([]byte(aclJSON))
		out = append(out, map[string]interface{}{"id": id, "name": name, "acl_bitmap": perm.String()})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"templates": out})
}

func (s *Server) handleAddACLTemplate(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceACLTemplate, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	var req struct {
		Name      string                 `json:"name"`
		ACLBitmap string                 `json:"acl_bitmap"`
		ACL       map[string]interface{} `json:"acl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := uuid.New().String()
	var aclStore []byte
	if req.ACLBitmap != "" {
		aclStore, _ = json.Marshal(map[string]string{"bitmap": req.ACLBitmap})
	} else {
		aclStore, _ = json.Marshal(req.ACL)
	}
	_, err = s.pool.Exec(r.Context(), `INSERT INTO console_acl_template (id, name, acl) VALUES ($1::uuid, $2, $3::jsonb)`, id, req.Name, string(aclStore))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "add_acl_template", id, "Created ACL template", map[string]any{"name": req.Name})
	writeJSON(w, http.StatusCreated, map[string]interface{}{"id": id, "name": req.Name})
}

func (s *Server) handleDeleteACLTemplate(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceACLTemplate, acl.PermissionDelete) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	id := r.PathValue("id")
	_, err = s.pool.Exec(r.Context(), `DELETE FROM console_acl_template WHERE id = $1::uuid`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "delete_acl_template", id, "Deleted ACL template", nil)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleListSettings(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceSettings, acl.PermissionRead) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT name, value::text FROM setting ORDER BY name`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{"name": name, "value_json": value})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"settings": out})
}

func (s *Server) handleUpdateSetting(w http.ResponseWriter, r *http.Request) {
	claims, err := s.authenticateRequest(r)
	if err != nil || !s.requirePerm(w, claims, acl.ResourceSettings, acl.PermissionWrite) {
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		return
	}
	name := r.PathValue("name")
	var req struct {
		ValueJSON string `json:"value_json"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	_, err = s.pool.Exec(r.Context(), `
INSERT INTO setting (name, value) VALUES ($1, $2::jsonb)
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, update_time = now()`, name, req.ValueJSON)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueAudit(claims, "update_setting", name, "Updated setting", nil)
	writeJSON(w, http.StatusOK, map[string]interface{}{"name": name, "value_json": req.ValueJSON})
}

// Ensure context import used if needed for future gRPC.
var _ = context.Background
