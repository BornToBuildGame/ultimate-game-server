package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/social"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	lua "github.com/yuin/gopher-lua"
)

// mapLuaNKExtended registers extended Lua nk bindings beyond the core MapLuaNK surface.
func mapLuaNKExtended(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
	mapLuaNKAccounts(L, nkTable, nk)
	mapLuaNKGroupsStorage(L, nkTable, nk)
	mapLuaNKAuthStreams(L, nkTable, nk)
	mapLuaNKBatch3(L, nkTable, nk)
}

// mapJSNKExtended registers extended JS nk bindings beyond the core MapJSNK surface.
func mapJSNKExtended(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	mapJSNKAccounts(vm, nkObj, nk)
	mapJSNKGroupsStorage(vm, nkObj, nk)
	mapJSNKAuthStreams(vm, nkObj, nk)
	mapJSNKBatch3(vm, nkObj, nk)
}


// jsNKValue marshals a Go value to a JS-friendly goja value, panicking on error.
func jsNKValue(vm *goja.Runtime, v interface{}, err error) goja.Value {
	if err != nil {
		panic(vm.NewGoError(err))
	}
	if v == nil {
		return goja.Null()
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(vm.NewGoError(err))
	}
	var raw interface{}
	if err := json.Unmarshal(bytes, &raw); err != nil {
		panic(vm.NewGoError(err))
	}
	return vm.ToValue(raw)
}

// --- GoRuntimeModule auth / account / group join methods ---

func (m *GoRuntimeModule) AuthenticateDevice(ctx context.Context, id, username string, create bool) (string, string, bool, error) {
	if m.dbPool == nil {
		return "", "", false, fmt.Errorf("database not configured")
	}
	u, created, err := auth.AuthenticateDevice(ctx, m.dbPool, id, auth.AuthOptions{Create: create, Username: username})
	if err != nil {
		return "", "", false, err
	}
	return u.ID.String(), u.Username, created, nil
}

func (m *GoRuntimeModule) AuthenticateCustom(ctx context.Context, id, username string, create bool) (string, string, bool, error) {
	if m.dbPool == nil {
		return "", "", false, fmt.Errorf("database not configured")
	}
	u, created, err := auth.AuthenticateCustomWithOpts(ctx, m.dbPool, id, auth.AuthOptions{Create: create, Username: username})
	if err != nil {
		return "", "", false, err
	}
	return u.ID.String(), u.Username, created, nil
}

func (m *GoRuntimeModule) AuthenticateEmail(ctx context.Context, email, password, username string, create bool) (string, string, bool, error) {
	if m.dbPool == nil {
		return "", "", false, fmt.Errorf("database not configured")
	}
	if create {
		u, err := auth.RegisterEmail(ctx, m.dbPool, username, email, password, username)
		if err != nil {
			return "", "", false, err
		}
		return u.ID.String(), u.Username, true, nil
	}
	u, err := auth.AuthenticateEmail(ctx, m.dbPool, email, password)
	if err != nil {
		return "", "", false, err
	}
	return u.ID.String(), u.Username, false, nil
}

func (m *GoRuntimeModule) AuthenticateTokenGenerate(userID, username string, expiresAt int64, vars map[string]string) (string, int64, error) {
	if m.tokenMgr == nil {
		return "", 0, fmt.Errorf("token manager not configured")
	}
	if userID == "" {
		return "", 0, fmt.Errorf("expects user id")
	}
	if _, err := uuid.Parse(userID); err != nil {
		return "", 0, fmt.Errorf("expects valid user id")
	}
	return m.tokenMgr.GenerateAccessToken(userID, username, expiresAt, vars)
}

func (m *GoRuntimeModule) LinkDevice(ctx context.Context, userID, deviceID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.AddDevice(ctx, m.dbPool, uid, deviceID, "{}", nil)
}

func (m *GoRuntimeModule) LinkCustom(ctx context.Context, userID, customID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.LinkProvider(ctx, m.dbPool, uid, "custom", customID)
}

func (m *GoRuntimeModule) LinkEmail(ctx context.Context, userID, email, password string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.LinkEmail(ctx, m.dbPool, uid, email, password)
}

func (m *GoRuntimeModule) UnlinkDevice(ctx context.Context, userID, deviceID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.UnlinkDevice(ctx, m.dbPool, uid, deviceID)
}

func (m *GoRuntimeModule) UnlinkCustom(ctx context.Context, userID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.UnlinkProvider(ctx, m.dbPool, uid, "custom")
}

func (m *GoRuntimeModule) UnlinkEmail(ctx context.Context, userID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.UnlinkProvider(ctx, m.dbPool, uid, "email")
}

func (m *GoRuntimeModule) AccountUpdateId(ctx context.Context, userID, username string, metadata map[string]interface{}, displayName, timezone, location, langTag, avatarURL string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	upd := auth.AccountUpdate{}
	if username != "" {
		upd.Username = &username
	}
	if displayName != "" {
		upd.DisplayName = &displayName
	}
	if timezone != "" {
		upd.Timezone = &timezone
	}
	if location != "" {
		upd.Location = &location
	}
	if langTag != "" {
		upd.LangTag = &langTag
	}
	if avatarURL != "" {
		upd.AvatarURL = &avatarURL
	}
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("error encoding metadata: %w", err)
		}
		s := string(b)
		upd.Metadata = &s
	}
	return auth.UpdateAccount(ctx, m.dbPool, uid, upd)
}

func (m *GoRuntimeModule) AccountDeleteId(ctx context.Context, userID string) error {
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return fmt.Errorf("expects valid user id")
	}
	return auth.SoftDeleteUser(ctx, m.dbPool, uid)
}

func (m *GoRuntimeModule) SessionLogout(userID, token, refreshToken string) error {
	if m.tokenMgr == nil || m.sessionStore == nil {
		return fmt.Errorf("session store not configured")
	}
	if refreshToken == "" && userID != "" {
		return auth.LogoutAll(context.Background(), m.sessionStore, m.tokenMgr, userID, token)
	}
	return auth.Logout(context.Background(), m.sessionStore, m.tokenMgr, token, refreshToken)
}

func (m *GoRuntimeModule) GroupUserJoin(ctx context.Context, groupID, userID, username string) error {
	_ = username
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	return social.JoinGroup(ctx, m.dbPool, userID, groupID, nil)
}

func (m *GoRuntimeModule) GroupUserLeave(ctx context.Context, groupID, userID, username string) error {
	_ = username
	if m.dbPool == nil {
		return fmt.Errorf("database not configured")
	}
	return social.LeaveGroup(ctx, m.dbPool, userID, groupID)
}

// mapLuaNKAccounts registers account/users/groups/streams/notifications/LB/tournament/json nk bindings.
func mapLuaNKAccounts(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
	L.SetField(nkTable, "account_get_id", L.NewFunction(func(L *lua.LState) int {
		acct, err := nk.AccountGetId(L.Context(), L.CheckString(1))
		if err != nil {
			L.RaiseError("account_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, acct))
		return 1
	}))

	L.SetField(nkTable, "users_get_id", L.NewFunction(func(L *lua.LState) int {
		ids := luaStringSlice(L.CheckTable(1))
		users, err := nk.UsersGetId(L.Context(), ids)
		if err != nil {
			L.RaiseError("users_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, users))
		return 1
	}))

	L.SetField(nkTable, "users_get_username", L.NewFunction(func(L *lua.LState) int {
		names := luaStringSlice(L.CheckTable(1))
		users, err := nk.UsersGetUsername(L.Context(), names)
		if err != nil {
			L.RaiseError("users_get_username failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, users))
		return 1
	}))

	L.SetField(nkTable, "users_get_random", L.NewFunction(func(L *lua.LState) int {
		users, err := nk.UsersGetRandom(L.Context(), L.OptInt(1, 1))
		if err != nil {
			L.RaiseError("users_get_random failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, users))
		return 1
	}))

	L.SetField(nkTable, "users_ban_id", L.NewFunction(func(L *lua.LState) int {
		if err := nk.UsersBanId(L.Context(), luaStringSlice(L.CheckTable(1))); err != nil {
			L.RaiseError("users_ban_id failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "users_unban_id", L.NewFunction(func(L *lua.LState) int {
		if err := nk.UsersUnbanId(L.Context(), luaStringSlice(L.CheckTable(1))); err != nil {
			L.RaiseError("users_unban_id failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "group_update", L.NewFunction(func(L *lua.LState) int {
		err := nk.GroupUpdate(L.Context(),
			L.CheckString(1), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""),
			L.OptString(5, ""), L.OptString(6, ""), L.OptString(7, "{}"), L.OptBool(8, true), L.OptInt(9, 0))
		if err != nil {
			L.RaiseError("group_update failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "group_delete", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupDelete(L.Context(), L.CheckString(1), L.OptString(2, "")); err != nil {
			L.RaiseError("group_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "groups_get_id", L.NewFunction(func(L *lua.LState) int {
		groups, err := nk.GroupsGetId(L.Context(), luaStringSlice(L.CheckTable(1)))
		if err != nil {
			L.RaiseError("groups_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, groups))
		return 1
	}))

	L.SetField(nkTable, "group_users_list", L.NewFunction(func(L *lua.LState) int {
		list, next, err := nk.GroupUsersList(L.Context(), L.CheckString(1), L.OptInt(2, 100), L.OptString(3, ""))
		if err != nil {
			L.RaiseError("group_users_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "group_users_kick", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUsersKick(L.Context(), L.CheckString(1), L.CheckString(2), luaStringSlice(L.CheckTable(3))); err != nil {
			L.RaiseError("group_users_kick failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "stream_user_get", L.NewFunction(func(L *lua.LState) int {
		p, err := nk.StreamUserGet(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), L.CheckString(5), L.OptString(6, ""))
		if err != nil {
			L.RaiseError("stream_user_get failed: %v", err)
			return 0
		}
		if p == nil {
			L.Push(lua.LNil)
			return 1
		}
		L.Push(ToLuaValue(L, p))
		return 1
	}))

	L.SetField(nkTable, "stream_close", L.NewFunction(func(L *lua.LState) int {
		if err := nk.StreamClose(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, "")); err != nil {
			L.RaiseError("stream_close failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "notifications_get_id", L.NewFunction(func(L *lua.LState) int {
		list, err := nk.NotificationsGetId(L.Context(), L.CheckString(1), luaStringSlice(L.CheckTable(2)))
		if err != nil {
			L.RaiseError("notifications_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))

	L.SetField(nkTable, "notifications_update", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(1)
		var updates []*NotificationUpdateParams
		tbl.ForEach(func(_, v lua.LValue) {
			row, ok := v.(*lua.LTable)
			if !ok {
				return
			}
			u := &NotificationUpdateParams{ID: row.RawGetString("id").String()}
			if s := row.RawGetString("subject"); s.Type() != lua.LTNil {
				str := s.String()
				u.Subject = &str
			}
			if s := row.RawGetString("content"); s.Type() != lua.LTNil {
				str := s.String()
				u.Content = &str
			}
			if s := row.RawGetString("sender_id"); s.Type() != lua.LTNil {
				str := s.String()
				u.SenderID = &str
			}
			updates = append(updates, u)
		})
		if err := nk.NotificationsUpdate(L.Context(), updates); err != nil {
			L.RaiseError("notifications_update failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "notifications_delete_id", L.NewFunction(func(L *lua.LState) int {
		if err := nk.NotificationsDeleteId(L.Context(), L.CheckString(1), luaStringSlice(L.CheckTable(2))); err != nil {
			L.RaiseError("notifications_delete_id failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "leaderboards_get_id", L.NewFunction(func(L *lua.LState) int {
		list, err := nk.LeaderboardsGetId(L.Context(), luaStringSlice(L.CheckTable(1)))
		if err != nil {
			L.RaiseError("leaderboards_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))

	L.SetField(nkTable, "tournament_list", L.NewFunction(func(L *lua.LState) int {
		list, next, err := nk.TournamentList(L.Context(),
			L.OptInt(1, 0), L.OptInt(2, 0), L.OptInt64(3, 0), L.OptInt64(4, 0),
			L.OptInt(5, 100), L.OptString(6, ""), L.OptBool(7, false))
		if err != nil {
			L.RaiseError("tournament_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "tournaments_get_id", L.NewFunction(func(L *lua.LState) int {
		list, err := nk.TournamentsGetId(L.Context(), luaStringSlice(L.CheckTable(1)))
		if err != nil {
			L.RaiseError("tournaments_get_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))

	L.SetField(nkTable, "json_encode", L.NewFunction(func(L *lua.LState) int {
		bytes, err := json.Marshal(ToGoValue(L.CheckAny(1)))
		if err != nil {
			L.RaiseError("json_encode failed: %v", err)
			return 0
		}
		L.Push(lua.LString(bytes))
		return 1
	}))

	L.SetField(nkTable, "json_decode", L.NewFunction(func(L *lua.LState) int {
		var raw interface{}
		if err := json.Unmarshal([]byte(L.CheckString(1)), &raw); err != nil {
			L.RaiseError("json_decode failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, raw))
		return 1
	}))
}

// mapJSNKAccounts registers account/users/groups/streams/notifications/LB/tournament/json nk bindings on the JS nk object.
func mapJSNKAccounts(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	jsRet := func(v interface{}, err error) goja.Value { return jsNKValue(vm, v, err) }

	_ = nkObj.Set("account_get_id", func(call goja.FunctionCall) goja.Value {
		acct, err := nk.AccountGetId(context.Background(), call.Argument(0).String())
		return jsRet(acct, err)
	})
	_ = nkObj.Set("accountGetId", nkObj.Get("account_get_id"))

	_ = nkObj.Set("users_get_id", func(call goja.FunctionCall) goja.Value {
		users, err := nk.UsersGetId(context.Background(), jsStringSlice(call.Argument(0).Export()))
		return jsRet(users, err)
	})
	_ = nkObj.Set("usersGetId", nkObj.Get("users_get_id"))

	_ = nkObj.Set("users_get_username", func(call goja.FunctionCall) goja.Value {
		users, err := nk.UsersGetUsername(context.Background(), jsStringSlice(call.Argument(0).Export()))
		return jsRet(users, err)
	})
	_ = nkObj.Set("usersGetUsername", nkObj.Get("users_get_username"))

	_ = nkObj.Set("users_get_random", func(call goja.FunctionCall) goja.Value {
		count := 1
		if !goja.IsUndefined(call.Argument(0)) {
			count = int(call.Argument(0).ToInteger())
		}
		users, err := nk.UsersGetRandom(context.Background(), count)
		return jsRet(users, err)
	})
	_ = nkObj.Set("usersGetRandom", nkObj.Get("users_get_random"))

	_ = nkObj.Set("users_ban_id", func(call goja.FunctionCall) goja.Value {
		if err := nk.UsersBanId(context.Background(), jsStringSlice(call.Argument(0).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("usersBanId", nkObj.Get("users_ban_id"))

	_ = nkObj.Set("users_unban_id", func(call goja.FunctionCall) goja.Value {
		if err := nk.UsersUnbanId(context.Background(), jsStringSlice(call.Argument(0).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("usersUnbanId", nkObj.Get("users_unban_id"))

	_ = nkObj.Set("group_update", func(call goja.FunctionCall) goja.Value {
		err := nk.GroupUpdate(context.Background(),
			call.Argument(0).String(), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)),
			jsOptString(call.Argument(4)), jsOptString(call.Argument(5)), jsOptStringDef(call.Argument(6), "{}"),
			jsOptBool(call.Argument(7), true), int(call.Argument(8).ToInteger()))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUpdate", nkObj.Get("group_update"))

	_ = nkObj.Set("group_delete", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupDelete(context.Background(), call.Argument(0).String(), jsOptString(call.Argument(1))); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupDelete", nkObj.Get("group_delete"))

	_ = nkObj.Set("groups_get_id", func(call goja.FunctionCall) goja.Value {
		groups, err := nk.GroupsGetId(context.Background(), jsStringSlice(call.Argument(0).Export()))
		return jsRet(groups, err)
	})
	_ = nkObj.Set("groupsGetId", nkObj.Get("groups_get_id"))

	_ = nkObj.Set("group_users_list", func(call goja.FunctionCall) goja.Value {
		limit := 100
		if !goja.IsUndefined(call.Argument(1)) {
			limit = int(call.Argument(1).ToInteger())
		}
		list, next, err := nk.GroupUsersList(context.Background(), call.Argument(0).String(), limit, jsOptString(call.Argument(2)))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"users": list, "cursor": next})
	})
	_ = nkObj.Set("groupUsersList", nkObj.Get("group_users_list"))

	_ = nkObj.Set("group_users_kick", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUsersKick(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsStringSlice(call.Argument(2).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUsersKick", nkObj.Get("group_users_kick"))

	_ = nkObj.Set("stream_user_get", func(call goja.FunctionCall) goja.Value {
		p, err := nk.StreamUserGet(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), call.Argument(4).String(), jsOptString(call.Argument(5)))
		return jsRet(p, err)
	})
	_ = nkObj.Set("streamUserGet", nkObj.Get("stream_user_get"))

	_ = nkObj.Set("stream_close", func(call goja.FunctionCall) goja.Value {
		if err := nk.StreamClose(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3))); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamClose", nkObj.Get("stream_close"))

	_ = nkObj.Set("notifications_get_id", func(call goja.FunctionCall) goja.Value {
		list, err := nk.NotificationsGetId(context.Background(), call.Argument(0).String(), jsStringSlice(call.Argument(1).Export()))
		return jsRet(list, err)
	})
	_ = nkObj.Set("notificationsGetId", nkObj.Get("notifications_get_id"))

	_ = nkObj.Set("notifications_update", func(call goja.FunctionCall) goja.Value {
		var raw []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &raw); err != nil {
			panic(vm.NewGoError(err))
		}
		updates := make([]*NotificationUpdateParams, 0, len(raw))
		for _, m := range raw {
			u := &NotificationUpdateParams{ID: getMapString(m, "id")}
			if s, ok := m["subject"].(string); ok {
				u.Subject = &s
			}
			if s, ok := m["content"].(string); ok {
				u.Content = &s
			}
			if s, ok := m["sender_id"].(string); ok {
				u.SenderID = &s
			}
			updates = append(updates, u)
		}
		if err := nk.NotificationsUpdate(context.Background(), updates); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("notificationsUpdate", nkObj.Get("notifications_update"))

	_ = nkObj.Set("notifications_delete_id", func(call goja.FunctionCall) goja.Value {
		if err := nk.NotificationsDeleteId(context.Background(), call.Argument(0).String(), jsStringSlice(call.Argument(1).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("notificationsDeleteId", nkObj.Get("notifications_delete_id"))

	_ = nkObj.Set("leaderboards_get_id", func(call goja.FunctionCall) goja.Value {
		list, err := nk.LeaderboardsGetId(context.Background(), jsStringSlice(call.Argument(0).Export()))
		return jsRet(list, err)
	})
	_ = nkObj.Set("leaderboardsGetId", nkObj.Get("leaderboards_get_id"))

	_ = nkObj.Set("tournament_list", func(call goja.FunctionCall) goja.Value {
		list, next, err := nk.TournamentList(context.Background(),
			int(call.Argument(0).ToInteger()), int(call.Argument(1).ToInteger()),
			call.Argument(2).ToInteger(), call.Argument(3).ToInteger(),
			jsOptInt(call.Argument(4), 100), jsOptString(call.Argument(5)), jsOptBool(call.Argument(6), false))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"tournaments": list, "cursor": next})
	})
	_ = nkObj.Set("tournamentList", nkObj.Get("tournament_list"))

	_ = nkObj.Set("tournaments_get_id", func(call goja.FunctionCall) goja.Value {
		list, err := nk.TournamentsGetId(context.Background(), jsStringSlice(call.Argument(0).Export()))
		return jsRet(list, err)
	})
	_ = nkObj.Set("tournamentsGetId", nkObj.Get("tournaments_get_id"))

	_ = nkObj.Set("json_encode", func(call goja.FunctionCall) goja.Value {
		bytes, err := json.Marshal(call.Argument(0).Export())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(string(bytes))
	})
	_ = nkObj.Set("jsonEncode", nkObj.Get("json_encode"))

	_ = nkObj.Set("json_decode", func(call goja.FunctionCall) goja.Value {
		var raw interface{}
		if err := json.Unmarshal([]byte(call.Argument(0).String()), &raw); err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(raw)
	})
	_ = nkObj.Set("jsonDecode", nkObj.Get("json_decode"))
}

func jsOptStringDef(v goja.Value, def string) string {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return def
	}
	s := v.String()
	if s == "" {
		return def
	}
	return s
}

func jsOptInt(v goja.Value, def int) int {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return def
	}
	return int(v.ToInteger())
}

// mapLuaNKGroupsStorage registers storage_write_retry and group admin nk bindings.
func mapLuaNKGroupsStorage(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
	L.SetField(nkTable, "storage_write_retry", L.NewFunction(func(L *lua.LState) int {
		keysTbl := L.CheckTable(1)
		updateFn := L.CheckFunction(2)
		maxRetries := L.OptInt(3, 5)
		var reads []*StorageRead
		keysTbl.ForEach(func(_, v lua.LValue) {
			row, ok := v.(*lua.LTable)
			if !ok {
				return
			}
			reads = append(reads, &StorageRead{
				Collection: row.RawGetString("collection").String(),
				Key:        row.RawGetString("key").String(),
				UserID:     row.RawGetString("user_id").String(),
			})
		})
		acks, err := nk.StorageWriteRetry(L.Context(), reads, func(objs []*StorageObject) ([]*StorageWrite, error) {
			luaObjs := ToLuaValue(L, objs)
			if err := L.CallByParam(lua.P{Fn: updateFn, NRet: 1, Protect: true}, luaObjs); err != nil {
				return nil, err
			}
			ret := L.Get(-1)
			L.Pop(1)
			if ret.Type() == lua.LTNil {
				return nil, nil
			}
			goVal := ToGoValue(ret)
			bytes, err := json.Marshal(goVal)
			if err != nil {
				return nil, err
			}
			var writes []*StorageWrite
			if err := json.Unmarshal(bytes, &writes); err != nil {
				// Also accept array of maps
				var raw []map[string]interface{}
				if err2 := json.Unmarshal(bytes, &raw); err2 != nil {
					return nil, err
				}
				for _, m := range raw {
					w := &StorageWrite{
						Collection: getMapString(m, "collection"),
						Key:        getMapString(m, "key"),
						UserID:     getMapString(m, "user_id"),
						Value:      getMapString(m, "value"),
						Version:    getMapString(m, "version"),
					}
					if v, ok := m["permission_read"].(float64); ok {
						w.PermissionRead = int32(v)
					}
					if v, ok := m["permission_write"].(float64); ok {
						w.PermissionWrite = int32(v)
					}
					writes = append(writes, w)
				}
			}
			return writes, nil
		}, maxRetries)
		if err != nil {
			L.RaiseError("storage_write_retry failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, acks))
		return 1
	}))

	L.SetField(nkTable, "group_users_add", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUsersAdd(L.Context(), L.CheckString(1), L.CheckString(2), luaStringSlice(L.CheckTable(3))); err != nil {
			L.RaiseError("group_users_add failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "group_users_ban", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUsersBan(L.Context(), L.CheckString(1), L.CheckString(2), luaStringSlice(L.CheckTable(3))); err != nil {
			L.RaiseError("group_users_ban failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "group_users_promote", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUsersPromote(L.Context(), L.CheckString(1), L.CheckString(2), luaStringSlice(L.CheckTable(3))); err != nil {
			L.RaiseError("group_users_promote failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "group_users_demote", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUsersDemote(L.Context(), L.CheckString(1), L.CheckString(2), luaStringSlice(L.CheckTable(3))); err != nil {
			L.RaiseError("group_users_demote failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "groups_get_random", L.NewFunction(func(L *lua.LState) int {
		groups, err := nk.GroupsGetRandom(L.Context(), L.OptInt(1, 1))
		if err != nil {
			L.RaiseError("groups_get_random failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, groups))
		return 1
	}))
}

// mapJSNKGroupsStorage registers storage_write_retry and group admin nk bindings on the JS nk object.
func mapJSNKGroupsStorage(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	jsRet := func(v interface{}, err error) goja.Value { return jsNKValue(vm, v, err) }

	_ = nkObj.Set("storage_write_retry", func(call goja.FunctionCall) goja.Value {
		var rawKeys []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &rawKeys); err != nil {
			panic(vm.NewGoError(err))
		}
		cb, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			panic(vm.NewTypeError("expected update function"))
		}
		maxRetries := 5
		if !goja.IsUndefined(call.Argument(2)) {
			maxRetries = int(call.Argument(2).ToInteger())
		}
		var reads []*StorageRead
		for _, m := range rawKeys {
			reads = append(reads, &StorageRead{
				Collection: getMapString(m, "collection"),
				Key:        getMapString(m, "key"),
				UserID:     getMapString(m, "user_id"),
			})
		}
		acks, err := nk.StorageWriteRetry(context.Background(), reads, func(objs []*StorageObject) ([]*StorageWrite, error) {
			objBytes, _ := json.Marshal(objs)
			var list []interface{}
			_ = json.Unmarshal(objBytes, &list)
			res, err := cb(goja.Undefined(), vm.ToValue(list))
			if err != nil {
				return nil, err
			}
			if goja.IsUndefined(res) || goja.IsNull(res) {
				return nil, nil
			}
			var writes []*StorageWrite
			if err := vm.ExportTo(res, &writes); err != nil {
				var raw []map[string]interface{}
				if err2 := vm.ExportTo(res, &raw); err2 != nil {
					return nil, fmt.Errorf("update fn must return write array: %w", err)
				}
				for _, m := range raw {
					w := &StorageWrite{
						Collection: getMapString(m, "collection"),
						Key:        getMapString(m, "key"),
						UserID:     getMapString(m, "user_id"),
						Value:      getMapString(m, "value"),
						Version:    getMapString(m, "version"),
					}
					if v, ok := m["permission_read"].(float64); ok {
						w.PermissionRead = int32(v)
					} else if v, ok := m["permission_read"].(int64); ok {
						w.PermissionRead = int32(v)
					}
					if v, ok := m["permission_write"].(float64); ok {
						w.PermissionWrite = int32(v)
					} else if v, ok := m["permission_write"].(int64); ok {
						w.PermissionWrite = int32(v)
					}
					writes = append(writes, w)
				}
			}
			return writes, nil
		}, maxRetries)
		return jsRet(acks, err)
	})
	_ = nkObj.Set("storageWriteRetry", nkObj.Get("storage_write_retry"))

	_ = nkObj.Set("group_users_add", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUsersAdd(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsStringSlice(call.Argument(2).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUsersAdd", nkObj.Get("group_users_add"))

	_ = nkObj.Set("group_users_ban", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUsersBan(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsStringSlice(call.Argument(2).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUsersBan", nkObj.Get("group_users_ban"))

	_ = nkObj.Set("group_users_promote", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUsersPromote(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsStringSlice(call.Argument(2).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUsersPromote", nkObj.Get("group_users_promote"))

	_ = nkObj.Set("group_users_demote", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUsersDemote(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsStringSlice(call.Argument(2).Export())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUsersDemote", nkObj.Get("group_users_demote"))

	_ = nkObj.Set("groups_get_random", func(call goja.FunctionCall) goja.Value {
		count := 1
		if !goja.IsUndefined(call.Argument(0)) {
			count = int(call.Argument(0).ToInteger())
		}
		groups, err := nk.GroupsGetRandom(context.Background(), count)
		return jsRet(groups, err)
	})
	_ = nkObj.Set("groupsGetRandom", nkObj.Get("groups_get_random"))
}

// mapLuaNKAuthStreams registers auth/link/account/session/stream helper nk bindings.
func mapLuaNKAuthStreams(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
	pushAuthTriple := func(L *lua.LState, userID, username string, created bool, err error, label string) int {
		if err != nil {
			L.RaiseError("%s failed: %v", label, err)
			return 0
		}
		L.Push(lua.LString(userID))
		L.Push(lua.LString(username))
		L.Push(lua.LBool(created))
		return 3
	}

	L.SetField(nkTable, "authenticate_device", L.NewFunction(func(L *lua.LState) int {
		uid, uname, created, err := nk.AuthenticateDevice(L.Context(), L.CheckString(1), L.OptString(2, ""), L.OptBool(3, true))
		return pushAuthTriple(L, uid, uname, created, err, "authenticate_device")
	}))
	L.SetField(nkTable, "authenticate_custom", L.NewFunction(func(L *lua.LState) int {
		uid, uname, created, err := nk.AuthenticateCustom(L.Context(), L.CheckString(1), L.OptString(2, ""), L.OptBool(3, true))
		return pushAuthTriple(L, uid, uname, created, err, "authenticate_custom")
	}))
	L.SetField(nkTable, "authenticate_email", L.NewFunction(func(L *lua.LState) int {
		uid, uname, created, err := nk.AuthenticateEmail(L.Context(), L.CheckString(1), L.CheckString(2), L.OptString(3, ""), L.OptBool(4, true))
		return pushAuthTriple(L, uid, uname, created, err, "authenticate_email")
	}))
	L.SetField(nkTable, "authenticate_token_generate", L.NewFunction(func(L *lua.LState) int {
		vars := luaStringMap(L.OptTable(4, nil))
		token, exp, err := nk.AuthenticateTokenGenerate(L.CheckString(1), L.OptString(2, ""), int64(L.OptNumber(3, 0)), vars)
		if err != nil {
			L.RaiseError("authenticate_token_generate failed: %v", err)
			return 0
		}
		L.Push(lua.LString(token))
		L.Push(lua.LNumber(exp))
		return 2
	}))

	L.SetField(nkTable, "link_device", L.NewFunction(func(L *lua.LState) int {
		if err := nk.LinkDevice(L.Context(), L.CheckString(1), L.CheckString(2)); err != nil {
			L.RaiseError("link_device failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "link_custom", L.NewFunction(func(L *lua.LState) int {
		if err := nk.LinkCustom(L.Context(), L.CheckString(1), L.CheckString(2)); err != nil {
			L.RaiseError("link_custom failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "link_email", L.NewFunction(func(L *lua.LState) int {
		if err := nk.LinkEmail(L.Context(), L.CheckString(1), L.CheckString(2), L.CheckString(3)); err != nil {
			L.RaiseError("link_email failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "unlink_device", L.NewFunction(func(L *lua.LState) int {
		if err := nk.UnlinkDevice(L.Context(), L.CheckString(1), L.CheckString(2)); err != nil {
			L.RaiseError("unlink_device failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "unlink_custom", L.NewFunction(func(L *lua.LState) int {
		if err := nk.UnlinkCustom(L.Context(), L.CheckString(1)); err != nil {
			L.RaiseError("unlink_custom failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "unlink_email", L.NewFunction(func(L *lua.LState) int {
		if err := nk.UnlinkEmail(L.Context(), L.CheckString(1)); err != nil {
			L.RaiseError("unlink_email failed: %v", err)
		}
		return 0
	}))

	L.SetField(nkTable, "account_update_id", L.NewFunction(func(L *lua.LState) int {
		meta := luaAnyMap(L, L.OptTable(3, nil))
		err := nk.AccountUpdateId(L.Context(), L.CheckString(1), L.OptString(2, ""), meta,
			L.OptString(4, ""), L.OptString(5, ""), L.OptString(6, ""), L.OptString(7, ""), L.OptString(8, ""))
		if err != nil {
			L.RaiseError("account_update_id failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "account_delete_id", L.NewFunction(func(L *lua.LState) int {
		if err := nk.AccountDeleteId(L.Context(), L.CheckString(1)); err != nil {
			L.RaiseError("account_delete_id failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "session_logout", L.NewFunction(func(L *lua.LState) int {
		if err := nk.SessionLogout(L.CheckString(1), L.OptString(2, ""), L.OptString(3, "")); err != nil {
			L.RaiseError("session_logout failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "group_user_join", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUserJoin(L.Context(), L.CheckString(1), L.CheckString(2), L.OptString(3, "")); err != nil {
			L.RaiseError("group_user_join failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "group_user_leave", L.NewFunction(func(L *lua.LState) int {
		if err := nk.GroupUserLeave(L.Context(), L.CheckString(1), L.CheckString(2), L.OptString(3, "")); err != nil {
			L.RaiseError("group_user_leave failed: %v", err)
		}
		return 0
	}))

	// Stream helpers
	L.SetField(nkTable, "stream_user_update", L.NewFunction(func(L *lua.LState) int {
		err := nk.StreamUserUpdate(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""),
			L.CheckString(5), L.CheckString(6), L.OptBool(7, false), L.OptBool(8, false), L.OptString(9, ""))
		if err != nil {
			L.RaiseError("stream_user_update failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "stream_user_kick", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(5)
		p := StreamPresenceView{
			UserID:    tbl.RawGetString("user_id").String(),
			SessionID: tbl.RawGetString("session_id").String(),
			Username:  tbl.RawGetString("username").String(),
		}
		err := nk.StreamUserKick(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), p)
		if err != nil {
			L.RaiseError("stream_user_kick failed: %v", err)
		}
		return 0
	}))
	L.SetField(nkTable, "stream_send_raw", L.NewFunction(func(L *lua.LState) int {
		raw := L.CheckString(5)
		data, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			data = []byte(raw)
		}
		var sessionIDs []string
		if L.GetTop() >= 6 && L.Get(6).Type() == lua.LTTable {
			sessionIDs = luaStringSlice(L.CheckTable(6))
		}
		reliable := L.OptBool(7, true)
		if err := nk.StreamSendRaw(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), data, sessionIDs, reliable); err != nil {
			L.RaiseError("stream_send_raw failed: %v", err)
		}
		return 0
	}))
}

// mapJSNKAuthStreams registers auth/link/account/session/stream helper nk bindings on the JS nk object.
func mapJSNKAuthStreams(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	jsAuthTriple := func(uid, uname string, created bool, err error) goja.Value {
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue([]interface{}{uid, uname, created})
	}

	_ = nkObj.Set("authenticate_device", func(call goja.FunctionCall) goja.Value {
		uid, uname, created, err := nk.AuthenticateDevice(context.Background(), call.Argument(0).String(), jsOptString(call.Argument(1)), jsOptBool(call.Argument(2), true))
		return jsAuthTriple(uid, uname, created, err)
	})
	_ = nkObj.Set("authenticateDevice", nkObj.Get("authenticate_device"))
	_ = nkObj.Set("authenticate_custom", func(call goja.FunctionCall) goja.Value {
		uid, uname, created, err := nk.AuthenticateCustom(context.Background(), call.Argument(0).String(), jsOptString(call.Argument(1)), jsOptBool(call.Argument(2), true))
		return jsAuthTriple(uid, uname, created, err)
	})
	_ = nkObj.Set("authenticateCustom", nkObj.Get("authenticate_custom"))
	_ = nkObj.Set("authenticate_email", func(call goja.FunctionCall) goja.Value {
		uid, uname, created, err := nk.AuthenticateEmail(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsOptString(call.Argument(2)), jsOptBool(call.Argument(3), true))
		return jsAuthTriple(uid, uname, created, err)
	})
	_ = nkObj.Set("authenticateEmail", nkObj.Get("authenticate_email"))
	_ = nkObj.Set("authenticate_token_generate", func(call goja.FunctionCall) goja.Value {
		vars := jsStringMap(call.Argument(3))
		token, exp, err := nk.AuthenticateTokenGenerate(call.Argument(0).String(), jsOptString(call.Argument(1)), int64(call.Argument(2).ToInteger()), vars)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue([]interface{}{token, exp})
	})
	_ = nkObj.Set("authenticateTokenGenerate", nkObj.Get("authenticate_token_generate"))

	_ = nkObj.Set("link_device", func(call goja.FunctionCall) goja.Value {
		if err := nk.LinkDevice(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("linkDevice", nkObj.Get("link_device"))
	_ = nkObj.Set("link_custom", func(call goja.FunctionCall) goja.Value {
		if err := nk.LinkCustom(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("linkCustom", nkObj.Get("link_custom"))
	_ = nkObj.Set("link_email", func(call goja.FunctionCall) goja.Value {
		if err := nk.LinkEmail(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("linkEmail", nkObj.Get("link_email"))
	_ = nkObj.Set("unlink_device", func(call goja.FunctionCall) goja.Value {
		if err := nk.UnlinkDevice(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("unlinkDevice", nkObj.Get("unlink_device"))
	_ = nkObj.Set("unlink_custom", func(call goja.FunctionCall) goja.Value {
		if err := nk.UnlinkCustom(context.Background(), call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("unlinkCustom", nkObj.Get("unlink_custom"))
	_ = nkObj.Set("unlink_email", func(call goja.FunctionCall) goja.Value {
		if err := nk.UnlinkEmail(context.Background(), call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("unlinkEmail", nkObj.Get("unlink_email"))

	_ = nkObj.Set("account_update_id", func(call goja.FunctionCall) goja.Value {
		meta := jsAnyMap(call.Argument(2))
		err := nk.AccountUpdateId(context.Background(), call.Argument(0).String(), jsOptString(call.Argument(1)), meta,
			jsOptString(call.Argument(3)), jsOptString(call.Argument(4)), jsOptString(call.Argument(5)), jsOptString(call.Argument(6)), jsOptString(call.Argument(7)))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("accountUpdateId", nkObj.Get("account_update_id"))
	_ = nkObj.Set("account_delete_id", func(call goja.FunctionCall) goja.Value {
		if err := nk.AccountDeleteId(context.Background(), call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("accountDeleteId", nkObj.Get("account_delete_id"))
	_ = nkObj.Set("session_logout", func(call goja.FunctionCall) goja.Value {
		if err := nk.SessionLogout(call.Argument(0).String(), jsOptString(call.Argument(1)), jsOptString(call.Argument(2))); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("sessionLogout", nkObj.Get("session_logout"))
	_ = nkObj.Set("group_user_join", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUserJoin(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsOptString(call.Argument(2))); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUserJoin", nkObj.Get("group_user_join"))
	_ = nkObj.Set("group_user_leave", func(call goja.FunctionCall) goja.Value {
		if err := nk.GroupUserLeave(context.Background(), call.Argument(0).String(), call.Argument(1).String(), jsOptString(call.Argument(2))); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("groupUserLeave", nkObj.Get("group_user_leave"))

	_ = nkObj.Set("stream_user_update", func(call goja.FunctionCall) goja.Value {
		err := nk.StreamUserUpdate(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)),
			call.Argument(4).String(), call.Argument(5).String(), jsOptBool(call.Argument(6), false), jsOptBool(call.Argument(7), false), jsOptString(call.Argument(8)))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamUserUpdate", nkObj.Get("stream_user_update"))
	_ = nkObj.Set("stream_user_kick", func(call goja.FunctionCall) goja.Value {
		var p StreamPresenceView
		if obj := call.Argument(4).Export(); obj != nil {
			b, _ := json.Marshal(obj)
			_ = json.Unmarshal(b, &p)
		}
		if err := nk.StreamUserKick(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), p); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamUserKick", nkObj.Get("stream_user_kick"))
	_ = nkObj.Set("stream_send_raw", func(call goja.FunctionCall) goja.Value {
		raw := call.Argument(4).String()
		data, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			data = []byte(raw)
		}
		var sessionIDs []string
		if !goja.IsUndefined(call.Argument(5)) && !goja.IsNull(call.Argument(5)) {
			if arr, ok := call.Argument(5).Export().([]interface{}); ok {
				for _, v := range arr {
					sessionIDs = append(sessionIDs, fmt.Sprint(v))
				}
			}
		}
		if err := nk.StreamSendRaw(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), data, sessionIDs, jsOptBool(call.Argument(6), true)); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamSendRaw", nkObj.Get("stream_send_raw"))
}

func luaAnyMap(L *lua.LState, tbl *lua.LTable) map[string]interface{} {
	_ = L
	if tbl == nil {
		return nil
	}
	goVal := ToGoValue(tbl)
	if m, ok := goVal.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func jsStringMap(v goja.Value) map[string]string {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}
	exported := v.Export()
	out := make(map[string]string)
	switch m := exported.(type) {
	case map[string]interface{}:
		for k, val := range m {
			out[k] = fmt.Sprint(val)
		}
	case map[string]string:
		return m
	}
	return out
}

func jsAnyMap(v goja.Value) map[string]interface{} {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}
	if m, ok := v.Export().(map[string]interface{}); ok {
		return m
	}
	b, err := json.Marshal(v.Export())
	if err != nil {
		return nil
	}
	var out map[string]interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

func mapLuaNKBatch3(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
	L.SetField(nkTable, "http_request", L.NewFunction(func(L *lua.LState) int {
		urlStr := L.CheckString(1)
		method := L.OptString(2, "GET")
		var headers map[string]string
		if tbl := L.OptTable(3, nil); tbl != nil {
			headers = make(map[string]string)
			tbl.ForEach(func(k, v lua.LValue) {
				headers[k.String()] = v.String()
			})
		}
		body := L.OptString(4, "")
		timeoutMs := L.OptInt(5, 5000)

		code, respHeaders, respBody, err := nk.HttpRequest(L.Context(), urlStr, method, headers, body, timeoutMs)
		if err != nil {
			L.RaiseError("http_request failed: %v", err)
			return 0
		}

		hdrTbl := L.NewTable()
		for k, v := range respHeaders {
			hdrTbl.RawSetString(k, lua.LString(v))
		}
		L.Push(lua.LNumber(code))
		L.Push(hdrTbl)
		L.Push(lua.LString(respBody))
		return 3
	}))

	L.SetField(nkTable, "sql_exec", L.NewFunction(func(L *lua.LState) int {
		query := L.CheckString(1)
		var args []interface{}
		if tbl := L.OptTable(2, nil); tbl != nil {
			tbl.ForEach(func(k, v lua.LValue) {
				args = append(args, ToGoValue(v))
			})
		}
		ct, err := nk.SqlExec(L.Context(), query, args)
		if err != nil {
			L.RaiseError("sql_exec failed: %v", err)
			return 0
		}
		L.Push(lua.LNumber(ct))
		return 1
	}))

	L.SetField(nkTable, "sql_query", L.NewFunction(func(L *lua.LState) int {
		query := L.CheckString(1)
		var args []interface{}
		if tbl := L.OptTable(2, nil); tbl != nil {
			tbl.ForEach(func(k, v lua.LValue) {
				args = append(args, ToGoValue(v))
			})
		}
		rows, err := nk.SqlQuery(L.Context(), query, args)
		if err != nil {
			L.RaiseError("sql_query failed: %v", err)
			return 0
		}
		resTbl := L.NewTable()
		for _, row := range rows {
			rowTbl := L.NewTable()
			for k, v := range row {
				rowTbl.RawSetString(k, ToLuaValue(L, v))
			}
			resTbl.Append(rowTbl)
		}
		L.Push(resTbl)
		return 1
	}))

	L.SetField(nkTable, "localcache_get", L.NewFunction(func(L *lua.LState) int {
		val, ok := nk.LocalCacheGet(L.CheckString(1))
		if !ok {
			L.Push(lua.LNil)
			L.Push(lua.LBool(false))
			return 2
		}
		L.Push(ToLuaValue(L, val))
		L.Push(lua.LBool(true))
		return 2
	}))

	L.SetField(nkTable, "localcache_set", L.NewFunction(func(L *lua.LState) int {
		key := L.CheckString(1)
		val := ToGoValue(L.Get(2))
		ttlSec := int64(L.OptInt(3, 0))
		nk.LocalCacheSet(key, val, ttlSec)
		return 0
	}))

	L.SetField(nkTable, "crypto_hash", L.NewFunction(func(L *lua.LState) int {
		res, err := nk.CryptoHash(L.CheckString(1), L.CheckString(2))
		if err != nil {
			L.RaiseError("crypto_hash failed: %v", err)
			return 0
		}
		L.Push(lua.LString(res))
		return 1
	}))

	L.SetField(nkTable, "crypto_hmac_hash", L.NewFunction(func(L *lua.LState) int {
		res, err := nk.CryptoHmacHash(L.CheckString(1), L.CheckString(2), L.CheckString(3))
		if err != nil {
			L.RaiseError("crypto_hmac_hash failed: %v", err)
			return 0
		}
		L.Push(lua.LString(res))
		return 1
	}))

	L.SetField(nkTable, "bcrypt_hash", L.NewFunction(func(L *lua.LState) int {
		res, err := nk.BcryptHash(L.CheckString(1))
		if err != nil {
			L.RaiseError("bcrypt_hash failed: %v", err)
			return 0
		}
		L.Push(lua.LString(res))
		return 1
	}))

	L.SetField(nkTable, "bcrypt_compare", L.NewFunction(func(L *lua.LState) int {
		res := nk.BcryptCompare(L.CheckString(1), L.CheckString(2))
		L.Push(lua.LBool(res))
		return 1
	}))

	L.SetField(nkTable, "uuid_v4", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(nk.UuidV4()))
		return 1
	}))
}

func mapJSNKBatch3(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	_ = nkObj.Set("http_request", func(call goja.FunctionCall) goja.Value {
		urlStr := call.Argument(0).String()
		method := jsOptString(call.Argument(1))
		headers := jsStringMap(call.Argument(2))
		body := jsOptString(call.Argument(3))
		timeoutMs := int(call.Argument(4).ToInteger())
		code, respHeaders, respBody, err := nk.HttpRequest(context.Background(), urlStr, method, headers, body, timeoutMs)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		res := map[string]interface{}{
			"code":    code,
			"headers": respHeaders,
			"body":    respBody,
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("httpRequest", nkObj.Get("http_request"))

	_ = nkObj.Set("sql_exec", func(call goja.FunctionCall) goja.Value {
		query := call.Argument(0).String()
		var args []interface{}
		if arr, ok := call.Argument(1).Export().([]interface{}); ok {
			args = arr
		}
		ct, err := nk.SqlExec(context.Background(), query, args)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(ct)
	})
	_ = nkObj.Set("sqlExec", nkObj.Get("sql_exec"))

	_ = nkObj.Set("sql_query", func(call goja.FunctionCall) goja.Value {
		query := call.Argument(0).String()
		var args []interface{}
		if arr, ok := call.Argument(1).Export().([]interface{}); ok {
			args = arr
		}
		rows, err := nk.SqlQuery(context.Background(), query, args)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(rows)
	})
	_ = nkObj.Set("sqlQuery", nkObj.Get("sql_query"))

	_ = nkObj.Set("localcache_get", func(call goja.FunctionCall) goja.Value {
		val, ok := nk.LocalCacheGet(call.Argument(0).String())
		if !ok {
			return goja.Null()
		}
		return vm.ToValue(val)
	})
	_ = nkObj.Set("localCacheGet", nkObj.Get("localcache_get"))

	_ = nkObj.Set("localcache_set", func(call goja.FunctionCall) goja.Value {
		key := call.Argument(0).String()
		val := call.Argument(1).Export()
		ttlSec := call.Argument(2).ToInteger()
		nk.LocalCacheSet(key, val, ttlSec)
		return goja.Undefined()
	})
	_ = nkObj.Set("localCacheSet", nkObj.Get("localcache_set"))

	_ = nkObj.Set("crypto_hash", func(call goja.FunctionCall) goja.Value {
		res, err := nk.CryptoHash(call.Argument(0).String(), call.Argument(1).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("cryptoHash", nkObj.Get("crypto_hash"))

	_ = nkObj.Set("crypto_hmac_hash", func(call goja.FunctionCall) goja.Value {
		res, err := nk.CryptoHmacHash(call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("cryptoHmacHash", nkObj.Get("crypto_hmac_hash"))

	_ = nkObj.Set("bcrypt_hash", func(call goja.FunctionCall) goja.Value {
		res, err := nk.BcryptHash(call.Argument(0).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("bcryptHash", nkObj.Get("bcrypt_hash"))

	_ = nkObj.Set("bcrypt_compare", func(call goja.FunctionCall) goja.Value {
		res := nk.BcryptCompare(call.Argument(0).String(), call.Argument(1).String())
		return vm.ToValue(res)
	})
	_ = nkObj.Set("bcryptCompare", nkObj.Get("bcrypt_compare"))

	_ = nkObj.Set("uuid_v4", func(call goja.FunctionCall) goja.Value {
		return vm.ToValue(nk.UuidV4())
	})
	_ = nkObj.Set("uuidV4", nkObj.Get("uuid_v4"))
}

