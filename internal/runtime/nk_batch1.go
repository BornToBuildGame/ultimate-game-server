package runtime

import (
	"context"
	"encoding/json"

	"github.com/dop251/goja"
	lua "github.com/yuin/gopher-lua"
)

// mapLuaNKBatch1 registers Round-6 batch-1 nk bindings (account/users/groups/streams/notifications/LB/tournament/json).
func mapLuaNKBatch1(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
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

// mapJSNKBatch1 registers Round-6 batch-1 nk bindings on the JS nk object.
func mapJSNKBatch1(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
	jsRet := func(v interface{}, err error) goja.Value {
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
