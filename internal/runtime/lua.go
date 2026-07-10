package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yuin/gopher-lua"
)

// MapLuaNK binds the core storage APIs to a Gopher-Lua VM.
func MapLuaNK(L *lua.LState, nk RuntimeModule) {
	nkTable := L.NewTable()

	// 1. Storage Read
	L.SetField(nkTable, "storage_read", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(1)
		var reads []*StorageRead

		tbl.ForEach(func(k, v lua.LValue) {
			if row, ok := v.(*lua.LTable); ok {
				reads = append(reads, &StorageRead{
					Collection: row.RawGetString("collection").String(),
					Key:        row.RawGetString("key").String(),
					UserID:     row.RawGetString("user_id").String(),
				})
			}
		})

		objs, err := nk.StorageRead(L.Context(), reads)
		if err != nil {
			L.RaiseError("storage_read failed: %v", err)
			return 0
		}

		resTable := L.NewTable()
		for _, o := range objs {
			oTbl := L.NewTable()
			L.SetField(oTbl, "collection", lua.LString(o.Collection))
			L.SetField(oTbl, "key", lua.LString(o.Key))
			L.SetField(oTbl, "user_id", lua.LString(o.UserID))
			L.SetField(oTbl, "value", lua.LString(o.Value))
			L.SetField(oTbl, "version", lua.LString(o.Version))
			L.SetField(oTbl, "permission_read", lua.LNumber(o.PermissionRead))
			L.SetField(oTbl, "permission_write", lua.LNumber(o.PermissionWrite))
			resTable.Append(oTbl)
		}
		L.Push(resTable)
		return 1
	}))

	// 2. Storage Write
	L.SetField(nkTable, "storage_write", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(1)
		var writes []*StorageWrite

		tbl.ForEach(func(k, v lua.LValue) {
			if row, ok := v.(*lua.LTable); ok {
				writes = append(writes, &StorageWrite{
					Collection:      row.RawGetString("collection").String(),
					Key:             row.RawGetString("key").String(),
					UserID:          row.RawGetString("user_id").String(),
					Value:           row.RawGetString("value").String(),
					Version:         row.RawGetString("version").String(),
					PermissionRead:  int32(row.RawGetString("permission_read").(lua.LNumber)),
					PermissionWrite: int32(row.RawGetString("permission_write").(lua.LNumber)),
				})
			}
		})

		acks, err := nk.StorageWrite(L.Context(), writes)
		if err != nil {
			L.RaiseError("storage_write failed: %v", err)
			return 0
		}

		resTable := L.NewTable()
		for _, a := range acks {
			aTbl := L.NewTable()
			L.SetField(aTbl, "collection", lua.LString(a.Collection))
			L.SetField(aTbl, "key", lua.LString(a.Key))
			L.SetField(aTbl, "user_id", lua.LString(a.UserID))
			L.SetField(aTbl, "version", lua.LString(a.Version))
			resTable.Append(aTbl)
		}
		L.Push(resTable)
		return 1
	}))

	// 3. Storage Delete
	L.SetField(nkTable, "storage_delete", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(1)
		var deletes []*StorageDelete

		tbl.ForEach(func(k, v lua.LValue) {
			if row, ok := v.(*lua.LTable); ok {
				deletes = append(deletes, &StorageDelete{
					Collection: row.RawGetString("collection").String(),
					Key:        row.RawGetString("key").String(),
					UserID:     row.RawGetString("user_id").String(),
					Version:    row.RawGetString("version").String(),
				})
			}
		})

		err := nk.StorageDelete(L.Context(), deletes)
		if err != nil {
			L.RaiseError("storage_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	// 4. Wallet Update
	L.SetField(nkTable, "wallet_update", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		changesetTbl := L.CheckTable(2)
		metadataTbl := L.OptTable(3, nil)

		changesetVal := ToGoValue(changesetTbl)
		var changeset map[string]int64
		if m, ok := changesetVal.(map[string]interface{}); ok {
			changeset = make(map[string]int64)
			for k, v := range m {
				if f, ok := v.(float64); ok {
					changeset[k] = int64(f)
				}
			}
		}

		var metadata map[string]interface{}
		if metadataTbl != nil {
			if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}

		newWallet, err := nk.WalletUpdate(L.Context(), userID, changeset, metadata, true)
		if err != nil {
			L.RaiseError("wallet_update failed: %v", err)
			return 0
		}

		L.Push(ToLuaValue(L, newWallet))
		return 1
	}))

	// 5. Leaderboard Record Write
	L.SetField(nkTable, "leaderboard_record_write", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		ownerID := L.CheckString(2)
		username := L.CheckString(3)
		score := L.CheckInt64(4)
		subscore := L.OptInt64(5, 0)
		metadataTbl := L.OptTable(6, nil)

		var metadata map[string]interface{}
		if metadataTbl != nil {
			if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}

		rec, err := nk.LeaderboardRecordWrite(L.Context(), id, ownerID, username, score, subscore, metadata)
		if err != nil {
			L.RaiseError("leaderboard_record_write failed: %v", err)
			return 0
		}

		resTbl := L.NewTable()
		L.SetField(resTbl, "leaderboard_id", lua.LString(rec.LeaderboardID))
		L.SetField(resTbl, "owner_id", lua.LString(rec.OwnerID))
		L.SetField(resTbl, "username", lua.LString(rec.Username))
		L.SetField(resTbl, "score", lua.LNumber(rec.Score))
		L.SetField(resTbl, "subscore", lua.LNumber(rec.Subscore))
		L.SetField(resTbl, "num_score", lua.LNumber(rec.NumScore))
		L.SetField(resTbl, "max_num_score", lua.LNumber(rec.MaxNumScore))
		L.SetField(resTbl, "metadata", lua.LString(rec.Metadata))
		L.SetField(resTbl, "rank", lua.LNumber(rec.Rank))
		L.Push(resTbl)
		return 1
	}))

	// 6. Notification Send
	L.SetField(nkTable, "notification_send", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		subject := L.CheckString(2)
		contentTbl := L.CheckTable(3)
		code := L.CheckInt(4)
		senderID := L.OptString(5, "")
		persistent := L.OptBool(6, true)

		var content map[string]interface{}
		if m, ok := ToGoValue(contentTbl).(map[string]interface{}); ok {
			content = m
		}

		err := nk.NotificationSend(L.Context(), userID, subject, content, code, senderID, persistent)
		if err != nil {
			L.RaiseError("notification_send failed: %v", err)
			return 0
		}
		return 0
	}))

	// 7. Match Create
	L.SetField(nkTable, "match_create", L.NewFunction(func(L *lua.LState) int {
		module := L.CheckString(1)
		paramsTbl := L.OptTable(2, nil)

		var params map[string]interface{}
		if paramsTbl != nil {
			if m, ok := ToGoValue(paramsTbl).(map[string]interface{}); ok {
				params = m
			}
		}

		matchID, err := nk.MatchCreate(L.Context(), module, params)
		if err != nil {
			L.RaiseError("match_create failed: %v", err)
			return 0
		}
		L.Push(lua.LString(matchID))
		return 1
	}))

	// 8. Leaderboard Create
	L.SetField(nkTable, "leaderboard_create", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		authoritative := L.CheckBool(2)
		sortOrder := L.CheckInt(3)
		operator := L.CheckInt(4)
		resetSchedule := L.OptString(5, "")
		metadataTbl := L.OptTable(6, nil)
		enableRanks := L.OptBool(7, true)

		var metadata map[string]interface{}
		if metadataTbl != nil {
			if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}

		err := nk.LeaderboardCreate(L.Context(), id, authoritative, sortOrder, operator, resetSchedule, metadata, enableRanks)
		if err != nil {
			L.RaiseError("leaderboard_create failed: %v", err)
			return 0
		}
		return 0
	}))

	// 9. Leaderboard Delete
	L.SetField(nkTable, "leaderboard_delete", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		err := nk.LeaderboardDelete(L.Context(), id)
		if err != nil {
			L.RaiseError("leaderboard_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	// 10. Tournament Create
	L.SetField(nkTable, "tournament_create", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		authoritative := L.CheckBool(2)
		sortOrder := L.CheckInt(3)
		operator := L.CheckInt(4)
		resetSchedule := L.OptString(5, "")
		metadataTbl := L.OptTable(6, nil)
		title := L.OptString(7, "")
		description := L.OptString(8, "")
		category := L.OptInt(9, 0)
		startTime := L.OptInt64(10, 0)
		endTime := L.OptInt64(11, 0)
		duration := L.OptInt(12, 0)
		maxSize := L.OptInt(13, 0)
		maxNumScore := L.OptInt(14, 0)
		joinRequired := L.OptBool(15, false)
		enableRanks := L.OptBool(16, true)

		var metadata map[string]interface{}
		if metadataTbl != nil {
			if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}

		err := nk.TournamentCreate(L.Context(), id, authoritative, sortOrder, operator, resetSchedule, metadata, title, description, category, startTime, endTime, duration, maxSize, maxNumScore, joinRequired, enableRanks)
		if err != nil {
			L.RaiseError("tournament_create failed: %v", err)
			return 0
		}
		return 0
	}))

	// 11. Tournament Delete
	L.SetField(nkTable, "tournament_delete", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		err := nk.TournamentDelete(L.Context(), id)
		if err != nil {
			L.RaiseError("tournament_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	// 12. Tournament Join
	L.SetField(nkTable, "tournament_join", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		ownerID := L.CheckString(2)
		username := L.CheckString(3)
		err := nk.TournamentJoin(L.Context(), id, ownerID, username)
		if err != nil {
			L.RaiseError("tournament_join failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetGlobal("nk", nkTable)
}

// ExecuteLuaRPC runs a Lua RPC function.
func ExecuteLuaRPC(L *lua.LState, funcName string, ctx context.Context, payload string) (string, error) {
	L.SetContext(ctx)
	fn := L.GetGlobal(funcName)
	if fn.Type() != lua.LTFunction {
		return "", fmt.Errorf("lua function %q not found", funcName)
	}

	// Build Context Table
	ctxTbl := L.NewTable()
	if userID := ctx.Value("user_id"); userID != nil {
		L.SetField(ctxTbl, "user_id", lua.LString(userID.(string)))
	}

	err := L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, ctxTbl, lua.LString(payload))
	if err != nil {
		return "", err
	}

	retVal := L.Get(-1)
	L.Pop(1)
	return retVal.String(), nil
}

// ExecuteLuaBeforeHook runs a Lua before hook.
func ExecuteLuaBeforeHook(L *lua.LState, funcName string, ctx context.Context, in interface{}) (interface{}, error) {
	L.SetContext(ctx)
	fn := L.GetGlobal(funcName)
	if fn.Type() != lua.LTFunction {
		return nil, fmt.Errorf("lua function %q not found", funcName)
	}

	// Translate request struct to Lua Table
	jsonBytes, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var rawMap interface{}
	if err := json.Unmarshal(jsonBytes, &rawMap); err != nil {
		return nil, err
	}
	luaVal := ToLuaValue(L, rawMap)

	ctxTbl := L.NewTable()
	if userID := ctx.Value("user_id"); userID != nil {
		L.SetField(ctxTbl, "user_id", lua.LString(userID.(string)))
	}

	err = L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, ctxTbl, luaVal)
	if err != nil {
		return nil, err
	}

	retVal := L.Get(-1)
	L.Pop(1)

	// If hook returns nil, reject or abort execution
	if retVal == lua.LNil {
		return nil, fmt.Errorf("before hook rejected request")
	}

	// Translate modified Lua Table back to Go struct
	goVal := ToGoValue(retVal)
	goBytes, err := json.Marshal(goVal)
	if err != nil {
		return nil, err
	}

	// Create a new instance of the same type as in
	outPtr := in
	if err := json.Unmarshal(goBytes, outPtr); err != nil {
		return nil, err
	}
	return outPtr, nil
}

// ExecuteLuaAfterHook runs a Lua after hook.
func ExecuteLuaAfterHook(L *lua.LState, funcName string, ctx context.Context, out interface{}, in interface{}) error {
	L.SetContext(ctx)
	fn := L.GetGlobal(funcName)
	if fn.Type() != lua.LTFunction {
		return fmt.Errorf("lua function %q not found", funcName)
	}

	outBytes, _ := json.Marshal(out)
	var outMap interface{}
	_ = json.Unmarshal(outBytes, &outMap)
	luaOut := ToLuaValue(L, outMap)

	inBytes, _ := json.Marshal(in)
	var inMap interface{}
	_ = json.Unmarshal(inBytes, &inMap)
	luaIn := ToLuaValue(L, inMap)

	ctxTbl := L.NewTable()
	if userID := ctx.Value("user_id"); userID != nil {
		L.SetField(ctxTbl, "user_id", lua.LString(userID.(string)))
	}

	err := L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, ctxTbl, luaOut, luaIn)
	return err
}

// ToLuaValue converts Go primitives/slices/maps to Lua values.
func ToLuaValue(L *lua.LState, val interface{}) lua.LValue {
	switch v := val.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(v)
	case float64:
		return lua.LNumber(v)
	case string:
		return lua.LString(v)
	case []interface{}:
		tbl := L.NewTable()
		for _, item := range v {
			tbl.Append(ToLuaValue(L, item))
		}
		return tbl
	case map[string]interface{}:
		tbl := L.NewTable()
		for k, item := range v {
			L.SetField(tbl, k, ToLuaValue(L, item))
		}
		return tbl
	default:
		// Try generic JSON conversion for structs, custom maps, etc.
		if bytes, err := json.Marshal(v); err == nil {
			var raw interface{}
			if err := json.Unmarshal(bytes, &raw); err == nil {
				return ToLuaValue(L, raw)
			}
		}
		return lua.LString(fmt.Sprintf("%v", v))
	}
}

// ToGoValue converts Lua values to Go primitives/slices/maps.
func ToGoValue(val lua.LValue) interface{} {
	switch v := val.(type) {
	case lua.LBool:
		return bool(v)
	case lua.LNumber:
		return float64(v)
	case lua.LString:
		return string(v)
	case *lua.LTable:
		isArr := true
		maxKey := 0
		v.ForEach(func(k, val lua.LValue) {
			if numKey, ok := k.(lua.LNumber); ok {
				if float64(numKey) == float64(int(numKey)) && int(numKey) > 0 {
					if int(numKey) > maxKey {
						maxKey = int(numKey)
					}
				} else {
					isArr = false
				}
			} else {
				isArr = false
			}
		})
		if isArr && maxKey > 0 {
			arr := make([]interface{}, maxKey)
			for i := 1; i <= maxKey; i++ {
				arr[i-1] = ToGoValue(v.RawGetInt(i))
			}
			return arr
		} else {
			m := make(map[string]interface{})
			v.ForEach(func(k, val lua.LValue) {
				m[k.String()] = ToGoValue(val)
			})
			return m
		}
	default:
		return nil
	}
}
