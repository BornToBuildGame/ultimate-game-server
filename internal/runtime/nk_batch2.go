package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"
	lua "github.com/yuin/gopher-lua"
)

// mapLuaNKBatch2a registers Round-7 binding-only nk methods (storage retry + group admin).
func mapLuaNKBatch2a(L *lua.LState, nkTable *lua.LTable, nk RuntimeModule) {
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

// mapJSNKBatch2a registers Round-7 binding-only nk methods on the JS nk object.
func mapJSNKBatch2a(vm *goja.Runtime, nkObj *goja.Object, nk RuntimeModule) {
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
		_ = json.Unmarshal(bytes, &raw)
		return vm.ToValue(raw)
	}

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
