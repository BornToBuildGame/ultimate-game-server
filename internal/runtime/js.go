package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dop251/goja"
)

// MapJSNK binds the core storage APIs to a Goja JS VM.
func MapJSNK(vm *goja.Runtime, nk RuntimeModule, timeout time.Duration) {
	// Set up background watchdog interrupt timer
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			vm.Interrupt("execution timeout exceeded")
		})
	}

	// Defer cleanup helper
	vm.Set("nk_cleanup", func() {
		if timer != nil {
			timer.Stop()
		}
	})

	nkObj := vm.NewObject()

	// 1. Storage Read
	_ = nkObj.Set("storage_read", func(call goja.FunctionCall) goja.Value {
		var rawArr []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &rawArr); err != nil {
			panic(vm.NewGoError(err))
		}
		var reads []*StorageRead
		for _, m := range rawArr {
			reads = append(reads, &StorageRead{
				Collection: getMapString(m, "collection"),
				Key:        getMapString(m, "key"),
				UserID:     getMapString(m, "user_id"),
			})
		}

		objs, err := nk.StorageRead(context.Background(), reads)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		objsBytes, _ := json.Marshal(objs)
		var list []interface{}
		_ = json.Unmarshal(objsBytes, &list)
		return vm.ToValue(list)
	})

	// 2. Storage Write
	_ = nkObj.Set("storage_write", func(call goja.FunctionCall) goja.Value {
		var rawArr []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &rawArr); err != nil {
			panic(vm.NewGoError(err))
		}
		var writes []*StorageWrite
		for _, m := range rawArr {
			writes = append(writes, &StorageWrite{
				Collection:      getMapString(m, "collection"),
				Key:             getMapString(m, "key"),
				UserID:          getMapString(m, "user_id"),
				Value:           getMapString(m, "value"),
				Version:         getMapString(m, "version"),
				PermissionRead:  getMapInt32(m, "permission_read"),
				PermissionWrite: getMapInt32(m, "permission_write"),
			})
		}

		acks, err := nk.StorageWrite(context.Background(), writes)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		acksBytes, _ := json.Marshal(acks)
		var list []interface{}
		_ = json.Unmarshal(acksBytes, &list)
		return vm.ToValue(list)
	})

	// 3. Storage Delete
	_ = nkObj.Set("storage_delete", func(call goja.FunctionCall) goja.Value {
		var rawArr []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &rawArr); err != nil {
			panic(vm.NewGoError(err))
		}
		var deletes []*StorageDelete
		for _, m := range rawArr {
			deletes = append(deletes, &StorageDelete{
				Collection: getMapString(m, "collection"),
				Key:        getMapString(m, "key"),
				UserID:     getMapString(m, "user_id"),
				Version:    getMapString(m, "version"),
			})
		}

		err := nk.StorageDelete(context.Background(), deletes)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		return goja.Undefined()
	})

	// 4. Wallet Update
	_ = nkObj.Set("wallet_update", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()

		var changeset map[string]int64
		if changesetVal := call.Argument(1).Export(); changesetVal != nil {
			if m, ok := changesetVal.(map[string]interface{}); ok {
				changeset = make(map[string]int64)
				for k, v := range m {
					switch val := v.(type) {
					case float64:
						changeset[k] = int64(val)
					case int64:
						changeset[k] = val
					case int:
						changeset[k] = int64(val)
					}
				}
			}
		}

		var metadata map[string]interface{}
		if metadataVal := call.Argument(2).Export(); metadataVal != nil {
			if m, ok := metadataVal.(map[string]interface{}); ok {
				metadata = m
			}
		}

		newWallet, err := nk.WalletUpdate(context.Background(), userID, changeset, metadata, true)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		return vm.ToValue(newWallet)
	})

	// 5. Leaderboard Record Write
	_ = nkObj.Set("leaderboard_record_write", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		ownerID := call.Argument(1).String()
		username := call.Argument(2).String()
		score := call.Argument(3).ToInteger()
		subscore := call.Argument(4).ToInteger()

		var metadata map[string]interface{}
		if metadataVal := call.Argument(5).Export(); metadataVal != nil {
			if m, ok := metadataVal.(map[string]interface{}); ok {
				metadata = m
			}
		}

		rec, err := nk.LeaderboardRecordWrite(context.Background(), id, ownerID, username, score, subscore, metadata)
		if err != nil {
			panic(vm.NewGoError(err))
		}

		bytes, _ := json.Marshal(rec)
		var m map[string]interface{}
		_ = json.Unmarshal(bytes, &m)
		return vm.ToValue(m)
	})

	// 6. Notification Send
	_ = nkObj.Set("notification_send", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		subject := call.Argument(1).String()

		var content map[string]interface{}
		if contentVal := call.Argument(2).Export(); contentVal != nil {
			if m, ok := contentVal.(map[string]interface{}); ok {
				content = m
			}
		}

		code := call.Argument(3).ToInteger()
		senderID := call.Argument(4).String()
		persistent := true
		if call.Argument(5).Export() != nil {
			persistent = call.Argument(5).ToBoolean()
		}

		err := nk.NotificationSend(context.Background(), userID, subject, content, int(code), senderID, persistent)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	// 7. Match Create
	_ = nkObj.Set("match_create", func(call goja.FunctionCall) goja.Value {
		module := call.Argument(0).String()

		var params map[string]interface{}
		if paramsVal := call.Argument(1).Export(); paramsVal != nil {
			if m, ok := paramsVal.(map[string]interface{}); ok {
				params = m
			}
		}

		matchID, err := nk.MatchCreate(context.Background(), module, params)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(matchID)
	})

	// 8. Leaderboard Create
	_ = nkObj.Set("leaderboard_create", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		authoritative := call.Argument(1).ToBoolean()
		sortOrder := call.Argument(2).ToInteger()
		operator := call.Argument(3).ToInteger()
		resetSchedule := call.Argument(4).String()
		
		var metadata map[string]interface{}
		if metadataVal := call.Argument(5).Export(); metadataVal != nil {
			if m, ok := metadataVal.(map[string]interface{}); ok {
				metadata = m
			}
		}

		enableRanks := true
		if call.Argument(6).Export() != nil {
			enableRanks = call.Argument(6).ToBoolean()
		}

		err := nk.LeaderboardCreate(context.Background(), id, authoritative, int(sortOrder), int(operator), resetSchedule, metadata, enableRanks)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	// 9. Leaderboard Delete
	_ = nkObj.Set("leaderboard_delete", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		err := nk.LeaderboardDelete(context.Background(), id)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	// 10. Tournament Create
	_ = nkObj.Set("tournament_create", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		authoritative := call.Argument(1).ToBoolean()
		sortOrder := call.Argument(2).ToInteger()
		operator := call.Argument(3).ToInteger()
		resetSchedule := call.Argument(4).String()

		var metadata map[string]interface{}
		if metadataVal := call.Argument(5).Export(); metadataVal != nil {
			if m, ok := metadataVal.(map[string]interface{}); ok {
				metadata = m
			}
		}

		title := call.Argument(6).String()
		description := call.Argument(7).String()
		category := call.Argument(8).ToInteger()
		startTime := call.Argument(9).ToInteger()
		endTime := call.Argument(10).ToInteger()
		duration := call.Argument(11).ToInteger()
		maxSize := call.Argument(12).ToInteger()
		maxNumScore := call.Argument(13).ToInteger()

		joinRequired := false
		if call.Argument(14).Export() != nil {
			joinRequired = call.Argument(14).ToBoolean()
		}

		enableRanks := true
		if call.Argument(15).Export() != nil {
			enableRanks = call.Argument(15).ToBoolean()
		}

		err := nk.TournamentCreate(context.Background(), id, authoritative, int(sortOrder), int(operator), resetSchedule, metadata, title, description, int(category), startTime, endTime, int(duration), int(maxSize), int(maxNumScore), joinRequired, enableRanks)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	// 11. Tournament Delete
	_ = nkObj.Set("tournament_delete", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		err := nk.TournamentDelete(context.Background(), id)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	// 12. Tournament Join
	_ = nkObj.Set("tournament_join", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		ownerID := call.Argument(1).String()
		username := call.Argument(2).String()
		err := nk.TournamentJoin(context.Background(), id, ownerID, username)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = vm.Set("nk", nkObj)
}

func getMapString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func getMapInt32(m map[string]interface{}, key string) int32 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case int64:
			return int32(v)
		case float64:
			return int32(v)
		case int:
			return int32(v)
		}
	}
	return 0
}

// ExecuteJSRPC runs a JS RPC function.
func ExecuteJSRPC(vm *goja.Runtime, funcName string, ctx context.Context, payload string) (string, error) {
	defer func() {
		if cleanup, ok := goja.AssertFunction(vm.Get("nk_cleanup")); ok {
			_, _ = cleanup(goja.Undefined())
		}
	}()

	fnVal := vm.Get(funcName)
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return "", fmt.Errorf("javascript function %q not found", funcName)
	}

	// Build Context Object
	ctxMap := make(map[string]interface{})
	if userID := ctx.Value("user_id"); userID != nil {
		ctxMap["user_id"] = userID.(string)
	}
	ctxVal := vm.ToValue(ctxMap)

	resVal, err := fn(goja.Undefined(), ctxVal, vm.ToValue(payload))
	if err != nil {
		return "", err
	}

	return resVal.String(), nil
}

// ExecuteJSBeforeHook runs a JS before hook.
func ExecuteJSBeforeHook(vm *goja.Runtime, funcName string, ctx context.Context, in interface{}) (interface{}, error) {
	defer func() {
		if cleanup, ok := goja.AssertFunction(vm.Get("nk_cleanup")); ok {
			_, _ = cleanup(goja.Undefined())
		}
	}()

	fnVal := vm.Get(funcName)
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return nil, fmt.Errorf("javascript function %q not found", funcName)
	}

	// Translate input request to JS object
	jsonBytes, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var rawMap interface{}
	if err := json.Unmarshal(jsonBytes, &rawMap); err != nil {
		return nil, err
	}
	jsIn := vm.ToValue(rawMap)

	ctxMap := make(map[string]interface{})
	if userID := ctx.Value("user_id"); userID != nil {
		ctxMap["user_id"] = userID.(string)
	}
	ctxVal := vm.ToValue(ctxMap)

	resVal, err := fn(goja.Undefined(), ctxVal, jsIn)
	if err != nil {
		return nil, err
	}

	if resVal == nil || goja.IsNull(resVal) || goja.IsUndefined(resVal) {
		return nil, fmt.Errorf("before hook rejected request")
	}

	// Translate modified JS object back to Go request struct
	goMap := resVal.Export()
	goBytes, err := json.Marshal(goMap)
	if err != nil {
		return nil, err
	}

	outPtr := in
	if err := json.Unmarshal(goBytes, outPtr); err != nil {
		return nil, err
	}
	return outPtr, nil
}

// ExecuteJSAfterHook runs a JS after hook.
func ExecuteJSAfterHook(vm *goja.Runtime, funcName string, ctx context.Context, out interface{}, in interface{}) error {
	defer func() {
		if cleanup, ok := goja.AssertFunction(vm.Get("nk_cleanup")); ok {
			_, _ = cleanup(goja.Undefined())
		}
	}()

	fnVal := vm.Get(funcName)
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return fmt.Errorf("javascript function %q not found", funcName)
	}

	outBytes, _ := json.Marshal(out)
	var outMap interface{}
	_ = json.Unmarshal(outBytes, &outMap)
	jsOut := vm.ToValue(outMap)

	inBytes, _ := json.Marshal(in)
	var inMap interface{}
	_ = json.Unmarshal(inBytes, &inMap)
	jsIn := vm.ToValue(inMap)

	ctxMap := make(map[string]interface{})
	if userID := ctx.Value("user_id"); userID != nil {
		ctxMap["user_id"] = userID.(string)
	}
	ctxVal := vm.ToValue(ctxMap)

	_, err := fn(goja.Undefined(), ctxVal, jsOut, jsIn)
	return err
}
