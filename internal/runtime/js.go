package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ultimate-game-server/internal/satori"
	"ultimate-game-server/internal/storage"

	"github.com/dop251/goja"
)

// MapJSNK binds the core storage APIs to a Goja JS VM.
func MapJSNK(vm *goja.Runtime, nk RuntimeModule, timeout time.Duration, registry ...*HookRegistry) {
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

	_ = nkObj.Set("storage_list", func(call goja.FunctionCall) goja.Value {
		callerID := call.Argument(0).String()
		userID := call.Argument(1).String()
		collection := call.Argument(2).String()
		limit := int(call.Argument(3).ToInteger())
		if limit == 0 {
			limit = 100
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(4)) {
			cursor = call.Argument(4).String()
		}
		list, next, err := nk.StorageList(context.Background(), callerID, userID, collection, limit, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"objects": list, "cursor": next})
	})

	// 4. Wallet Update
	walletUpdateFn := func(call goja.FunctionCall) goja.Value {
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
		updateLedger := true
		if !goja.IsUndefined(call.Argument(3)) && !goja.IsNull(call.Argument(3)) {
			updateLedger = call.Argument(3).ToBoolean()
		}

		updated, previous, err := nk.WalletUpdate(context.Background(), userID, changeset, metadata, updateLedger)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		_ = previous
		return vm.ToValue(updated)
	}
	_ = nkObj.Set("wallet_update", walletUpdateFn)
	_ = nkObj.Set("walletUpdate", walletUpdateFn)

	_ = nkObj.Set("wallets_update", func(call goja.FunctionCall) goja.Value {
		updateLedger := true
		if !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			updateLedger = call.Argument(1).ToBoolean()
		}
		var updates []*WalletUpdateParams
		if raw := call.Argument(0).Export(); raw != nil {
			if arr, ok := raw.([]interface{}); ok {
				for _, item := range arr {
					m, ok := item.(map[string]interface{})
					if !ok {
						continue
					}
					u := &WalletUpdateParams{UserID: fmt.Sprint(m["user_id"])}
					if cs, ok := m["changeset"].(map[string]interface{}); ok {
						u.Changeset = make(map[string]int64)
						for k, v := range cs {
							if f, ok := v.(float64); ok {
								u.Changeset[k] = int64(f)
							}
						}
					}
					if meta, ok := m["metadata"].(map[string]interface{}); ok {
						u.Metadata = meta
					}
					updates = append(updates, u)
				}
			}
		}
		results, err := nk.WalletsUpdate(context.Background(), updates, updateLedger)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(results)
	})
	_ = nkObj.Set("walletsUpdate", nkObj.Get("wallets_update"))

	multiUpdateFn := func(call goja.FunctionCall) goja.Value {
		accounts := parseJSAccountUpdates(call.Argument(0).Export())
		writes := parseJSStorageWrites(call.Argument(1).Export())
		deletes := parseJSStorageDeletes(call.Argument(2).Export())
		wallets := parseJSWalletUpdates(call.Argument(3).Export())
		updateLedger := true
		if !goja.IsUndefined(call.Argument(4)) && !goja.IsNull(call.Argument(4)) {
			updateLedger = call.Argument(4).ToBoolean()
		}
		acks, results, err := nk.MultiUpdate(context.Background(), accounts, writes, deletes, wallets, updateLedger)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"acks": acks, "wallets": results})
	}
	_ = nkObj.Set("multi_update", multiUpdateFn)
	_ = nkObj.Set("multiUpdate", multiUpdateFn)

	storageIndexListFn := func(call goja.FunctionCall) goja.Value {
		callerID := ""
		if !goja.IsUndefined(call.Argument(0)) && !goja.IsNull(call.Argument(0)) {
			callerID = call.Argument(0).String()
		}
		indexName := call.Argument(1).String()
		query := "*"
		if !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
			query = call.Argument(2).String()
		}
		limit := 10
		if !goja.IsUndefined(call.Argument(3)) && !goja.IsNull(call.Argument(3)) {
			limit = int(call.Argument(3).ToInteger())
		}
		var order []string
		if raw := call.Argument(4).Export(); raw != nil {
			if arr, ok := raw.([]interface{}); ok {
				for _, v := range arr {
					order = append(order, fmt.Sprint(v))
				}
			}
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(5)) && !goja.IsNull(call.Argument(5)) {
			cursor = call.Argument(5).String()
		}
		objs, next, err := nk.StorageIndexList(context.Background(), callerID, indexName, query, limit, order, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"objects": objs, "cursor": next})
	}
	_ = nkObj.Set("storage_index_list", storageIndexListFn)
	_ = nkObj.Set("storageIndexList", storageIndexListFn)

	if grm, ok := nk.(*GoRuntimeModule); ok {
		registerStorageIndexFn := func(call goja.FunctionCall) goja.Value {
			name := call.Argument(0).String()
			collection := call.Argument(1).String()
			key := ""
			if !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
				key = call.Argument(2).String()
			}
			fields := jsStringSlice(call.Argument(3).Export())
			sortable := jsStringSlice(call.Argument(4).Export())
			maxEntries := 1000
			if !goja.IsUndefined(call.Argument(5)) && !goja.IsNull(call.Argument(5)) {
				maxEntries = int(call.Argument(5).ToInteger())
			}
			indexOnly := false
			if !goja.IsUndefined(call.Argument(6)) && !goja.IsNull(call.Argument(6)) {
				indexOnly = call.Argument(6).ToBoolean()
			}
			if err := grm.RegisterStorageIndex(name, collection, key, fields, sortable, maxEntries, indexOnly); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		}
		_ = nkObj.Set("register_storage_index", registerStorageIndexFn)
		_ = nkObj.Set("registerStorageIndex", registerStorageIndexFn)

		registerStorageIndexFilterFn := func(call goja.FunctionCall) goja.Value {
			indexName := call.Argument(0).String()
			fn, ok := goja.AssertFunction(call.Argument(1))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register_storage_index_filter requires a function")))
			}
			if err := grm.RegisterStorageIndexFilter(indexName, func(ctx context.Context, write *storage.StorageObject) (bool, error) {
				ret, err := fn(goja.Undefined(), vm.ToValue(write))
				if err != nil {
					return false, err
				}
				if goja.IsUndefined(ret) || goja.IsNull(ret) {
					return true, nil
				}
				return ret.ToBoolean(), nil
			}); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		}
		_ = nkObj.Set("register_storage_index_filter", registerStorageIndexFilterFn)
		_ = nkObj.Set("registerStorageIndexFilter", registerStorageIndexFilterFn)
	}

	_ = nkObj.Set("wallet_ledger_list", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(2)) {
			cursor = call.Argument(2).String()
		}
		items, next, err := nk.WalletLedgerList(context.Background(), userID, limit, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"items": items, "cursor": next})
	})
	_ = nkObj.Set("walletLedgerList", nkObj.Get("wallet_ledger_list"))

	_ = nkObj.Set("wallet_ledger_update", func(call goja.FunctionCall) goja.Value {
		ledgerID := call.Argument(0).String()
		userID := call.Argument(1).String()
		var metadata map[string]interface{}
		if m, ok := call.Argument(2).Export().(map[string]interface{}); ok {
			metadata = m
		}
		if err := nk.WalletLedgerUpdate(context.Background(), ledgerID, userID, metadata); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("walletLedgerUpdate", nkObj.Get("wallet_ledger_update"))

	_ = nkObj.Set("purchase_validate_apple", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(2)) {
			persist = call.Argument(2).ToBoolean()
		}
		vp, err := nk.PurchaseValidateApple(context.Background(), call.Argument(0).String(), call.Argument(1).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(vp)
	})
	_ = nkObj.Set("purchase_validate_google", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(3)) {
			persist = call.Argument(3).ToBoolean()
		}
		vp, err := nk.PurchaseValidateGoogle(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(vp)
	})
	_ = nkObj.Set("purchase_validate_huawei", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(3)) {
			persist = call.Argument(3).ToBoolean()
		}
		vp, err := nk.PurchaseValidateHuawei(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(vp)
	})
	_ = nkObj.Set("purchase_validate_facebook_instant", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(2)) {
			persist = call.Argument(2).ToBoolean()
		}
		vp, err := nk.PurchaseValidateFacebookInstant(context.Background(), call.Argument(0).String(), call.Argument(1).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(vp)
	})
	_ = nkObj.Set("purchase_validate_samsung", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(2)) {
			persist = call.Argument(2).ToBoolean()
		}
		vp, err := nk.PurchaseValidateSamsung(context.Background(), call.Argument(0).String(), call.Argument(1).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(vp)
	})
	_ = nkObj.Set("purchases_list", func(call goja.FunctionCall) goja.Value {
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		list, err := nk.PurchasesList(context.Background(), call.Argument(0).String(), limit)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(list)
	})
	_ = nkObj.Set("subscription_validate_apple", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(2)) {
			persist = call.Argument(2).ToBoolean()
		}
		sub, err := nk.SubscriptionValidateApple(context.Background(), call.Argument(0).String(), call.Argument(1).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(sub)
	})
	_ = nkObj.Set("subscription_validate_google", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(3)) {
			persist = call.Argument(3).ToBoolean()
		}
		sub, err := nk.SubscriptionValidateGoogle(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(sub)
	})
	_ = nkObj.Set("subscriptions_list", func(call goja.FunctionCall) goja.Value {
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		list, err := nk.SubscriptionsList(context.Background(), call.Argument(0).String(), limit)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(list)
	})
	_ = nkObj.Set("subscription_get_product_id", func(call goja.FunctionCall) goja.Value {
		sub, err := nk.SubscriptionGetProductID(context.Background(), call.Argument(0).String(), call.Argument(1).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(sub)
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

	// 6. Notification APIs
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
		persistent := false
		if !goja.IsUndefined(call.Argument(5)) && !goja.IsNull(call.Argument(5)) {
			persistent = call.Argument(5).ToBoolean()
		}

		err := nk.NotificationSend(context.Background(), userID, subject, content, int(code), senderID, persistent)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("notifications_list", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(2)) {
			cursor = call.Argument(2).String()
		}
		list, next, err := nk.NotificationsList(context.Background(), userID, limit, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, n := range list {
			rows[i] = map[string]interface{}{
				"id":         n.ID,
				"subject":    n.Subject,
				"content":    n.Content,
				"code":       n.Code,
				"sender_id":  n.SenderID,
				"persistent": n.Persistent,
			}
		}
		return vm.ToValue(map[string]interface{}{"notifications": rows, "cacheable_cursor": next})
	})

	_ = nkObj.Set("notifications_delete", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		var ids []string
		if err := vm.ExportTo(call.Argument(1), &ids); err != nil {
			panic(vm.NewGoError(err))
		}
		if err := nk.NotificationsDelete(context.Background(), userID, ids); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("notification_send_all", func(call goja.FunctionCall) goja.Value {
		subject := call.Argument(0).String()
		var content map[string]interface{}
		if contentVal := call.Argument(1).Export(); contentVal != nil {
			if m, ok := contentVal.(map[string]interface{}); ok {
				content = m
			}
		}
		code := int(call.Argument(2).ToInteger())
		persistent := false
		if !goja.IsUndefined(call.Argument(3)) && !goja.IsNull(call.Argument(3)) {
			persistent = call.Argument(3).ToBoolean()
		}
		if err := nk.NotificationSendAll(context.Background(), subject, content, code, persistent); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("notifications_send", func(call goja.FunctionCall) goja.Value {
		var raw []map[string]interface{}
		if err := vm.ExportTo(call.Argument(0), &raw); err != nil {
			panic(vm.NewGoError(err))
		}
		params := make([]*NotificationSendParams, 0, len(raw))
		for _, m := range raw {
			p := &NotificationSendParams{
				UserID:   getMapString(m, "user_id"),
				Subject:  getMapString(m, "subject"),
				SenderID: getMapString(m, "sender_id"),
				Code:     int(getMapInt32(m, "code")),
			}
			if b, ok := m["persistent"].(bool); ok {
				p.Persistent = b
			}
			if c, ok := m["content"].(map[string]interface{}); ok {
				p.Content = c
			}
			params = append(params, p)
		}
		if err := nk.NotificationsSend(context.Background(), params); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("friends_list", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		var statePtr *int
		if !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
			st := int(call.Argument(2).ToInteger())
			statePtr = &st
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(3)) {
			cursor = call.Argument(3).String()
		}
		list, next, err := nk.FriendsList(context.Background(), userID, limit, statePtr, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, f := range list {
			rows[i] = map[string]interface{}{"user_id": f.UserID, "username": f.Username, "state": f.State}
		}
		return vm.ToValue(map[string]interface{}{"friends": rows, "cursor": next})
	})

	_ = nkObj.Set("party_list", func(call goja.FunctionCall) goja.Value {
		limit := int(call.Argument(0).ToInteger())
		if limit == 0 {
			limit = 10
		}
		var openPtr *bool
		if !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			b := call.Argument(1).ToBoolean()
			openPtr = &b
		}
		hidden := false
		if !goja.IsUndefined(call.Argument(2)) {
			hidden = call.Argument(2).ToBoolean()
		}
		query := ""
		if !goja.IsUndefined(call.Argument(3)) {
			query = call.Argument(3).String()
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(4)) {
			cursor = call.Argument(4).String()
		}
		list, next, err := nk.PartyList(context.Background(), limit, openPtr, hidden, query, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, p := range list {
			rows[i] = map[string]interface{}{
				"id": p.ID, "open": p.Open, "hidden": p.Hidden, "max_size": p.MaxSize, "label": p.Label,
			}
		}
		return vm.ToValue(map[string]interface{}{"parties": rows, "cursor": next})
	})

	_ = nkObj.Set("channelIdBuild", func(call goja.FunctionCall) goja.Value {
		id, err := nk.ChannelIdBuild(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger()))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(id)
	})

	_ = nkObj.Set("channelMessageSend", func(call goja.FunctionCall) goja.Value {
		content, _ := call.Argument(1).Export().(map[string]interface{})
		persist := true
		if !goja.IsUndefined(call.Argument(4)) {
			persist = call.Argument(4).ToBoolean()
		}
		ack, err := nk.ChannelMessageSend(context.Background(), call.Argument(0).String(), content, call.Argument(2).String(), call.Argument(3).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"channel_id": ack.ChannelID, "message_id": ack.MessageID, "code": ack.Code, "persistent": ack.Persistent})
	})

	_ = nkObj.Set("channelMessageUpdate", func(call goja.FunctionCall) goja.Value {
		content, _ := call.Argument(2).Export().(map[string]interface{})
		persist := true
		if !goja.IsUndefined(call.Argument(5)) {
			persist = call.Argument(5).ToBoolean()
		}
		ack, err := nk.ChannelMessageUpdate(context.Background(), call.Argument(0).String(), call.Argument(1).String(), content, call.Argument(3).String(), call.Argument(4).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"message_id": ack.MessageID, "code": ack.Code})
	})

	_ = nkObj.Set("channelMessageRemove", func(call goja.FunctionCall) goja.Value {
		persist := true
		if !goja.IsUndefined(call.Argument(4)) {
			persist = call.Argument(4).ToBoolean()
		}
		ack, err := nk.ChannelMessageRemove(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String(), call.Argument(3).String(), persist)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"message_id": ack.MessageID, "code": ack.Code})
	})

	_ = nkObj.Set("channelMessagesList", func(call goja.FunctionCall) goja.Value {
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 20
		}
		forward := true
		if !goja.IsUndefined(call.Argument(2)) {
			forward = call.Argument(2).ToBoolean()
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(3)) {
			cursor = call.Argument(3).String()
		}
		list, next, prev, cacheable, err := nk.ChannelMessagesList(context.Background(), call.Argument(0).String(), limit, forward, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, m := range list {
			rows[i] = map[string]interface{}{
				"channel_id": m.ChannelID, "message_id": m.MessageID, "code": m.Code,
				"sender_id": m.SenderID, "username": m.Username, "content": m.Content, "persistent": m.Persistent,
			}
		}
		return vm.ToValue(map[string]interface{}{"messages": rows, "next_cursor": next, "prev_cursor": prev, "cacheable_cursor": cacheable})
	})

	_ = nkObj.Set("group_create", func(call goja.FunctionCall) goja.Value {
		g, err := nk.GroupCreate(context.Background(),
			call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).String(),
			call.Argument(3).String(), call.Argument(4).String(), call.Argument(5).String(),
			call.Argument(6).ToBoolean(), int(call.Argument(7).ToInteger()))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"id": g.ID, "name": g.Name, "open": g.Open})
	})

	_ = nkObj.Set("groups_list", func(call goja.FunctionCall) goja.Value {
		var openPtr *bool
		if !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
			b := call.Argument(2).ToBoolean()
			openPtr = &b
		}
		list, next, err := nk.GroupsList(context.Background(), call.Argument(0).String(), call.Argument(1).String(), openPtr,
			int(call.Argument(3).ToInteger()), int(call.Argument(4).ToInteger()), call.Argument(5).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, g := range list {
			rows[i] = map[string]interface{}{"id": g.ID, "name": g.Name, "open": g.Open}
		}
		return vm.ToValue(map[string]interface{}{"groups": rows, "cursor": next})
	})

	_ = nkObj.Set("friends_add", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		usernames := jsStringSlice(call.Argument(2).Export())
		if err := nk.FriendsAdd(context.Background(), userID, ids, usernames, nil); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("friends_delete", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		usernames := jsStringSlice(call.Argument(2).Export())
		if err := nk.FriendsDelete(context.Background(), userID, ids, usernames); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("friends_block", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		usernames := jsStringSlice(call.Argument(2).Export())
		if err := nk.FriendsBlock(context.Background(), userID, ids, usernames); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("friends_of_friends_list", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		limit := int(call.Argument(1).ToInteger())
		if limit == 0 {
			limit = 100
		}
		cursor := ""
		if !goja.IsUndefined(call.Argument(2)) {
			cursor = call.Argument(2).String()
		}
		list, next, err := nk.FriendsOfFriendsList(context.Background(), userID, limit, cursor)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		rows := make([]map[string]interface{}, len(list))
		for i, f := range list {
			rows[i] = map[string]interface{}{"referrer": f.Referrer, "user_id": f.UserID, "username": f.Username}
		}
		return vm.ToValue(map[string]interface{}{"friendsOfFriends": rows, "cursor": next})
	})

	_ = nkObj.Set("friend_metadata_update", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		friendID := call.Argument(1).String()
		var metadata map[string]any
		if m, ok := call.Argument(2).Export().(map[string]interface{}); ok {
			metadata = m
		}
		if err := nk.FriendMetadataUpdate(context.Background(), userID, friendID, metadata); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("users_get_friend_status", func(call goja.FunctionCall) goja.Value {
		userID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		statusMap, err := nk.UsersGetFriendStatus(context.Background(), userID, ids)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(statusMap)
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
	_ = nkObj.Set("matchCreate", nkObj.Get("match_create"))

	_ = nkObj.Set("match_list", func(call goja.FunctionCall) goja.Value {
		limit := int(call.Argument(0).ToInteger())
		if limit == 0 {
			limit = 100
		}
		authoritative := call.Argument(1).ToBoolean()
		label := ""
		if !goja.IsUndefined(call.Argument(2)) {
			label = call.Argument(2).String()
		}
		minSize := int(call.Argument(3).ToInteger())
		maxSize := int(call.Argument(4).ToInteger())
		list, err := nk.MatchList(context.Background(), limit, authoritative, label, minSize, maxSize)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(list)
	})
	_ = nkObj.Set("matchList", nkObj.Get("match_list"))
	_ = nkObj.Set("match_get", func(call goja.FunctionCall) goja.Value {
		info, err := nk.MatchGet(context.Background(), call.Argument(0).String())
		if err != nil {
			return goja.Null()
		}
		return vm.ToValue(info)
	})
	_ = nkObj.Set("matchGet", nkObj.Get("match_get"))
	_ = nkObj.Set("match_signal", func(call goja.FunctionCall) goja.Value {
		res, err := nk.MatchSignal(context.Background(), call.Argument(0).String(), call.Argument(1).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("matchSignal", nkObj.Get("match_signal"))

	_ = nkObj.Set("status_follow", func(call goja.FunctionCall) goja.Value {
		sessionID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		if err := nk.StatusFollow(sessionID, ids); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("statusFollow", nkObj.Get("status_follow"))
	_ = nkObj.Set("status_unfollow", func(call goja.FunctionCall) goja.Value {
		sessionID := call.Argument(0).String()
		ids := jsStringSlice(call.Argument(1).Export())
		if err := nk.StatusUnfollow(sessionID, ids); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("statusUnfollow", nkObj.Get("status_unfollow"))

	_ = nkObj.Set("stream_user_list", func(call goja.FunctionCall) goja.Value {
		mode := int16(call.Argument(0).ToInteger())
		list, err := nk.StreamUserList(mode, jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), jsOptBool(call.Argument(4), true), jsOptBool(call.Argument(5), true))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(list)
	})
	_ = nkObj.Set("streamUserList", nkObj.Get("stream_user_list"))
	_ = nkObj.Set("stream_user_join", func(call goja.FunctionCall) goja.Value {
		ok, err := nk.StreamUserJoin(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), call.Argument(4).String(), call.Argument(5).String(), jsOptBool(call.Argument(6), false), jsOptBool(call.Argument(7), false), jsOptString(call.Argument(8)))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(ok)
	})
	_ = nkObj.Set("streamUserJoin", nkObj.Get("stream_user_join"))
	_ = nkObj.Set("stream_user_leave", func(call goja.FunctionCall) goja.Value {
		if err := nk.StreamUserLeave(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), jsOptString(call.Argument(4)), call.Argument(5).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamUserLeave", nkObj.Get("stream_user_leave"))
	_ = nkObj.Set("stream_count", func(call goja.FunctionCall) goja.Value {
		n, err := nk.StreamCount(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)))
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(n)
	})
	_ = nkObj.Set("streamCount", nkObj.Get("stream_count"))
	_ = nkObj.Set("stream_send", func(call goja.FunctionCall) goja.Value {
		ids := jsStringSlice(call.Argument(5).Export())
		if err := nk.StreamSend(int16(call.Argument(0).ToInteger()), jsOptString(call.Argument(1)), jsOptString(call.Argument(2)), jsOptString(call.Argument(3)), call.Argument(4).String(), ids, jsOptBool(call.Argument(6), true)); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("streamSend", nkObj.Get("stream_send"))
	_ = nkObj.Set("session_disconnect", func(call goja.FunctionCall) goja.Value {
		if err := nk.SessionDisconnect(call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})
	_ = nkObj.Set("sessionDisconnect", nkObj.Get("session_disconnect"))

	_ = nkObj.Set("rpc", func(call goja.FunctionCall) goja.Value {
		payload := ""
		if !goja.IsUndefined(call.Argument(1)) {
			payload = call.Argument(1).String()
		}
		res, err := nk.RpcCall(context.Background(), call.Argument(0).String(), payload)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(res)
	})
	_ = nkObj.Set("rpc_call", nkObj.Get("rpc"))
	_ = nkObj.Set("rpcCall", nkObj.Get("rpc"))
	_ = nkObj.Set("getSatori", func(call goja.FunctionCall) goja.Value {
		client := nk.GetSatori()
		if client == nil {
			return goja.Null()
		}
		obj := vm.NewObject()
		argStrings := func(v goja.Value) []string {
			if goja.IsUndefined(v) || goja.IsNull(v) {
				return nil
			}
			return jsStringSlice(v.Export())
		}
		_ = obj.Set("authenticate", func(call goja.FunctionCall) goja.Value {
			var def, custom map[string]string
			_ = vm.ExportTo(call.Argument(1), &def)
			_ = vm.ExportTo(call.Argument(2), &custom)
			noSession := false
			if !goja.IsUndefined(call.Argument(3)) {
				noSession = call.Argument(3).ToBoolean()
			}
			out, err := client.Authenticate(context.Background(), call.Argument(0).String(), def, custom, noSession)
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("identityDelete", func(call goja.FunctionCall) goja.Value {
			if err := client.IdentityDelete(context.Background(), call.Argument(0).String()); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("propertiesGet", func(call goja.FunctionCall) goja.Value {
			out, err := client.PropertiesGet(context.Background(), call.Argument(0).String())
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("propertiesUpdate", func(call goja.FunctionCall) goja.Value {
			upd := &satori.PropertiesUpdate{}
			_ = vm.ExportTo(call.Argument(1), &upd.Default)
			_ = vm.ExportTo(call.Argument(2), &upd.Custom)
			if err := client.PropertiesUpdate(context.Background(), call.Argument(0).String(), upd); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("eventsPublish", func(call goja.FunctionCall) goja.Value {
			var events []*satori.Event
			_ = vm.ExportTo(call.Argument(1), &events)
			if err := client.EventsPublish(context.Background(), call.Argument(0).String(), events); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("serverEventsPublish", func(call goja.FunctionCall) goja.Value {
			var events []*satori.Event
			_ = vm.ExportTo(call.Argument(0), &events)
			if err := client.ServerEventsPublish(context.Background(), events); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("experimentsList", func(call goja.FunctionCall) goja.Value {
			out, err := client.ExperimentsList(context.Background(), call.Argument(0).String(), argStrings(call.Argument(1)), argStrings(call.Argument(2)))
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("flagsList", func(call goja.FunctionCall) goja.Value {
			out, err := client.FlagsList(context.Background(), call.Argument(0).String(), argStrings(call.Argument(1)), argStrings(call.Argument(2)))
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("flagsOverridesList", func(call goja.FunctionCall) goja.Value {
			out, err := client.FlagsOverridesList(context.Background(), call.Argument(0).String(), argStrings(call.Argument(1)), argStrings(call.Argument(2)))
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("liveEventsList", func(call goja.FunctionCall) goja.Value {
			out, err := client.LiveEventsList(context.Background(), call.Argument(0).String(), argStrings(call.Argument(1)), argStrings(call.Argument(2)),
				int32(call.Argument(3).ToInteger()), int32(call.Argument(4).ToInteger()), call.Argument(5).ToInteger(), call.Argument(6).ToInteger())
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("liveEventJoin", func(call goja.FunctionCall) goja.Value {
			if err := client.LiveEventJoin(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("messagesList", func(call goja.FunctionCall) goja.Value {
			limit := 100
			if !goja.IsUndefined(call.Argument(1)) {
				limit = int(call.Argument(1).ToInteger())
			}
			forward := true
			if !goja.IsUndefined(call.Argument(2)) {
				forward = call.Argument(2).ToBoolean()
			}
			cursor := ""
			if !goja.IsUndefined(call.Argument(3)) {
				cursor = call.Argument(3).String()
			}
			out, err := client.MessagesList(context.Background(), call.Argument(0).String(), limit, forward, cursor, argStrings(call.Argument(4)))
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return vm.ToValue(out)
		})
		_ = obj.Set("messageUpdate", func(call goja.FunctionCall) goja.Value {
			if err := client.MessageUpdate(context.Background(), call.Argument(0).String(), call.Argument(1).String(), call.Argument(2).ToInteger(), call.Argument(3).ToInteger()); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = obj.Set("messageDelete", func(call goja.FunctionCall) goja.Value {
			if err := client.MessageDelete(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		for _, pair := range [][2]string{
			{"authenticate", "authenticate"},
			{"identityDelete", "identity_delete"},
			{"propertiesGet", "properties_get"},
			{"propertiesUpdate", "properties_update"},
			{"eventsPublish", "events_publish"},
			{"serverEventsPublish", "server_events_publish"},
			{"experimentsList", "experiments_list"},
			{"flagsList", "flags_list"},
			{"flagsOverridesList", "flags_overrides_list"},
			{"liveEventsList", "live_events_list"},
			{"liveEventJoin", "live_event_join"},
			{"messagesList", "messages_list"},
			{"messageUpdate", "message_update"},
			{"messageDelete", "message_delete"},
		} {
			_ = obj.Set(pair[1], obj.Get(pair[0]))
		}
		return obj
	})
	_ = nkObj.Set("get_satori", nkObj.Get("getSatori"))

	if len(registry) > 0 && registry[0] != nil {
		reg := registry[0]
		_ = nkObj.Set("registerRpc", func(call goja.FunctionCall) goja.Value {
			id := strings.ToLower(call.Argument(0).String())
			fn, ok := goja.AssertFunction(call.Argument(1))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("registerRpc requires a function")))
			}
			globalName := "__rpc_" + id
			_ = vm.Set(globalName, fn)
			reg.RegisterJSRPC(id, globalName)
			return goja.Undefined()
		})
		_ = nkObj.Set("register_rpc", nkObj.Get("registerRpc"))

		registerBefore := func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register before requires a function")))
			}
			id := call.Argument(1).String()
			globalName := "__before_" + id
			_ = vm.Set(globalName, fn)
			reg.RegisterJSBefore(id, globalName)
			return goja.Undefined()
		}
		registerAfter := func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register after requires a function")))
			}
			id := call.Argument(1).String()
			globalName := "__after_" + id
			_ = vm.Set(globalName, fn)
			reg.RegisterJSAfter(id, globalName)
			return goja.Undefined()
		}
		_ = nkObj.Set("register_req_before", registerBefore)
		_ = nkObj.Set("registerReqBefore", registerBefore)
		_ = nkObj.Set("register_req_after", registerAfter)
		_ = nkObj.Set("registerReqAfter", registerAfter)
		_ = nkObj.Set("register_rt_before", registerBefore)
		_ = nkObj.Set("registerRtBefore", registerBefore)
		_ = nkObj.Set("register_rt_after", registerAfter)
		_ = nkObj.Set("registerRtAfter", registerAfter)

		_ = nkObj.Set("registerPurchaseNotificationApple", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("expects a function")))
			}
			reg.RegisterPurchaseNotificationApple(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, notificationType int, purchase *ValidatedPurchaseView, rawPayload string) error {
				_, err := fn(goja.Undefined(), vm.ToValue(notificationType), vm.ToValue(purchase), vm.ToValue(rawPayload))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_purchase_notification_apple", nkObj.Get("registerPurchaseNotificationApple"))
		_ = nkObj.Set("registerPurchaseNotificationGoogle", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("expects a function")))
			}
			reg.RegisterPurchaseNotificationGoogle(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, notificationType int, purchase *ValidatedPurchaseView, rawPayload string) error {
				_, err := fn(goja.Undefined(), vm.ToValue(notificationType), vm.ToValue(purchase), vm.ToValue(rawPayload))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_purchase_notification_google", nkObj.Get("registerPurchaseNotificationGoogle"))
		_ = nkObj.Set("registerSubscriptionNotificationApple", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("expects a function")))
			}
			reg.RegisterSubscriptionNotificationApple(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, notificationType int, subscription *ValidatedSubscriptionView, rawPayload string) error {
				_, err := fn(goja.Undefined(), vm.ToValue(notificationType), vm.ToValue(subscription), vm.ToValue(rawPayload))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_subscription_notification_apple", nkObj.Get("registerSubscriptionNotificationApple"))
		_ = nkObj.Set("registerSubscriptionNotificationGoogle", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("expects a function")))
			}
			reg.RegisterSubscriptionNotificationGoogle(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, notificationType int, subscription *ValidatedSubscriptionView, rawPayload string) error {
				_, err := fn(goja.Undefined(), vm.ToValue(notificationType), vm.ToValue(subscription), vm.ToValue(rawPayload))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_subscription_notification_google", nkObj.Get("registerSubscriptionNotificationGoogle"))

		_ = nkObj.Set("register_matchmaker_matched", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register_matchmaker_matched requires a function")))
			}
			reg.RegisterMatchmakerMatched(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, entries []interface{}) (string, error) {
				ret, err := fn(goja.Undefined(), vm.ToValue(entries))
				if err != nil {
					return "", err
				}
				if goja.IsUndefined(ret) || goja.IsNull(ret) {
					return "", nil
				}
				return ret.String(), nil
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("registerMatchmakerMatched", nkObj.Get("register_matchmaker_matched"))

		_ = nkObj.Set("register_matchmaker_override", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register_matchmaker_override requires a function")))
			}
			reg.RegisterMatchmakerOverride(func(ctx context.Context, matches [][]interface{}) [][]interface{} {
				ret, err := fn(goja.Undefined(), vm.ToValue(matches))
				if err != nil || goja.IsUndefined(ret) || goja.IsNull(ret) {
					return matches
				}
				exported := ret.Export()
				outer, ok := exported.([]interface{})
				if !ok {
					return matches
				}
				result := make([][]interface{}, 0, len(outer))
				for _, g := range outer {
					if slice, ok := g.([]interface{}); ok {
						result = append(result, slice)
					}
				}
				return result
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("registerMatchmakerOverride", nkObj.Get("register_matchmaker_override"))

		_ = nkObj.Set("register_matchmaker_processor", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register_matchmaker_processor requires a function")))
			}
			reg.RegisterMatchmakerProcessor(func(ctx context.Context, tickets []interface{}) [][]interface{} {
				ret, err := fn(goja.Undefined(), vm.ToValue(tickets))
				if err != nil || goja.IsUndefined(ret) || goja.IsNull(ret) {
					return nil
				}
				exported := ret.Export()
				outer, ok := exported.([]interface{})
				if !ok {
					return nil
				}
				result := make([][]interface{}, 0, len(outer))
				for _, g := range outer {
					if slice, ok := g.([]interface{}); ok {
						result = append(result, slice)
					}
				}
				return result
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("registerMatchmakerProcessor", nkObj.Get("register_matchmaker_processor"))

		_ = nkObj.Set("register_shutdown", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("register_shutdown requires a function")))
			}
			reg.RegisterShutdown(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule) {
				_, _ = fn(goja.Undefined())
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("registerShutdown", nkObj.Get("register_shutdown"))

		_ = nkObj.Set("registerCron", func(call goja.FunctionCall) goja.Value {
			name := call.Argument(0).String()
			schedule := call.Argument(1).String()
			fn, ok := goja.AssertFunction(call.Argument(2))
			if !ok {
				panic(vm.NewGoError(fmt.Errorf("registerCron requires a function")))
			}
			err := reg.RegisterCron(name, &CronJob{
				Schedule: schedule,
				Handler: func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule) error {
					_, err := fn(goja.Undefined())
					return err
				},
			})
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return goja.Undefined()
		})
		_ = nkObj.Set("register_cron", nkObj.Get("registerCron"))
	}

	_ = nkObj.Set("cronNext", func(call goja.FunctionCall) goja.Value {
		expr := call.Argument(0).String()
		ts := call.Argument(1).ToInteger()
		next, err := nk.CronNext(expr, ts)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(next)
	})
	_ = nkObj.Set("cron_next", nkObj.Get("cronNext"))
	_ = nkObj.Set("cronPrev", func(call goja.FunctionCall) goja.Value {
		expr := call.Argument(0).String()
		ts := call.Argument(1).ToInteger()
		prev, err := nk.CronPrev(expr, ts)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(prev)
	})
	_ = nkObj.Set("cron_prev", nkObj.Get("cronPrev"))

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

	toJSRecords := func(recs []*LeaderboardRecord) []map[string]interface{} {
		out := make([]map[string]interface{}, len(recs))
		for i, r := range recs {
			out[i] = map[string]interface{}{
				"leaderboard_id": r.LeaderboardID, "owner_id": r.OwnerID, "username": r.Username,
				"score": r.Score, "subscore": r.Subscore, "rank": r.Rank, "metadata": r.Metadata,
			}
		}
		return out
	}

	_ = nkObj.Set("leaderboard_records_list", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		var ownerIDs []string
		if arr, ok := call.Argument(1).Export().([]interface{}); ok {
			for _, v := range arr {
				ownerIDs = append(ownerIDs, fmt.Sprintf("%v", v))
			}
		}
		limit := int(call.Argument(2).ToInteger())
		cursor := call.Argument(3).String()
		expiry := call.Argument(4).ToInteger()
		recs, next, prev, err := nk.LeaderboardRecordsList(context.Background(), id, ownerIDs, limit, cursor, expiry)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs), "next_cursor": next, "prev_cursor": prev})
	})

	_ = nkObj.Set("leaderboard_records_around_owner", func(call goja.FunctionCall) goja.Value {
		recs, err := nk.LeaderboardRecordsAroundOwner(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger()), call.Argument(3).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs)})
	})

	_ = nkObj.Set("leaderboard_records_haystack", func(call goja.FunctionCall) goja.Value {
		recs, next, prev, err := nk.LeaderboardRecordsHaystack(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger()), call.Argument(3).String(), call.Argument(4).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs), "next_cursor": next, "prev_cursor": prev})
	})

	_ = nkObj.Set("leaderboard_records_list_cursor_from_rank", func(call goja.FunctionCall) goja.Value {
		cur, err := nk.LeaderboardRecordsListCursorFromRank(context.Background(), call.Argument(0).String(), call.Argument(1).ToInteger(), call.Argument(2).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(cur)
	})

	_ = nkObj.Set("leaderboard_list", func(call goja.FunctionCall) goja.Value {
		list, next, err := nk.LeaderboardList(context.Background(), int(call.Argument(0).ToInteger()), call.Argument(1).String())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		ids := make([]string, len(list))
		for i, lb := range list {
			ids[i] = lb.ID
		}
		return vm.ToValue(map[string]interface{}{"leaderboards": ids, "next_cursor": next})
	})

	_ = nkObj.Set("leaderboard_ranks_disable", func(call goja.FunctionCall) goja.Value {
		if err := nk.LeaderboardRanksDisable(context.Background(), call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("tournament_ranks_disable", func(call goja.FunctionCall) goja.Value {
		if err := nk.TournamentRanksDisable(context.Background(), call.Argument(0).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("tournament_record_delete", func(call goja.FunctionCall) goja.Value {
		if err := nk.TournamentRecordDelete(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("tournament_records_haystack", func(call goja.FunctionCall) goja.Value {
		recs, next, prev, err := nk.TournamentRecordsHaystack(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger()), call.Argument(3).String(), call.Argument(4).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs), "next_cursor": next, "prev_cursor": prev})
	})

	_ = nkObj.Set("leaderboard_record_delete", func(call goja.FunctionCall) goja.Value {
		if err := nk.LeaderboardRecordDelete(context.Background(), call.Argument(0).String(), call.Argument(1).String()); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	_ = nkObj.Set("tournament_record_write", func(call goja.FunctionCall) goja.Value {
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
		rec, err := nk.TournamentRecordWrite(context.Background(), id, ownerID, username, score, subscore, metadata)
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"leaderboard_id": rec.LeaderboardID, "score": rec.Score, "rank": rec.Rank})
	})

	_ = nkObj.Set("tournament_records_list", func(call goja.FunctionCall) goja.Value {
		id := call.Argument(0).String()
		var ownerIDs []string
		if arr, ok := call.Argument(1).Export().([]interface{}); ok {
			for _, v := range arr {
				ownerIDs = append(ownerIDs, fmt.Sprintf("%v", v))
			}
		}
		recs, next, prev, err := nk.TournamentRecordsList(context.Background(), id, ownerIDs, int(call.Argument(2).ToInteger()), call.Argument(3).String(), call.Argument(4).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs), "next_cursor": next, "prev_cursor": prev})
	})

	_ = nkObj.Set("tournament_records_around_owner", func(call goja.FunctionCall) goja.Value {
		recs, err := nk.TournamentRecordsAroundOwner(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger()), call.Argument(3).ToInteger())
		if err != nil {
			panic(vm.NewGoError(err))
		}
		return vm.ToValue(map[string]interface{}{"records": toJSRecords(recs)})
	})

	_ = nkObj.Set("tournament_add_attempt", func(call goja.FunctionCall) goja.Value {
		if err := nk.TournamentAddAttempt(context.Background(), call.Argument(0).String(), call.Argument(1).String(), int(call.Argument(2).ToInteger())); err != nil {
			panic(vm.NewGoError(err))
		}
		return goja.Undefined()
	})

	if len(registry) > 0 && registry[0] != nil {
		reg := registry[0]
		_ = nkObj.Set("register_leaderboard_reset", func(call goja.FunctionCall) goja.Value {
			cb, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewTypeError("expected function"))
			}
			reg.RegisterLeaderboardReset(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, leaderboardID string, reset int64) error {
				_, err := cb(goja.Undefined(), vm.ToValue(leaderboardID), vm.ToValue(reset))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_tournament_end", func(call goja.FunctionCall) goja.Value {
			cb, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewTypeError("expected function"))
			}
			reg.RegisterTournamentEnd(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, tournamentID string, end, reset int64) error {
				_, err := cb(goja.Undefined(), vm.ToValue(tournamentID), vm.ToValue(end), vm.ToValue(reset))
				return err
			})
			return goja.Undefined()
		})
		_ = nkObj.Set("register_tournament_reset", func(call goja.FunctionCall) goja.Value {
			cb, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.NewTypeError("expected function"))
			}
			reg.RegisterTournamentReset(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, tournamentID string, end, reset int64) error {
				_, err := cb(goja.Undefined(), vm.ToValue(tournamentID), vm.ToValue(end), vm.ToValue(reset))
				return err
			})
			return goja.Undefined()
		})
	}

	mapJSNKExtended(vm, nkObj, nk)

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

	// Translate modified JS object back to Go
	goMap := resVal.Export()
	if _, ok := in.(map[string]interface{}); ok {
		goBytes, err := json.Marshal(goMap)
		if err != nil {
			return nil, err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(goBytes, &m); err != nil {
			return nil, err
		}
		return m, nil
	}

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

func jsStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func jsOptString(v goja.Value) string {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return ""
	}
	return v.String()
}

func jsOptBool(v goja.Value, def bool) bool {
	if goja.IsUndefined(v) || goja.IsNull(v) {
		return def
	}
	return v.ToBoolean()
}

func parseJSAccountUpdates(raw interface{}) []*AccountUpdateParams {
	arr, _ := raw.([]interface{})
	out := make([]*AccountUpdateParams, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		u := &AccountUpdateParams{UserID: fmt.Sprint(m["user_id"])}
		if v, ok := m["username"].(string); ok {
			u.Username = &v
		}
		if v, ok := m["display_name"].(string); ok {
			u.DisplayName = &v
		}
		out = append(out, u)
	}
	return out
}

func parseJSStorageWrites(raw interface{}) []*StorageWrite {
	arr, _ := raw.([]interface{})
	out := make([]*StorageWrite, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		w := &StorageWrite{
			Collection: fmt.Sprint(m["collection"]),
			Key:        fmt.Sprint(m["key"]),
			UserID:     fmt.Sprint(m["user_id"]),
			Version:    fmt.Sprint(m["version"]),
		}
		switch v := m["value"].(type) {
		case string:
			w.Value = v
		default:
			b, _ := json.Marshal(v)
			w.Value = string(b)
		}
		out = append(out, w)
	}
	return out
}

func parseJSStorageDeletes(raw interface{}) []*StorageDelete {
	arr, _ := raw.([]interface{})
	out := make([]*StorageDelete, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		out = append(out, &StorageDelete{
			Collection: fmt.Sprint(m["collection"]),
			Key:        fmt.Sprint(m["key"]),
			UserID:     fmt.Sprint(m["user_id"]),
			Version:    fmt.Sprint(m["version"]),
		})
	}
	return out
}

func parseJSWalletUpdates(raw interface{}) []*WalletUpdateParams {
	arr, _ := raw.([]interface{})
	out := make([]*WalletUpdateParams, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		u := &WalletUpdateParams{UserID: fmt.Sprint(m["user_id"])}
		if cs, ok := m["changeset"].(map[string]interface{}); ok {
			u.Changeset = make(map[string]int64)
			for k, v := range cs {
				if f, ok := v.(float64); ok {
					u.Changeset[k] = int64(f)
				}
			}
		}
		out = append(out, u)
	}
	return out
}
