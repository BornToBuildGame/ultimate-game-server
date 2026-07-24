package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yuin/gopher-lua"
)

// MapLuaNK binds the core storage APIs to a Gopher-Lua VM.
func MapLuaNK(L *lua.LState, nk RuntimeModule, registry ...*HookRegistry) {
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

	L.SetField(nkTable, "storage_list", L.NewFunction(func(L *lua.LState) int {
		callerID := L.OptString(1, "")
		userID := L.OptString(2, "")
		collection := L.CheckString(3)
		limit := L.OptInt(4, 100)
		cursor := L.OptString(5, "")
		list, next, err := nk.StorageList(L.Context(), callerID, userID, collection, limit, cursor)
		if err != nil {
			L.RaiseError("storage_list failed: %v", err)
			return 0
		}
		resTable := L.NewTable()
		for _, o := range list {
			oTbl := L.NewTable()
			L.SetField(oTbl, "collection", lua.LString(o.Collection))
			L.SetField(oTbl, "key", lua.LString(o.Key))
			L.SetField(oTbl, "user_id", lua.LString(o.UserID))
			L.SetField(oTbl, "value", lua.LString(o.Value))
			L.SetField(oTbl, "version", lua.LString(o.Version))
			resTable.Append(oTbl)
		}
		L.Push(resTable)
		L.Push(lua.LString(next))
		return 2
	}))

	// 4. Wallet Update
	L.SetField(nkTable, "wallet_update", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		changesetTbl := L.CheckTable(2)
		metadataTbl := L.OptTable(3, nil)
		updateLedger := L.OptBool(4, true)

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

		updated, previous, err := nk.WalletUpdate(L.Context(), userID, changeset, metadata, updateLedger)
		if err != nil {
			L.RaiseError("wallet_update failed: %v", err)
			return 0
		}

		L.Push(ToLuaValue(L, updated))
		L.Push(ToLuaValue(L, previous))
		return 2
	}))

	L.SetField(nkTable, "wallets_update", L.NewFunction(func(L *lua.LState) int {
		updatesTbl := L.CheckTable(1)
		updateLedger := L.OptBool(2, true)
		raw := ToGoValue(updatesTbl)
		arr, _ := raw.([]interface{})
		updates := make([]*WalletUpdateParams, 0, len(arr))
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
		results, err := nk.WalletsUpdate(L.Context(), updates, updateLedger)
		if err != nil {
			L.RaiseError("wallets_update failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, results))
		return 1
	}))

	L.SetField(nkTable, "wallet_ledger_list", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		limit := L.OptInt(2, 100)
		cursor := L.OptString(3, "")
		items, next, err := nk.WalletLedgerList(L.Context(), userID, limit, cursor)
		if err != nil {
			L.RaiseError("wallet_ledger_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, items))
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "wallet_ledger_update", L.NewFunction(func(L *lua.LState) int {
		ledgerID := L.CheckString(1)
		userID := L.CheckString(2)
		metadataTbl := L.CheckTable(3)
		var metadata map[string]interface{}
		if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
			metadata = m
		}
		if err := nk.WalletLedgerUpdate(L.Context(), ledgerID, userID, metadata); err != nil {
			L.RaiseError("wallet_ledger_update failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "purchase_validate_apple", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		receipt := L.CheckString(2)
		persist := L.OptBool(3, true)
		vp, err := nk.PurchaseValidateApple(L.Context(), userID, receipt, persist)
		if err != nil {
			L.RaiseError("purchase_validate_apple failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, vp))
		return 1
	}))
	L.SetField(nkTable, "purchase_validate_google", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		productID := L.CheckString(2)
		token := L.CheckString(3)
		persist := L.OptBool(4, true)
		vp, err := nk.PurchaseValidateGoogle(L.Context(), userID, productID, token, persist)
		if err != nil {
			L.RaiseError("purchase_validate_google failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, vp))
		return 1
	}))
	L.SetField(nkTable, "purchase_validate_huawei", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		data := L.CheckString(2)
		sig := L.OptString(3, "")
		persist := L.OptBool(4, true)
		vp, err := nk.PurchaseValidateHuawei(L.Context(), userID, data, sig, persist)
		if err != nil {
			L.RaiseError("purchase_validate_huawei failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, vp))
		return 1
	}))
	L.SetField(nkTable, "purchase_validate_facebook_instant", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		signed := L.CheckString(2)
		persist := L.OptBool(3, true)
		vp, err := nk.PurchaseValidateFacebookInstant(L.Context(), userID, signed, persist)
		if err != nil {
			L.RaiseError("purchase_validate_facebook_instant failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, vp))
		return 1
	}))
	L.SetField(nkTable, "purchase_validate_samsung", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		purchaseID := L.CheckString(2)
		persist := L.OptBool(3, true)
		vp, err := nk.PurchaseValidateSamsung(L.Context(), userID, purchaseID, persist)
		if err != nil {
			L.RaiseError("purchase_validate_samsung failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, vp))
		return 1
	}))
	L.SetField(nkTable, "purchases_list", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		limit := L.OptInt(2, 100)
		list, err := nk.PurchasesList(L.Context(), userID, limit)
		if err != nil {
			L.RaiseError("purchases_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))
	L.SetField(nkTable, "subscription_validate_apple", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		receipt := L.CheckString(2)
		persist := L.OptBool(3, true)
		sub, err := nk.SubscriptionValidateApple(L.Context(), userID, receipt, persist)
		if err != nil {
			L.RaiseError("subscription_validate_apple failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, sub))
		return 1
	}))
	L.SetField(nkTable, "subscription_validate_google", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		productID := L.CheckString(2)
		token := L.CheckString(3)
		persist := L.OptBool(4, true)
		sub, err := nk.SubscriptionValidateGoogle(L.Context(), userID, productID, token, persist)
		if err != nil {
			L.RaiseError("subscription_validate_google failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, sub))
		return 1
	}))
	L.SetField(nkTable, "subscriptions_list", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		limit := L.OptInt(2, 100)
		list, err := nk.SubscriptionsList(L.Context(), userID, limit)
		if err != nil {
			L.RaiseError("subscriptions_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))
	L.SetField(nkTable, "subscription_get_product_id", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		productID := L.CheckString(2)
		sub, err := nk.SubscriptionGetProductID(L.Context(), userID, productID)
		if err != nil {
			L.RaiseError("subscription_get_product_id failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, sub))
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

	// 6. Notification APIs
	L.SetField(nkTable, "notification_send", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		subject := L.CheckString(2)
		contentTbl := L.CheckTable(3)
		code := L.CheckInt(4)
		senderID := L.OptString(5, "")
		persistent := L.OptBool(6, false)

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

	L.SetField(nkTable, "notifications_list", L.NewFunction(func(L *lua.LState) int {
		list, cursor, err := nk.NotificationsList(L.Context(), L.CheckString(1), L.OptInt(2, 100), L.OptString(3, ""))
		if err != nil {
			L.RaiseError("notifications_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, n := range list {
			row := L.NewTable()
			L.SetField(row, "id", lua.LString(n.ID))
			L.SetField(row, "subject", lua.LString(n.Subject))
			L.SetField(row, "content", lua.LString(n.Content))
			L.SetField(row, "code", lua.LNumber(n.Code))
			L.SetField(row, "sender_id", lua.LString(n.SenderID))
			L.SetField(row, "persistent", lua.LBool(n.Persistent))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(cursor))
		return 2
	}))

	L.SetField(nkTable, "notifications_delete", L.NewFunction(func(L *lua.LState) int {
		ids := luaStringSlice(L.CheckTable(2))
		if err := nk.NotificationsDelete(L.Context(), L.CheckString(1), ids); err != nil {
			L.RaiseError("notifications_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "notification_send_all", L.NewFunction(func(L *lua.LState) int {
		subject := L.CheckString(1)
		contentTbl := L.CheckTable(2)
		code := L.CheckInt(3)
		persistent := L.OptBool(4, false)
		var content map[string]interface{}
		if m, ok := ToGoValue(contentTbl).(map[string]interface{}); ok {
			content = m
		}
		if err := nk.NotificationSendAll(L.Context(), subject, content, code, persistent); err != nil {
			L.RaiseError("notification_send_all failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "notifications_send", L.NewFunction(func(L *lua.LState) int {
		tbl := L.CheckTable(1)
		var params []*NotificationSendParams
		tbl.ForEach(func(_, v lua.LValue) {
			row, ok := v.(*lua.LTable)
			if !ok {
				return
			}
			p := &NotificationSendParams{}
			if s, ok := row.RawGetString("user_id").(lua.LString); ok {
				p.UserID = string(s)
			}
			if s, ok := row.RawGetString("subject").(lua.LString); ok {
				p.Subject = string(s)
			}
			if n, ok := row.RawGetString("code").(lua.LNumber); ok {
				p.Code = int(n)
			}
			if s, ok := row.RawGetString("sender_id").(lua.LString); ok {
				p.SenderID = string(s)
			}
			if b, ok := row.RawGetString("persistent").(lua.LBool); ok {
				p.Persistent = bool(b)
			}
			if c := row.RawGetString("content"); c != lua.LNil {
				if m, ok := ToGoValue(c).(map[string]interface{}); ok {
					p.Content = m
				}
			}
			params = append(params, p)
		})
		if err := nk.NotificationsSend(L.Context(), params); err != nil {
			L.RaiseError("notifications_send failed: %v", err)
			return 0
		}
		return 0
	}))

	// Friends APIs
	L.SetField(nkTable, "friends_list", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		limit := L.OptInt(2, 100)
		var statePtr *int
		if L.GetTop() >= 3 && L.Get(3).Type() != lua.LTNil {
			st := L.CheckInt(3)
			statePtr = &st
		}
		cursor := L.OptString(4, "")
		list, next, err := nk.FriendsList(L.Context(), userID, limit, statePtr, cursor)
		if err != nil {
			L.RaiseError("friends_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, f := range list {
			row := L.NewTable()
			L.SetField(row, "user_id", lua.LString(f.UserID))
			L.SetField(row, "username", lua.LString(f.Username))
			L.SetField(row, "state", lua.LNumber(f.State))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "party_list", L.NewFunction(func(L *lua.LState) int {
		limit := L.OptInt(1, 10)
		var openPtr *bool
		if L.GetTop() >= 2 && L.Get(2).Type() != lua.LTNil {
			b := L.CheckBool(2)
			openPtr = &b
		}
		hidden := L.OptBool(3, false)
		query := L.OptString(4, "")
		cursor := L.OptString(5, "")
		list, next, err := nk.PartyList(L.Context(), limit, openPtr, hidden, query, cursor)
		if err != nil {
			L.RaiseError("party_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, p := range list {
			row := L.NewTable()
			L.SetField(row, "id", lua.LString(p.ID))
			L.SetField(row, "open", lua.LBool(p.Open))
			L.SetField(row, "hidden", lua.LBool(p.Hidden))
			L.SetField(row, "max_size", lua.LNumber(p.MaxSize))
			L.SetField(row, "label", lua.LString(p.Label))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "channel_id_build", L.NewFunction(func(L *lua.LState) int {
		id, err := nk.ChannelIdBuild(L.Context(), L.CheckString(1), L.CheckString(2), L.OptInt(3, 1))
		if err != nil {
			L.RaiseError("channel_id_build failed: %v", err)
			return 0
		}
		L.Push(lua.LString(id))
		return 1
	}))

	L.SetField(nkTable, "channel_message_send", L.NewFunction(func(L *lua.LState) int {
		channelID := L.CheckString(1)
		contentTbl := L.CheckTable(2)
		var content map[string]interface{}
		if m, ok := ToGoValue(contentTbl).(map[string]interface{}); ok {
			content = m
		}
		ack, err := nk.ChannelMessageSend(L.Context(), channelID, content, L.OptString(3, ""), L.OptString(4, ""), L.OptBool(5, true))
		if err != nil {
			L.RaiseError("channel_message_send failed: %v", err)
			return 0
		}
		row := L.NewTable()
		L.SetField(row, "channel_id", lua.LString(ack.ChannelID))
		L.SetField(row, "message_id", lua.LString(ack.MessageID))
		L.SetField(row, "code", lua.LNumber(ack.Code))
		L.SetField(row, "persistent", lua.LBool(ack.Persistent))
		L.Push(row)
		return 1
	}))

	L.SetField(nkTable, "channel_message_update", L.NewFunction(func(L *lua.LState) int {
		var content map[string]interface{}
		if m, ok := ToGoValue(L.CheckTable(3)).(map[string]interface{}); ok {
			content = m
		}
		ack, err := nk.ChannelMessageUpdate(L.Context(), L.CheckString(1), L.CheckString(2), content, L.OptString(4, ""), L.OptString(5, ""), L.OptBool(6, true))
		if err != nil {
			L.RaiseError("channel_message_update failed: %v", err)
			return 0
		}
		row := L.NewTable()
		L.SetField(row, "message_id", lua.LString(ack.MessageID))
		L.SetField(row, "code", lua.LNumber(ack.Code))
		L.Push(row)
		return 1
	}))

	L.SetField(nkTable, "channel_message_remove", L.NewFunction(func(L *lua.LState) int {
		ack, err := nk.ChannelMessageRemove(L.Context(), L.CheckString(1), L.CheckString(2), L.OptString(3, ""), L.OptString(4, ""), L.OptBool(5, true))
		if err != nil {
			L.RaiseError("channel_message_remove failed: %v", err)
			return 0
		}
		row := L.NewTable()
		L.SetField(row, "message_id", lua.LString(ack.MessageID))
		L.SetField(row, "code", lua.LNumber(ack.Code))
		L.Push(row)
		return 1
	}))

	L.SetField(nkTable, "channel_messages_list", L.NewFunction(func(L *lua.LState) int {
		list, next, prev, cacheable, err := nk.ChannelMessagesList(L.Context(), L.CheckString(1), L.OptInt(2, 20), L.OptBool(3, true), L.OptString(4, ""))
		if err != nil {
			L.RaiseError("channel_messages_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, m := range list {
			row := L.NewTable()
			L.SetField(row, "channel_id", lua.LString(m.ChannelID))
			L.SetField(row, "message_id", lua.LString(m.MessageID))
			L.SetField(row, "code", lua.LNumber(m.Code))
			L.SetField(row, "sender_id", lua.LString(m.SenderID))
			L.SetField(row, "username", lua.LString(m.Username))
			L.SetField(row, "content", lua.LString(m.Content))
			L.SetField(row, "persistent", lua.LBool(m.Persistent))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		L.Push(lua.LString(prev))
		L.Push(lua.LString(cacheable))
		return 4
	}))

	L.SetField(nkTable, "group_create", L.NewFunction(func(L *lua.LState) int {
		g, err := nk.GroupCreate(L.Context(), L.CheckString(1), L.CheckString(2), L.OptString(3, ""), L.OptString(4, ""), L.OptString(5, "en"), L.OptString(6, "{}"), L.OptBool(7, true), L.OptInt(8, 100))
		if err != nil {
			L.RaiseError("group_create failed: %v", err)
			return 0
		}
		row := L.NewTable()
		L.SetField(row, "id", lua.LString(g.ID))
		L.SetField(row, "name", lua.LString(g.Name))
		L.SetField(row, "open", lua.LBool(g.Open))
		L.Push(row)
		return 1
	}))

	L.SetField(nkTable, "groups_list", L.NewFunction(func(L *lua.LState) int {
		var openPtr *bool
		if L.GetTop() >= 3 && L.Get(3).Type() != lua.LTNil {
			b := L.CheckBool(3)
			openPtr = &b
		}
		list, next, err := nk.GroupsList(L.Context(), L.OptString(1, ""), L.OptString(2, ""), openPtr, L.OptInt(4, 0), L.OptInt(5, 10), L.OptString(6, ""))
		if err != nil {
			L.RaiseError("groups_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, g := range list {
			row := L.NewTable()
			L.SetField(row, "id", lua.LString(g.ID))
			L.SetField(row, "name", lua.LString(g.Name))
			L.SetField(row, "open", lua.LBool(g.Open))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "user_groups_list", L.NewFunction(func(L *lua.LState) int {
		list, next, err := nk.UserGroupsList(L.Context(), L.CheckString(1), L.OptInt(2, 10), L.OptString(3, ""))
		if err != nil {
			L.RaiseError("user_groups_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, ug := range list {
			row := L.NewTable()
			if ug.Group != nil {
				L.SetField(row, "group_id", lua.LString(ug.Group.ID))
				L.SetField(row, "name", lua.LString(ug.Group.Name))
			}
			L.SetField(row, "state", lua.LNumber(ug.State))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "friends_add", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		idsTbl := L.OptTable(2, nil)
		usernamesTbl := L.OptTable(3, nil)
		ids := luaStringSlice(idsTbl)
		usernames := luaStringSlice(usernamesTbl)
		if err := nk.FriendsAdd(L.Context(), userID, ids, usernames, nil); err != nil {
			L.RaiseError("friends_add failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "friends_delete", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		ids := luaStringSlice(L.OptTable(2, nil))
		usernames := luaStringSlice(L.OptTable(3, nil))
		if err := nk.FriendsDelete(L.Context(), userID, ids, usernames); err != nil {
			L.RaiseError("friends_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "friends_block", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		ids := luaStringSlice(L.OptTable(2, nil))
		usernames := luaStringSlice(L.OptTable(3, nil))
		if err := nk.FriendsBlock(L.Context(), userID, ids, usernames); err != nil {
			L.RaiseError("friends_block failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "friends_of_friends_list", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		limit := L.OptInt(2, 100)
		cursor := L.OptString(3, "")
		list, next, err := nk.FriendsOfFriendsList(L.Context(), userID, limit, cursor)
		if err != nil {
			L.RaiseError("friends_of_friends_list failed: %v", err)
			return 0
		}
		tbl := L.CreateTable(len(list), 0)
		for i, f := range list {
			row := L.NewTable()
			L.SetField(row, "referrer", lua.LString(f.Referrer))
			L.SetField(row, "user_id", lua.LString(f.UserID))
			L.SetField(row, "username", lua.LString(f.Username))
			tbl.RawSetInt(i+1, row)
		}
		L.Push(tbl)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "friend_metadata_update", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		friendID := L.CheckString(2)
		metaTbl := L.OptTable(3, nil)
		var metadata map[string]any
		if metaTbl != nil {
			if m, ok := ToGoValue(metaTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}
		if err := nk.FriendMetadataUpdate(L.Context(), userID, friendID, metadata); err != nil {
			L.RaiseError("friend_metadata_update failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "users_get_friend_status", L.NewFunction(func(L *lua.LState) int {
		userID := L.CheckString(1)
		ids := luaStringSlice(L.OptTable(2, nil))
		statusMap, err := nk.UsersGetFriendStatus(L.Context(), userID, ids)
		if err != nil {
			L.RaiseError("users_get_friend_status failed: %v", err)
			return 0
		}
		tbl := L.NewTable()
		for k, v := range statusMap {
			L.SetField(tbl, k, lua.LNumber(v))
		}
		L.Push(tbl)
		return 1
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

	L.SetField(nkTable, "match_list", L.NewFunction(func(L *lua.LState) int {
		limit := L.OptInt(1, 100)
		authoritative := L.OptBool(2, false)
		label := L.OptString(3, "")
		minSize := L.OptInt(4, 0)
		maxSize := L.OptInt(5, 0)
		list, err := nk.MatchList(L.Context(), limit, authoritative, label, minSize, maxSize)
		if err != nil {
			L.RaiseError("match_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))
	L.SetField(nkTable, "match_get", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		info, err := nk.MatchGet(L.Context(), id)
		if err != nil {
			L.Push(lua.LNil)
			return 1
		}
		L.Push(ToLuaValue(L, info))
		return 1
	}))
	L.SetField(nkTable, "match_signal", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		data := L.CheckString(2)
		res, err := nk.MatchSignal(L.Context(), id, data)
		if err != nil {
			L.RaiseError("match_signal failed: %v", err)
			return 0
		}
		L.Push(lua.LString(res))
		return 1
	}))

	L.SetField(nkTable, "status_follow", L.NewFunction(func(L *lua.LState) int {
		sessionID := L.CheckString(1)
		ids := luaStringSlice(L.OptTable(2, nil))
		if err := nk.StatusFollow(sessionID, ids); err != nil {
			L.RaiseError("status_follow failed: %v", err)
			return 0
		}
		return 0
	}))
	L.SetField(nkTable, "status_unfollow", L.NewFunction(func(L *lua.LState) int {
		sessionID := L.CheckString(1)
		ids := luaStringSlice(L.OptTable(2, nil))
		if err := nk.StatusUnfollow(sessionID, ids); err != nil {
			L.RaiseError("status_unfollow failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "stream_user_list", L.NewFunction(func(L *lua.LState) int {
		mode := int16(L.CheckInt(1))
		list, err := nk.StreamUserList(mode, L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), L.OptBool(5, true), L.OptBool(6, true))
		if err != nil {
			L.RaiseError("stream_user_list failed: %v", err)
			return 0
		}
		L.Push(ToLuaValue(L, list))
		return 1
	}))
	L.SetField(nkTable, "stream_user_join", L.NewFunction(func(L *lua.LState) int {
		ok, err := nk.StreamUserJoin(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), L.CheckString(5), L.CheckString(6), L.OptBool(7, false), L.OptBool(8, false), L.OptString(9, ""))
		if err != nil {
			L.RaiseError("stream_user_join failed: %v", err)
			return 0
		}
		L.Push(lua.LBool(ok))
		return 1
	}))
	L.SetField(nkTable, "stream_user_leave", L.NewFunction(func(L *lua.LState) int {
		if err := nk.StreamUserLeave(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), L.OptString(5, ""), L.CheckString(6)); err != nil {
			L.RaiseError("stream_user_leave failed: %v", err)
			return 0
		}
		return 0
	}))
	L.SetField(nkTable, "stream_count", L.NewFunction(func(L *lua.LState) int {
		n, err := nk.StreamCount(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""))
		if err != nil {
			L.RaiseError("stream_count failed: %v", err)
			return 0
		}
		L.Push(lua.LNumber(n))
		return 1
	}))
	L.SetField(nkTable, "stream_send", L.NewFunction(func(L *lua.LState) int {
		ids := luaStringSlice(L.OptTable(6, nil))
		if err := nk.StreamSend(int16(L.CheckInt(1)), L.OptString(2, ""), L.OptString(3, ""), L.OptString(4, ""), L.CheckString(5), ids, L.OptBool(7, true)); err != nil {
			L.RaiseError("stream_send failed: %v", err)
			return 0
		}
		return 0
	}))
	L.SetField(nkTable, "session_disconnect", L.NewFunction(func(L *lua.LState) int {
		if err := nk.SessionDisconnect(L.CheckString(1)); err != nil {
			L.RaiseError("session_disconnect failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "rpc", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		payload := L.OptString(2, "")
		res, err := nk.RpcCall(L.Context(), id, payload)
		if err != nil {
			L.RaiseError("rpc failed: %v", err)
			return 0
		}
		L.Push(lua.LString(res))
		return 1
	}))
	L.SetField(nkTable, "rpc_call", L.GetField(nkTable, "rpc"))

	if len(registry) > 0 && registry[0] != nil {
		reg := registry[0]
		L.SetField(nkTable, "register_rpc", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			id := strings.ToLower(L.CheckString(2))
			globalName := "__rpc_" + id
			L.SetGlobal(globalName, fn)
			reg.RegisterLuaRPC(id, globalName)
			return 0
		}))
		L.SetField(nkTable, "register_req_before", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			id := L.CheckString(2)
			globalName := "__before_" + id
			L.SetGlobal(globalName, fn)
			reg.RegisterLuaBefore(id, globalName)
			return 0
		}))
		L.SetField(nkTable, "register_req_after", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			id := L.CheckString(2)
			globalName := "__after_" + id
			L.SetGlobal(globalName, fn)
			reg.RegisterLuaAfter(id, globalName)
			return 0
		}))
		L.SetField(nkTable, "register_rt_before", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			id := L.CheckString(2)
			globalName := "__rt_before_" + id
			L.SetGlobal(globalName, fn)
			reg.RegisterLuaBefore(id, globalName)
			return 0
		}))
		L.SetField(nkTable, "register_rt_after", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			id := L.CheckString(2)
			globalName := "__rt_after_" + id
			L.SetGlobal(globalName, fn)
			reg.RegisterLuaAfter(id, globalName)
			return 0
		}))
		L.SetField(nkTable, "register_matchmaker_matched", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			reg.RegisterMatchmakerMatched(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, entries []interface{}) (string, error) {
				L.SetContext(ctx)
				tbl := L.NewTable()
				for i, e := range entries {
					tbl.RawSetInt(i+1, ToLuaValue(L, e))
				}
				if err := L.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true}, tbl); err != nil {
					return "", err
				}
				ret := L.Get(-1)
				L.Pop(1)
				if ret.Type() == lua.LTString {
					return ret.String(), nil
				}
				return "", nil
			})
			return 0
		}))
	}

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

	L.SetField(nkTable, "leaderboard_list", L.NewFunction(func(L *lua.LState) int {
		list, next, err := nk.LeaderboardList(L.Context(), L.OptInt(1, 10), L.OptString(2, ""))
		if err != nil {
			L.RaiseError("leaderboard_list failed: %v", err)
			return 0
		}
		arr := L.NewTable()
		for i, lb := range list {
			tbl := L.NewTable()
			L.SetField(tbl, "id", lua.LString(lb.ID))
			L.RawSetInt(arr, i+1, tbl)
		}
		L.Push(arr)
		L.Push(lua.LString(next))
		return 2
	}))

	L.SetField(nkTable, "leaderboard_ranks_disable", L.NewFunction(func(L *lua.LState) int {
		if err := nk.LeaderboardRanksDisable(L.Context(), L.CheckString(1)); err != nil {
			L.RaiseError("leaderboard_ranks_disable failed: %v", err)
		}
		return 0
	}))

	L.SetField(nkTable, "leaderboard_records_list_cursor_from_rank", L.NewFunction(func(L *lua.LState) int {
		cur, err := nk.LeaderboardRecordsListCursorFromRank(L.Context(), L.CheckString(1), L.CheckInt64(2), L.OptInt64(3, 0))
		if err != nil {
			L.RaiseError("leaderboard_records_list_cursor_from_rank failed: %v", err)
			return 0
		}
		L.Push(lua.LString(cur))
		return 1
	}))

	L.SetField(nkTable, "tournament_record_delete", L.NewFunction(func(L *lua.LState) int {
		if err := nk.TournamentRecordDelete(L.Context(), L.CheckString(1), L.CheckString(2)); err != nil {
			L.RaiseError("tournament_record_delete failed: %v", err)
		}
		return 0
	}))

	L.SetField(nkTable, "tournament_ranks_disable", L.NewFunction(func(L *lua.LState) int {
		if err := nk.TournamentRanksDisable(L.Context(), L.CheckString(1)); err != nil {
			L.RaiseError("tournament_ranks_disable failed: %v", err)
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

	pushRecordList := func(L *lua.LState, records []*LeaderboardRecord, next, prev string) int {
		arr := L.NewTable()
		for i, rec := range records {
			tbl := L.NewTable()
			L.SetField(tbl, "leaderboard_id", lua.LString(rec.LeaderboardID))
			L.SetField(tbl, "owner_id", lua.LString(rec.OwnerID))
			L.SetField(tbl, "username", lua.LString(rec.Username))
			L.SetField(tbl, "score", lua.LNumber(rec.Score))
			L.SetField(tbl, "subscore", lua.LNumber(rec.Subscore))
			L.SetField(tbl, "rank", lua.LNumber(rec.Rank))
			L.SetField(tbl, "metadata", lua.LString(rec.Metadata))
			arr.RawSetInt(i+1, tbl)
		}
		L.Push(arr)
		L.Push(lua.LString(next))
		L.Push(lua.LString(prev))
		return 3
	}

	L.SetField(nkTable, "leaderboard_records_list", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		var ownerIDs []string
		if tbl, ok := L.Get(2).(*lua.LTable); ok {
			tbl.ForEach(func(_, v lua.LValue) { ownerIDs = append(ownerIDs, v.String()) })
		}
		limit := L.OptInt(3, 10)
		cursor := L.OptString(4, "")
		expiry := L.OptInt64(5, 0)
		recs, next, prev, err := nk.LeaderboardRecordsList(L.Context(), id, ownerIDs, limit, cursor, expiry)
		if err != nil {
			L.RaiseError("leaderboard_records_list failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, next, prev)
	}))

	L.SetField(nkTable, "leaderboard_records_around_owner", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		ownerID := L.CheckString(2)
		limit := L.OptInt(3, 10)
		expiry := L.OptInt64(4, 0)
		recs, err := nk.LeaderboardRecordsAroundOwner(L.Context(), id, ownerID, limit, expiry)
		if err != nil {
			L.RaiseError("leaderboard_records_around_owner failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, "", "")
	}))

	L.SetField(nkTable, "leaderboard_records_haystack", L.NewFunction(func(L *lua.LState) int {
		recs, next, prev, err := nk.LeaderboardRecordsHaystack(L.Context(), L.CheckString(1), L.CheckString(2), L.OptInt(3, 10), L.OptString(4, ""), L.OptInt64(5, 0))
		if err != nil {
			L.RaiseError("leaderboard_records_haystack failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, next, prev)
	}))

	L.SetField(nkTable, "tournament_records_haystack", L.NewFunction(func(L *lua.LState) int {
		recs, next, prev, err := nk.TournamentRecordsHaystack(L.Context(), L.CheckString(1), L.CheckString(2), L.OptInt(3, 10), L.OptString(4, ""), L.OptInt64(5, 0))
		if err != nil {
			L.RaiseError("tournament_records_haystack failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, next, prev)
	}))

	L.SetField(nkTable, "leaderboard_record_delete", L.NewFunction(func(L *lua.LState) int {
		if err := nk.LeaderboardRecordDelete(L.Context(), L.CheckString(1), L.CheckString(2)); err != nil {
			L.RaiseError("leaderboard_record_delete failed: %v", err)
			return 0
		}
		return 0
	}))

	L.SetField(nkTable, "tournament_record_write", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		ownerID := L.CheckString(2)
		username := L.CheckString(3)
		score := int64(L.CheckNumber(4))
		subscore := int64(L.OptNumber(5, 0))
		metadataTbl := L.OptTable(6, nil)
		var metadata map[string]interface{}
		if metadataTbl != nil {
			if m, ok := ToGoValue(metadataTbl).(map[string]interface{}); ok {
				metadata = m
			}
		}
		rec, err := nk.TournamentRecordWrite(L.Context(), id, ownerID, username, score, subscore, metadata)
		if err != nil {
			L.RaiseError("tournament_record_write failed: %v", err)
			return 0
		}
		resTbl := L.NewTable()
		L.SetField(resTbl, "leaderboard_id", lua.LString(rec.LeaderboardID))
		L.SetField(resTbl, "owner_id", lua.LString(rec.OwnerID))
		L.SetField(resTbl, "score", lua.LNumber(rec.Score))
		L.SetField(resTbl, "rank", lua.LNumber(rec.Rank))
		L.Push(resTbl)
		return 1
	}))

	L.SetField(nkTable, "tournament_records_list", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		var ownerIDs []string
		if tbl, ok := L.Get(2).(*lua.LTable); ok {
			tbl.ForEach(func(_, v lua.LValue) { ownerIDs = append(ownerIDs, v.String()) })
		}
		limit := L.OptInt(3, 10)
		cursor := L.OptString(4, "")
		expiry := L.OptInt64(5, 0)
		recs, next, prev, err := nk.TournamentRecordsList(L.Context(), id, ownerIDs, limit, cursor, expiry)
		if err != nil {
			L.RaiseError("tournament_records_list failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, next, prev)
	}))

	L.SetField(nkTable, "tournament_records_around_owner", L.NewFunction(func(L *lua.LState) int {
		id := L.CheckString(1)
		ownerID := L.CheckString(2)
		limit := L.OptInt(3, 10)
		expiry := L.OptInt64(4, 0)
		recs, err := nk.TournamentRecordsAroundOwner(L.Context(), id, ownerID, limit, expiry)
		if err != nil {
			L.RaiseError("tournament_records_around_owner failed: %v", err)
			return 0
		}
		return pushRecordList(L, recs, "", "")
	}))

	L.SetField(nkTable, "tournament_add_attempt", L.NewFunction(func(L *lua.LState) int {
		if err := nk.TournamentAddAttempt(L.Context(), L.CheckString(1), L.CheckString(2), L.CheckInt(3)); err != nil {
			L.RaiseError("tournament_add_attempt failed: %v", err)
			return 0
		}
		return 0
	}))

	if len(registry) > 0 && registry[0] != nil {
		reg := registry[0]
		L.SetField(nkTable, "register_leaderboard_reset", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			reg.RegisterLeaderboardReset(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, leaderboardID string, reset int64) error {
				L.SetContext(ctx)
				return L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, lua.LString(leaderboardID), lua.LNumber(reset))
			})
			return 0
		}))
		L.SetField(nkTable, "register_tournament_end", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			reg.RegisterTournamentEnd(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, tournamentID string, end, reset int64) error {
				L.SetContext(ctx)
				return L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, lua.LString(tournamentID), lua.LNumber(end), lua.LNumber(reset))
			})
			return 0
		}))
		L.SetField(nkTable, "register_tournament_reset", L.NewFunction(func(L *lua.LState) int {
			fn := L.CheckFunction(1)
			reg.RegisterTournamentReset(func(ctx context.Context, logger Logger, db *sql.DB, nkMod RuntimeModule, tournamentID string, end, reset int64) error {
				L.SetContext(ctx)
				return L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, lua.LString(tournamentID), lua.LNumber(end), lua.LNumber(reset))
			})
			return 0
		}))
	}

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

	// Translate modified Lua Table back to Go
	goVal := ToGoValue(retVal)
	if _, ok := in.(map[string]interface{}); ok {
		if m, ok := goVal.(map[string]interface{}); ok {
			return m, nil
		}
		goBytes, err := json.Marshal(goVal)
		if err != nil {
			return nil, err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(goBytes, &m); err != nil {
			return nil, err
		}
		return m, nil
	}

	goBytes, err := json.Marshal(goVal)
	if err != nil {
		return nil, err
	}

	// Create a new instance of the same type as in (must be pointer for structs)
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
func luaStringSlice(tbl *lua.LTable) []string {
	if tbl == nil {
		return nil
	}
	var out []string
	tbl.ForEach(func(_, v lua.LValue) {
		if s, ok := v.(lua.LString); ok {
			out = append(out, string(s))
		}
	})
	return out
}

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
