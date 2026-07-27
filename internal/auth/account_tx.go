package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AccountUpdateParams is a single account mutation for MultiUpdate.
type AccountUpdateParams struct {
	UserID      string
	Username    *string
	DisplayName *string
	AvatarURL   *string
	LangTag     *string
	Location    *string
	Timezone    *string
	Metadata    *string
}

// UpdateAccountsTx applies account profile updates inside a transaction.
func UpdateAccountsTx(ctx context.Context, tx pgx.Tx, updates []AccountUpdateParams) error {
	for _, upd := range updates {
		userID, err := uuid.Parse(upd.UserID)
		if err != nil {
			return errors.New("invalid user id")
		}
		username := upd.Username
		if username != nil {
			u := strings.TrimSpace(*username)
			if len(u) < 3 || len(u) > 64 {
				return errors.New("username must be between 3 and 64 characters")
			}
			username = &u
		}
		if upd.Metadata != nil {
			var js json.RawMessage
			if err := json.Unmarshal([]byte(*upd.Metadata), &js); err != nil {
				return errors.New("metadata must be valid JSON")
			}
		}
		query := `
			UPDATE users SET
				username = COALESCE($2, username),
				display_name = COALESCE($3, display_name),
				avatar_url = COALESCE($4, avatar_url),
				lang_tag = COALESCE($5, lang_tag),
				location = COALESCE($6, location),
				timezone = COALESCE($7, timezone),
				metadata = COALESCE($8::jsonb, metadata),
				update_time = now()
			WHERE id = $1`
		_, err = tx.Exec(ctx, query, userID, username, upd.DisplayName, upd.AvatarURL,
			upd.LangTag, upd.Location, upd.Timezone, upd.Metadata)
		if err != nil {
			return err
		}
	}
	return nil
}
