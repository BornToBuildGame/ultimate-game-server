package runtime

import "os"

// RegisterCronEnabled reports whether Optional RegisterCron is enabled.
func RegisterCronEnabled() bool {
	return os.Getenv("UGE_ENABLE_REGISTER_CRON") == "true"
}
