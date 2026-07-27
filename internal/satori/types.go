package satori

// Properties holds default/custom/computed identity properties.
type Properties struct {
	Default  map[string]string `json:"default,omitempty"`
	Custom   map[string]string `json:"custom,omitempty"`
	Computed map[string]string `json:"computed,omitempty"`
}

// PropertiesUpdate updates identity properties.
type PropertiesUpdate struct {
	Default   map[string]string `json:"default,omitempty"`
	Custom    map[string]string `json:"custom,omitempty"`
	Recompute *bool             `json:"recompute,omitempty"`
}

// Event is a Satori analytics event.
type Event struct {
	Name             string            `json:"name,omitempty"`
	Id               string            `json:"id,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Value            string            `json:"value,omitempty"`
	IdentityId       string            `json:"identity_id,omitempty"`
	SessionId        string            `json:"session_id,omitempty"`
	SessionIssuedAt  int64             `json:"session_issued_at,omitempty"`
	SessionExpiresAt int64             `json:"session_expires_at,omitempty"`
	Timestamp        int64             `json:"-"`
}

// ExperimentList wraps experiment entries.
type ExperimentList struct {
	Experiments []*Experiment `json:"experiments,omitempty"`
}

// Experiment is a Satori experiment assignment.
type Experiment struct {
	Name      string   `json:"name,omitempty"`
	Value     string   `json:"value,omitempty"`
	Labels    []string `json:"labels,omitempty"`
	FlagNames []string `json:"flag_names,omitempty"`
}

// FlagList wraps feature flags.
type FlagList struct {
	Flags []*Flag `json:"flags,omitempty"`
}

// Flag is a feature flag value.
type Flag struct {
	Name             string   `json:"name,omitempty"`
	Value            string   `json:"value,omitempty"`
	Labels           []string `json:"labels,omitempty"`
	ConditionChanged bool     `json:"condition_changed,omitempty"`
}

// FlagOverridesList wraps flag override groups.
type FlagOverridesList struct {
	Flags []*FlagOverrides `json:"flags,omitempty"`
}

// FlagOverrides lists overrides for one flag.
type FlagOverrides struct {
	FlagName  string          `json:"flag_name,omitempty"`
	Labels    []string        `json:"labels,omitempty"`
	Overrides []*FlagOverride `json:"overrides,omitempty"`
}

// FlagOverride is a single override entry.
type FlagOverride struct {
	Type          int    `json:"type,omitempty"`
	Name          string `json:"name,omitempty"`
	VariantName   string `json:"variant_name,omitempty"`
	Value         string `json:"value,omitempty"`
	CreateTimeSec int64  `json:"create_time_sec,string,omitempty"`
}

// LiveEventList wraps live events.
type LiveEventList struct {
	LiveEvents []*LiveEvent `json:"live_events,omitempty"`
}

// LiveEvent is a scheduled LiveOps event.
type LiveEvent struct {
	Name               string   `json:"name,omitempty"`
	Description        string   `json:"description,omitempty"`
	Value              string   `json:"value,omitempty"`
	Labels             []string `json:"labels,omitempty"`
	ActiveStartTimeSec int64    `json:"active_start_time_sec,string,omitempty"`
	ActiveEndTimeSec   int64    `json:"active_end_time_sec,string,omitempty"`
	Id                 string   `json:"id,omitempty"`
	StartTimeSec       int64    `json:"start_time_sec,string,omitempty"`
	EndTimeSec         int64    `json:"end_time_sec,string,omitempty"`
	DurationSec        int64    `json:"duration_sec,string,omitempty"`
	ResetCronExpr      string   `json:"reset_cron,omitempty"`
	Status             int      `json:"status,omitempty"`
	FlagNames          []string `json:"flag_names,omitempty"`
}

// MessageList wraps inbox messages.
type MessageList struct {
	Messages        []*Message `json:"messages,omitempty"`
	NextCursor      string     `json:"next_cursor,omitempty"`
	PrevCursor      string     `json:"prev_cursor,omitempty"`
	CacheableCursor string     `json:"cacheable_cursor,omitempty"`
}

// Message is an inbox message.
type Message struct {
	ScheduleId  string         `json:"schedule_id,omitempty"`
	SendTime    int64          `json:"send_time,string,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreateTime  int64          `json:"create_time,string,omitempty"`
	UpdateTime  int64          `json:"update_time,string,omitempty"`
	ReadTime    int64          `json:"read_time,string,omitempty"`
	ConsumeTime int64          `json:"consume_time,string,omitempty"`
	Text        string         `json:"text,omitempty"`
	Id          string         `json:"id,omitempty"`
	Title       string         `json:"title,omitempty"`
	ImageUrl    string         `json:"image_url,omitempty"`
}

// MessageIntegration is a console messaging channel type.
type MessageIntegration int

const (
	MessageIntegrationUnknown MessageIntegration = 0
	MessageIntegrationFCM     MessageIntegration = 1
	MessageIntegrationAPNS    MessageIntegration = 2
)

// MessageIntegrationChannel is push vs email.
type MessageIntegrationChannel int

const (
	MessageIntegrationChannelDefault MessageIntegrationChannel = 0
	MessageIntegrationChannelPush    MessageIntegrationChannel = 1
	MessageIntegrationChannelEmail   MessageIntegrationChannel = 2
)

// MessageIntegrationChannels lists channels for an integration.
type MessageIntegrationChannels struct {
	Channels []MessageIntegrationChannel `json:"channels,omitempty"`
}

// MessageTemplateOverride overrides template content for a send.
type MessageTemplateOverride struct {
	Title        string                               `json:"title,omitempty"`
	Value        string                               `json:"value,omitempty"`
	ImageURL     string                               `json:"image_url,omitempty"`
	JsonMetadata string                               `json:"json_metadata,omitempty"`
	Variants     map[string]*MessageTemplateOverride  `json:"variants,omitempty"`
}

// MessageSendResults wraps delivery results.
type MessageSendResults struct {
	DeliveryResults []*MessageSendResult `json:"delivery_results,omitempty"`
}

// MessageSendResult is per-recipient delivery status.
type MessageSendResult struct {
	RecipientID        string                            `json:"recipient_id,omitempty"`
	IntegrationResults []*MessageSendIntegrationResult   `json:"integration_results,omitempty"`
}

// MessageSendIntegrationResult is per-integration delivery status.
type MessageSendIntegrationResult struct {
	IntegrationType MessageIntegration        `json:"integration_type,omitempty"`
	Success         bool                      `json:"success,omitempty"`
	ErrorMessage    string                    `json:"error_message,omitempty"`
	ChannelType     MessageIntegrationChannel `json:"channel_type,omitempty"`
}
