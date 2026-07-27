package fleet

import "time"

// InstanceInfo describes a dedicated game-server instance.
type InstanceInfo struct {
	Id             string                 `json:"id"`
	ConnectionInfo *ConnectionInfo        `json:"connection_info"`
	CreateTime     time.Time              `json:"create_time"`
	PlayerCount    int                    `json:"player_count"`
	Status         string                 `json:"status"`
	Metadata       map[string]interface{} `json:"metadata"`
}

// ConnectionInfo is platform-specific connection data.
type ConnectionInfo struct {
	IpAddress string `json:"ip_address"`
	DnsName   string `json:"dns_name"`
	Port      int    `json:"port"`
}

// JoinInfo is returned from Join.
type JoinInfo struct {
	InstanceInfo *InstanceInfo  `json:"instance_info"`
	SessionInfo  []*SessionInfo `json:"session_info"`
}

// SessionInfo reserves a player slot on an instance.
type SessionInfo struct {
	UserId    string `json:"user_id"`
	SessionId string `json:"session_id"`
}

// FleetUserLatencies is optional latency hint for Create placement.
type FleetUserLatencies struct {
	UserId                string
	LatencyInMilliseconds float32
	RegionIdentifier      string
}

// FmCreateStatus reports Create outcome.
type FmCreateStatus int

const (
	CreateSuccess FmCreateStatus = iota
	CreateTimeout
	CreateError
)

// FmCreateCallbackFn is invoked when async Create completes.
type FmCreateCallbackFn func(status FmCreateStatus, instanceInfo *InstanceInfo, sessionInfo []*SessionInfo, metadata map[string]any, err error)

// FmCallbackHandler stores and invokes Create callbacks by id.
type FmCallbackHandler interface {
	GenerateCallbackId() string
	SetCallback(callbackId string, fn FmCreateCallbackFn)
	InvokeCallback(callbackId string, status FmCreateStatus, instanceInfo *InstanceInfo, sessionInfo []*SessionInfo, metadata map[string]any, err error)
}
