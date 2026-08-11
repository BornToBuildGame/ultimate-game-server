// Package fleet is the public FleetManager surface for embedding Ultimate Game Engine.
package fleet

import (
	internalfleet "github.com/BornToBuildGame/ultimate-game-server/internal/fleet"
)

type (
	Manager            = internalfleet.Manager
	Initializer        = internalfleet.Initializer
	InstanceInfo       = internalfleet.InstanceInfo
	ConnectionInfo     = internalfleet.ConnectionInfo
	JoinInfo           = internalfleet.JoinInfo
	SessionInfo        = internalfleet.SessionInfo
	FleetUserLatencies = internalfleet.FleetUserLatencies
	FmCreateStatus     = internalfleet.FmCreateStatus
	FmCreateCallbackFn = internalfleet.FmCreateCallbackFn
	FmCallbackHandler  = internalfleet.FmCallbackHandler
	LocalStub          = internalfleet.LocalStub
)

const (
	CreateSuccess = internalfleet.CreateSuccess
	CreateTimeout = internalfleet.CreateTimeout
	CreateError   = internalfleet.CreateError
)

// NewLocalStub creates an empty in-process fleet stub.
func NewLocalStub() *LocalStub {
	return internalfleet.NewLocalStub()
}

// NewLocalFmCallbackHandler creates the default in-process callback handler.
func NewLocalFmCallbackHandler() FmCallbackHandler {
	return internalfleet.NewLocalFmCallbackHandler()
}
