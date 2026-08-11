// Package config is the public configuration surface for embedding Ultimate Game Engine.
// Implementations live in internal/config; this package re-exports types and constructors
// so external modules can bootstrap without importing internal/.
package config

import (
	internalconfig "github.com/BornToBuildGame/ultimate-game-server/internal/config"
)

// Config is the server configuration interface.
type Config = internalconfig.Config

// Nested configuration structs returned by Config getters.
type (
	DatabaseConfig      = internalconfig.DatabaseConfig
	SessionConfig       = internalconfig.SessionConfig
	SocketConfig        = internalconfig.SocketConfig
	RuntimeConfig       = internalconfig.RuntimeConfig
	ConsoleConfig       = internalconfig.ConsoleConfig
	IAPConfig           = internalconfig.IAPConfig
	IAPAppleConfig      = internalconfig.IAPAppleConfig
	IAPGoogleConfig     = internalconfig.IAPGoogleConfig
	IAPHuaweiConfig     = internalconfig.IAPHuaweiConfig
	IAPFacebookConfig   = internalconfig.IAPFacebookConfig
	IAPSamsungConfig    = internalconfig.IAPSamsungConfig
	MultiInstanceConfig = internalconfig.MultiInstanceConfig
)

// NewConfig returns a configuration initialized with defaults.
func NewConfig() Config {
	return internalconfig.NewConfig()
}

// Parse parses arguments, config files, and env overrides into a Config instance.
func Parse(args []string) (Config, error) {
	return internalconfig.Parse(args)
}
