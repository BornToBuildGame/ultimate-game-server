package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config interface {
	GetName() string
	GetDatabase() *DatabaseConfig
	GetSession() *SessionConfig
	GetSocket() *SocketConfig
	GetRuntime() *RuntimeConfig
	GetConsole() *ConsoleConfig
	GetIAP() *IAPConfig
}

type DatabaseConfig struct {
	DSN             string        `yaml:"dsn" json:"dsn"`
	ReadDSN         string        `yaml:"read_dsn" json:"read_dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns" json:"max_idle_conns"`
	MaxConnLifetime time.Duration `yaml:"max_conn_lifetime" json:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `yaml:"max_conn_idle_time" json:"max_conn_idle_time"`
	Migration       bool          `yaml:"migration" json:"migration"`
}

type SessionConfig struct {
	EncryptionKey string        `yaml:"encryption_key" json:"encryption_key"`
	TokenExpiryMs int64         `yaml:"token_expiry_ms" json:"token_expiry_ms"`
}

type SocketConfig struct {
	HTTPAddr string `yaml:"http_addr" json:"http_addr"`
	GRPCAddr string `yaml:"grpc_addr" json:"grpc_addr"`
}

type RuntimeConfig struct {
	Path    string `yaml:"path" json:"path"`
	HTTPKey string `yaml:"http_key" json:"http_key"`
}

type ConsoleConfig struct {
	Address string `yaml:"address" json:"address"`
}

type IAPAppleConfig struct {
	SharedPassword           string `yaml:"shared_password" json:"shared_password"`
	NotificationsEndpointID string `yaml:"notifications_endpoint_id" json:"notifications_endpoint_id"`
}

type IAPGoogleConfig struct {
	ClientEmail             string `yaml:"client_email" json:"client_email"`
	PrivateKey              string `yaml:"private_key" json:"private_key"`
	PackageName             string `yaml:"package_name" json:"package_name"`
	NotificationsEndpointID string `yaml:"notifications_endpoint_id" json:"notifications_endpoint_id"`
}

type IAPHuaweiConfig struct {
	PublicKey    string `yaml:"public_key" json:"public_key"`
	ClientID     string `yaml:"client_id" json:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret"`
}

type IAPFacebookConfig struct {
	AppSecret string `yaml:"app_secret" json:"app_secret"`
}

type IAPSamsungConfig struct {
	PackageName string `yaml:"package_name" json:"package_name"`
}

type IAPConfig struct {
	Apple           IAPAppleConfig    `yaml:"apple" json:"apple"`
	Google          IAPGoogleConfig   `yaml:"google" json:"google"`
	Huawei          IAPHuaweiConfig   `yaml:"huawei" json:"huawei"`
	FacebookInstant IAPFacebookConfig `yaml:"facebook_instant" json:"facebook_instant"`
	Samsung         IAPSamsungConfig  `yaml:"samsung" json:"samsung"`
}

type configImpl struct {
	Name     string          `yaml:"name" json:"name"`
	Database DatabaseConfig  `yaml:"database" json:"database"`
	Session  SessionConfig   `yaml:"session" json:"session"`
	Socket   SocketConfig    `yaml:"socket" json:"socket"`
	Runtime  RuntimeConfig   `yaml:"runtime" json:"runtime"`
	Console  ConsoleConfig   `yaml:"console" json:"console"`
	IAP      IAPConfig       `yaml:"iap" json:"iap"`
}

func (c *configImpl) GetName() string                { return c.Name }
func (c *configImpl) GetDatabase() *DatabaseConfig   { return &c.Database }
func (c *configImpl) GetSession() *SessionConfig     { return &c.Session }
func (c *configImpl) GetSocket() *SocketConfig       { return &c.Socket }
func (c *configImpl) GetRuntime() *RuntimeConfig     { return &c.Runtime }
func (c *configImpl) GetConsole() *ConsoleConfig     { return &c.Console }
func (c *configImpl) GetIAP() *IAPConfig             { return &c.IAP }

// NewConfig returns a configuration initialized with defaults.
func NewConfig() Config {
	return &configImpl{
		Name: "ultimate-game-server",
		Database: DatabaseConfig{
			DSN:             "postgres://game_admin:game_password@localhost:5432/ultimate_game_db?sslmode=disable",
			MaxOpenConns:    20,
			MaxIdleConns:    5,
			MaxConnLifetime: 30 * time.Minute,
			MaxConnIdleTime: 10 * time.Minute,
			Migration:       true,
		},
		Session: SessionConfig{
			EncryptionKey: "super_secret_signing_key_at_least_32_bytes_long_1234567",
			TokenExpiryMs: 86400000, // 24 hours
		},
		Socket: SocketConfig{
			HTTPAddr: "0.0.0.0:7350",
			GRPCAddr: "0.0.0.0:7349",
		},
		Runtime: RuntimeConfig{
			Path: "data/modules",
		},
		Console: ConsoleConfig{
			Address: "0.0.0.0:7351",
		},
	}
}

// Parse parses arguments, config files, and env overrides into a Config instance.
func Parse(args []string) (Config, error) {
	c := NewConfig().(*configImpl)

	// 1. First Pass: Detect config file flag (either -config or --config)
	var configPath string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-config" || arg == "--config" {
			if i+1 < len(args) {
				configPath = args[i+1]
				break
			}
		} else if strings.HasPrefix(arg, "-config=") {
			configPath = strings.TrimPrefix(arg, "-config=")
			break
		} else if strings.HasPrefix(arg, "--config=") {
			configPath = strings.TrimPrefix(arg, "--config=")
			break
		}
	}

	if configPath == "" {
		configPath = os.Getenv("CONFIG")
	}

	// 2. Load YAML Config File if present
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %q: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("failed to parse yaml config file %q: %w", configPath, err)
		}
	}

	// 3. Apply Environment Variable Overrides
	applyEnvOverrides(c)

	// 4. Apply CLI Flag Overrides
	if err := applyFlagOverrides(c, args); err != nil {
		return nil, err
	}

	return c, nil
}

func applyEnvOverrides(c *configImpl) {
	if val := os.Getenv("DATABASE_URL"); val != "" {
		c.Database.DSN = val
	}
	if val := os.Getenv("DATABASE_READ_URL"); val != "" {
		c.Database.ReadDSN = val
	}
	if val := os.Getenv("DATABASE_MIGRATION"); val != "" {
		c.Database.Migration = parseBool(val, c.Database.Migration)
	}
	if val := os.Getenv("DATABASE_MAX_OPEN_CONNS"); val != "" {
		c.Database.MaxOpenConns = parseInt(val, c.Database.MaxOpenConns)
	}
	if val := os.Getenv("DATABASE_MAX_IDLE_CONNS"); val != "" {
		c.Database.MaxIdleConns = parseInt(val, c.Database.MaxIdleConns)
	}
	if val := os.Getenv("DATABASE_CONN_MAX_LIFETIME"); val != "" {
		c.Database.MaxConnLifetime = parseDuration(val, c.Database.MaxConnLifetime)
	}
	if val := os.Getenv("DATABASE_CONN_MAX_IDLE_TIME"); val != "" {
		c.Database.MaxConnIdleTime = parseDuration(val, c.Database.MaxConnIdleTime)
	}
	if val := os.Getenv("JWT_SECRET"); val != "" {
		c.Session.EncryptionKey = val
	}
	if val := os.Getenv("HTTP_ADDR"); val != "" {
		c.Socket.HTTPAddr = val
	}
	if val := os.Getenv("GRPC_ADDR"); val != "" {
		c.Socket.GRPCAddr = val
	}
	if val := os.Getenv("CONSOLE_ADDR"); val != "" {
		c.Console.Address = val
	}
	if val := os.Getenv("UGE_RUNTIME_PATH"); val != "" {
		c.Runtime.Path = val
	}
	if val := os.Getenv("UGE_RPC_HTTP_KEY"); val != "" {
		c.Runtime.HTTPKey = val
	}

	// Apple IAP
	if val := os.Getenv("APPLE_SHARED_PASSWORD"); val != "" {
		c.IAP.Apple.SharedPassword = val
	}
	if val := firstEnv("IAP_APPLE_NOTIFICATIONS_ENDPOINT_ID", "APPLE_NOTIFICATIONS_ENDPOINT_ID"); val != "" {
		c.IAP.Apple.NotificationsEndpointID = val
	}

	// Google IAP
	if val := os.Getenv("GOOGLE_IAP_CLIENT_EMAIL"); val != "" {
		c.IAP.Google.ClientEmail = val
	}
	if val := os.Getenv("GOOGLE_IAP_PRIVATE_KEY"); val != "" {
		c.IAP.Google.PrivateKey = val
	}
	if val := os.Getenv("GOOGLE_IAP_PACKAGE_NAME"); val != "" {
		c.IAP.Google.PackageName = val
	}
	if val := firstEnv("IAP_GOOGLE_NOTIFICATIONS_ENDPOINT_ID", "GOOGLE_NOTIFICATIONS_ENDPOINT_ID"); val != "" {
		c.IAP.Google.NotificationsEndpointID = val
	}

	// Huawei IAP
	if val := os.Getenv("HUAWEI_IAP_PUBLIC_KEY"); val != "" {
		c.IAP.Huawei.PublicKey = val
	}
	if val := os.Getenv("HUAWEI_CLIENT_ID"); val != "" {
		c.IAP.Huawei.ClientID = val
	}
	if val := os.Getenv("HUAWEI_CLIENT_SECRET"); val != "" {
		c.IAP.Huawei.ClientSecret = val
	}

	// Facebook IAP
	if val := os.Getenv("FACEBOOK_APP_SECRET"); val != "" {
		c.IAP.FacebookInstant.AppSecret = val
	}

	// Samsung IAP
	if val := os.Getenv("SAMSUNG_PACKAGE_NAME"); val != "" {
		c.IAP.Samsung.PackageName = val
	}
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func applyFlagOverrides(c *configImpl, args []string) error {
	fs := flag.NewFlagSet("config", flag.ContinueOnError)

	// Define flags supporting both standard flat names and hierarchical names
	var (
		httpAddr            = fs.String("http_addr", "", "")
		grpcAddr            = fs.String("grpc_addr", "", "")
		dsn                 = fs.String("dsn", "", "")
		readDsn             = fs.String("read_dsn", "", "")
		jwtSecret           = fs.String("jwt_secret", "", "")
		runtimePath         = fs.String("runtime_path", "", "")
		rpcHTTPKey          = fs.String("rpc_http_key", "", "")
		dbMigration         = fs.String("database_migration", "", "")
		dbMaxOpen           = fs.Int("database_max_open_conns", 0, "")
		dbMaxIdle           = fs.Int("database_max_idle_conns", 0, "")
		dbConnLifetime      = fs.Duration("database_conn_max_lifetime", 0, "")
		dbConnIdle          = fs.Duration("database_conn_max_idle_time", 0, "")
		consoleAddr         = fs.String("console_addr", "", "")
		name                = fs.String("name", "", "")

		// Hierarchical bindings
		dbDsnHdr     = fs.String("database.dsn", "", "")
		dbReadDsnHdr = fs.String("database.read_dsn", "", "")
		dbMaxOpenHdr = fs.Int("database.max_open_conns", 0, "")
		dbMaxIdleHdr = fs.Int("database.max_idle_conns", 0, "")
		sessKeyHdr   = fs.String("session.encryption_key", "", "")
		sockHTTPHdr  = fs.String("socket.http_addr", "", "")
		sockGRPCHdr  = fs.String("socket.grpc_addr", "", "")
		rtPathHdr    = fs.String("runtime.path", "", "")
		rtHTTPKeyHdr = fs.String("runtime.http_key", "", "")
		consAddrHdr  = fs.String("console.address", "", "")

		// To prevent flag.Parse from logging help / config flags
		_ = fs.String("config", "", "")
	)

	// Filter flags to only parse defined flags and ignore unknown ones (like other tools' test flags)
	var filteredArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			name := strings.TrimLeft(strings.Split(arg, "=")[0], "-")
			if fs.Lookup(name) != nil {
				filteredArgs = append(filteredArgs, arg)
				// If not containing '=', pull next arg if it doesn't start with '-'
				if !strings.Contains(arg, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
					filteredArgs = append(filteredArgs, args[i+1])
					i++
				}
			}
		}
	}

	if err := fs.Parse(filteredArgs); err != nil {
		return err
	}

	// Apply flat and hierarchical flags
	if *name != "" {
		c.Name = *name
	}
	if *httpAddr != "" {
		c.Socket.HTTPAddr = *httpAddr
	}
	if *sockHTTPHdr != "" {
		c.Socket.HTTPAddr = *sockHTTPHdr
	}
	if *grpcAddr != "" {
		c.Socket.GRPCAddr = *grpcAddr
	}
	if *sockGRPCHdr != "" {
		c.Socket.GRPCAddr = *sockGRPCHdr
	}
	if *dsn != "" {
		c.Database.DSN = *dsn
	}
	if *dbDsnHdr != "" {
		c.Database.DSN = *dbDsnHdr
	}
	if *readDsn != "" {
		c.Database.ReadDSN = *readDsn
	}
	if *dbReadDsnHdr != "" {
		c.Database.ReadDSN = *dbReadDsnHdr
	}
	if *jwtSecret != "" {
		c.Session.EncryptionKey = *jwtSecret
	}
	if *sessKeyHdr != "" {
		c.Session.EncryptionKey = *sessKeyHdr
	}
	if *runtimePath != "" {
		c.Runtime.Path = *runtimePath
	}
	if *rtPathHdr != "" {
		c.Runtime.Path = *rtPathHdr
	}
	if *rpcHTTPKey != "" {
		c.Runtime.HTTPKey = *rpcHTTPKey
	}
	if *rtHTTPKeyHdr != "" {
		c.Runtime.HTTPKey = *rtHTTPKeyHdr
	}
	if *consoleAddr != "" {
		c.Console.Address = *consoleAddr
	}
	if *consAddrHdr != "" {
		c.Console.Address = *consAddrHdr
	}

	if *dbMaxOpen > 0 {
		c.Database.MaxOpenConns = *dbMaxOpen
	}
	if *dbMaxOpenHdr > 0 {
		c.Database.MaxOpenConns = *dbMaxOpenHdr
	}
	if *dbMaxIdle > 0 {
		c.Database.MaxIdleConns = *dbMaxIdle
	}
	if *dbMaxIdleHdr > 0 {
		c.Database.MaxIdleConns = *dbMaxIdleHdr
	}
	if *dbConnLifetime > 0 {
		c.Database.MaxConnLifetime = *dbConnLifetime
	}
	if *dbConnIdle > 0 {
		c.Database.MaxConnIdleTime = *dbConnIdle
	}

	if *dbMigration != "" {
		c.Database.Migration = parseBool(*dbMigration, c.Database.Migration)
	}

	return nil
}

func parseBool(v string, defaultValue bool) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func parseInt(v string, defaultValue int) int {
	if i, err := strconv.Atoi(v); err == nil {
		return i
	}
	return defaultValue
}

func parseDuration(v string, defaultValue time.Duration) time.Duration {
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return defaultValue
}
