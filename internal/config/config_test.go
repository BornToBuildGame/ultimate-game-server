package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewConfig(t *testing.T) {
	c := NewConfig()
	if c.GetName() != "ultimate-game-server" {
		t.Errorf("expected name 'ultimate-game-server', got %q", c.GetName())
	}
	if c.GetDatabase().MaxOpenConns != 20 {
		t.Errorf("expected MaxOpenConns 20, got %d", c.GetDatabase().MaxOpenConns)
	}
}

func TestParseYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlData := `
name: "custom-game-server"
database:
  dsn: "postgres://user:pass@host:5432/dbname"
  read_dsn: "postgres://user:pass@replica:5432/dbname"
  max_open_conns: 40
  max_idle_conns: 10
  migration: false
session:
  encryption_key: "yaml-signing-key"
socket:
  http_addr: "127.0.0.1:8080"
  grpc_addr: "127.0.0.1:8081"
console:
  address: "127.0.0.1:8082"
iap:
  apple:
    shared_password: "apple-secret"
`
	if err := os.WriteFile(configPath, []byte(yamlData), 0644); err != nil {
		t.Fatalf("failed to write tmp config: %v", err)
	}

	c, err := Parse([]string{"-config", configPath})
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if c.GetName() != "custom-game-server" {
		t.Errorf("expected name 'custom-game-server', got %q", c.GetName())
	}
	if c.GetDatabase().DSN != "postgres://user:pass@host:5432/dbname" {
		t.Errorf("expected database DSN to match YAML")
	}
	if c.GetDatabase().ReadDSN != "postgres://user:pass@replica:5432/dbname" {
		t.Errorf("expected read DSN replica:5432")
	}
	if c.GetDatabase().MaxOpenConns != 40 {
		t.Errorf("expected MaxOpenConns 40, got %d", c.GetDatabase().MaxOpenConns)
	}
	if c.GetDatabase().Migration != false {
		t.Errorf("expected Migration false")
	}
	if c.GetSession().EncryptionKey != "yaml-signing-key" {
		t.Errorf("expected session encryption_key to match YAML")
	}
	if c.GetSocket().HTTPAddr != "127.0.0.1:8080" {
		t.Errorf("expected http addr 127.0.0.1:8080, got %q", c.GetSocket().HTTPAddr)
	}
	if c.GetSocket().GRPCAddr != "127.0.0.1:8081" {
		t.Errorf("expected grpc addr 127.0.0.1:8081, got %q", c.GetSocket().GRPCAddr)
	}
	if c.GetConsole().Address != "127.0.0.1:8082" {
		t.Errorf("expected console address 127.0.0.1:8082, got %q", c.GetConsole().Address)
	}
	if c.GetIAP().Apple.SharedPassword != "apple-secret" {
		t.Errorf("expected apple shared password to match YAML")
	}
}

func TestParseEnvOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://env-override")
	t.Setenv("DATABASE_MIGRATION", "false")
	t.Setenv("DATABASE_MAX_OPEN_CONNS", "55")
	t.Setenv("JWT_SECRET", "env-jwt-secret")
	t.Setenv("APPLE_SHARED_PASSWORD", "env-apple-secret")

	c, err := Parse([]string{})
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if c.GetDatabase().DSN != "postgres://env-override" {
		t.Errorf("expected database DSN from env")
	}
	if c.GetDatabase().Migration != false {
		t.Errorf("expected database Migration false from env")
	}
	if c.GetDatabase().MaxOpenConns != 55 {
		t.Errorf("expected database MaxOpenConns 55 from env")
	}
	if c.GetSession().EncryptionKey != "env-jwt-secret" {
		t.Errorf("expected jwt secret from env")
	}
	if c.GetIAP().Apple.SharedPassword != "env-apple-secret" {
		t.Errorf("expected apple secret from env")
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	args := []string{
		"-dsn=postgres://flag-override",
		"-http_addr=127.0.0.1:9090",
		"-database.max_open_conns=88",
		"--session.encryption_key=flag-signing-key",
	}

	c, err := Parse(args)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if c.GetDatabase().DSN != "postgres://flag-override" {
		t.Errorf("expected database DSN from flag")
	}
	if c.GetSocket().HTTPAddr != "127.0.0.1:9090" {
		t.Errorf("expected http addr from flag")
	}
	if c.GetDatabase().MaxOpenConns != 88 {
		t.Errorf("expected database max open conns from flag")
	}
	if c.GetSession().EncryptionKey != "flag-signing-key" {
		t.Errorf("expected encryption key from flag")
	}
}
