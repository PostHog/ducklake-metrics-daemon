package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// clearAllDucklakeEnv wipes the env vars Load() touches so test cases
// start from a known baseline. t.Setenv handles restore for any we set
// during the test.
func clearAllDucklakeEnv(t *testing.T) {
	t.Helper()
	vars := []string{
		"DUCKLAKE_TENANT", "DUCKLAKE_RDS_HOST", "DUCKLAKE_RDS_PORT",
		"DUCKLAKE_RDS_DATABASE", "DUCKLAKE_RDS_USERNAME", "DUCKLAKE_RDS_PASSWORD",
		"DUCKLAKE_TENANTS_CONFIG", "DUCKLAKE_METRICS_CONFIG", "DUCKLAKE_METRICS_DISABLE",
		"DUCKLAKE_METRICS_LIVENESS_TIMEOUT", "DUCKLAKE_QUERY_TIMEOUT",
		"DUCKLAKE_TENANT_RESTART_COOLDOWN", "DUCKLAKE_METRICS_PORT",
	}
	for _, v := range vars {
		t.Setenv(v, "")
		os.Unsetenv(v)
	}
}

func TestLoadSingleTenantEnv(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "alpha.example.com")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "alpha")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "alpha")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Tenants) != 1 {
		t.Fatalf("Tenants len=%d, want 1", len(cfg.Tenants))
	}
	a, ok := cfg.Tenants["alpha"]
	if !ok {
		t.Fatal("alpha missing")
	}
	if a.Port != 5432 {
		t.Errorf("Port=%d, want 5432 default", a.Port)
	}
	if cfg.HTTPPort != 9100 {
		t.Errorf("HTTPPort=%d, want 9100 default", cfg.HTTPPort)
	}
	if cfg.LivenessTimeout != 300*time.Second {
		t.Errorf("LivenessTimeout=%v, want 5m default", cfg.LivenessTimeout)
	}
	if cfg.QueryTimeout != 60*time.Second {
		t.Errorf("QueryTimeout=%v, want 60s default", cfg.QueryTimeout)
	}
	if cfg.TenantRestartCooldown != 300*time.Second {
		t.Errorf("TenantRestartCooldown=%v, want 5m default", cfg.TenantRestartCooldown)
	}
}

func TestLoadSingleTenantMissing(t *testing.T) {
	clearAllDucklakeEnv(t)
	// Set tenant but leave RDS_* unset.
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	_, err := Load()
	if err == nil {
		t.Fatal("want error for missing RDS_* envs")
	}
	for _, want := range []string{"DUCKLAKE_RDS_HOST", "DUCKLAKE_RDS_DATABASE", "DUCKLAKE_RDS_USERNAME", "DUCKLAKE_RDS_PASSWORD"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err=%v, want %s in message", err, want)
		}
	}
}

func TestLoadMultiTenantYAML(t *testing.T) {
	clearAllDucklakeEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "tenants.yaml")
	body := `tenants:
  - id: alpha
    host: a.example.com
    database: a
    username: a
    passwordEnv: A_PW
  - id: beta
    host: b.example.com
    database: b
    username: b
    passwordEnv: B_PW
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUCKLAKE_TENANTS_CONFIG", path)
	t.Setenv("A_PW", "alpha-secret")
	t.Setenv("B_PW", "beta-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Tenants) != 2 {
		t.Fatalf("len=%d, want 2", len(cfg.Tenants))
	}
}

func TestLoadYAMLWinsOverEnv(t *testing.T) {
	// When both are set, the YAML path takes precedence and the env
	// vars are silently ignored. Pin that contract.
	clearAllDucklakeEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "tenants.yaml")
	body := `tenants:
  - id: yaml-only
    host: y.example.com
    database: y
    username: y
    passwordEnv: Y_PW
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUCKLAKE_TENANTS_CONFIG", path)
	t.Setenv("Y_PW", "x")
	t.Setenv("DUCKLAKE_TENANT", "env-only")
	t.Setenv("DUCKLAKE_RDS_HOST", "e.example.com")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "e")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "e")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := cfg.Tenants["yaml-only"]; !ok {
		t.Error("yaml-only missing — YAML path didn't win")
	}
	if _, ok := cfg.Tenants["env-only"]; ok {
		t.Error("env-only present — env path ran when it shouldn't have")
	}
}

func TestLoadCustomTimeouts(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	t.Setenv("DUCKLAKE_CYCLE_INTERVAL", "120")
	t.Setenv("DUCKLAKE_QUERY_TIMEOUT", "30")
	t.Setenv("DUCKLAKE_TENANT_RESTART_COOLDOWN", "60")
	t.Setenv("DUCKLAKE_MAX_QUERY_ROWS", "5000")
	t.Setenv("DUCKLAKE_METRICS_PORT", "9999")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.CycleInterval != 120*time.Second {
		t.Errorf("CycleInterval=%v, want 120s", cfg.CycleInterval)
	}
	if cfg.QueryTimeout != 30*time.Second {
		t.Errorf("QueryTimeout=%v, want 30s", cfg.QueryTimeout)
	}
	if cfg.TenantRestartCooldown != 60*time.Second {
		t.Errorf("TenantRestartCooldown=%v, want 60s", cfg.TenantRestartCooldown)
	}
	if cfg.MaxQueryRows != 5000 {
		t.Errorf("MaxQueryRows=%d, want 5000", cfg.MaxQueryRows)
	}
	if cfg.HTTPPort != 9999 {
		t.Errorf("HTTPPort=%d, want 9999", cfg.HTTPPort)
	}
}

func TestLoadDefaultMaxQueryRows(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxQueryRows != 10000 {
		t.Errorf("MaxQueryRows=%d, want default 10000", cfg.MaxQueryRows)
	}
}

func TestLoadQueryTimeoutMustBeLessThanCycleInterval(t *testing.T) {
	// A single query taking its full QueryTimeout would consume the
	// entire cycle window; the next ticker would kill the cycle every
	// interval. Catch the misconfig at startup.
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	// Default CycleInterval=300; set QueryTimeout=300 (equal — should fail).
	t.Setenv("DUCKLAKE_QUERY_TIMEOUT", "300")

	_, err := Load()
	if err == nil {
		t.Fatal("want error for QueryTimeout >= CycleInterval")
	}
	if !strings.Contains(err.Error(), "QUERY_TIMEOUT") {
		t.Errorf("err=%v, want QUERY_TIMEOUT in message", err)
	}
}

func TestLoadQueryTimeoutGreaterThanCycleInterval(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	t.Setenv("DUCKLAKE_QUERY_TIMEOUT", "600")
	t.Setenv("DUCKLAKE_CYCLE_INTERVAL", "300")

	_, err := Load()
	if err == nil {
		t.Fatal("want error")
	}
}

func TestLoadCycleIntervalDefault(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CycleInterval != 300*time.Second {
		t.Errorf("CycleInterval=%v, want 5m default", cfg.CycleInterval)
	}
}

func TestLoadDisabledQueriesParsed(t *testing.T) {
	clearAllDucklakeEnv(t)
	t.Setenv("DUCKLAKE_TENANT", "alpha")
	t.Setenv("DUCKLAKE_RDS_HOST", "a")
	t.Setenv("DUCKLAKE_RDS_DATABASE", "a")
	t.Setenv("DUCKLAKE_RDS_USERNAME", "a")
	t.Setenv("DUCKLAKE_RDS_PASSWORD", "x")
	t.Setenv("DUCKLAKE_METRICS_DISABLE", "foo, bar ,baz")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, want := range []string{"foo", "bar", "baz"} {
		if !cfg.DisabledQueries[want] {
			t.Errorf("disabled set missing %q (got %v)", want, cfg.DisabledQueries)
		}
	}
}

func TestParseSecondsInvalid(t *testing.T) {
	t.Setenv("FOO_SEC", "not-an-int")
	got := parseSeconds("FOO_SEC", 42*time.Second)
	if got != 42*time.Second {
		t.Errorf("got %v, want fall-back to default 42s", got)
	}
}

func TestParseSecondsZeroOrNeg(t *testing.T) {
	t.Setenv("FOO_SEC", "0")
	got := parseSeconds("FOO_SEC", 42*time.Second)
	if got != 42*time.Second {
		t.Errorf("0 should fall back to default; got %v", got)
	}
}
