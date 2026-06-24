// Package config loads daemon configuration from env vars (single-tenant)
// or a YAML tenants inventory (multi-tenant). The env-var path is the
// historical single-tenant shape from millpond's Python daemon
// (DUCKLAKE_RDS_*, DUCKLAKE_TENANT); the YAML path is the multi-tenant
// inventory loaded from DUCKLAKE_TENANTS_CONFIG.
//
// Selection: if DUCKLAKE_TENANTS_CONFIG is set, the YAML path wins and
// the DUCKLAKE_RDS_* env vars are ignored. If it's unset, the env path
// is required (the daemon needs at least one tenant to do anything
// useful — it refuses to start without one).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
)

// Config is the loaded daemon config. Tenants holds one catalog.Conn per
// tenant ID; in the env-fallback path it has exactly one entry keyed by
// DUCKLAKE_TENANT.
type Config struct {
	Tenants         map[string]catalog.Conn // tenant id → connection details
	HTTPPort        int
	UserQueriesPath string          // DUCKLAKE_METRICS_CONFIG; "" disables
	DisabledQueries map[string]bool // DUCKLAKE_METRICS_DISABLE, comma-separated names
	// CycleInterval is how often the per-tenant scheduler fires a new
	// cycle goroutine. Every cycle: all queries run serially against
	// the tenant's catalog + a final pool-stat emission. Default 300s
	// (5 min); configurable via DUCKLAKE_CYCLE_INTERVAL.
	//
	// /-/healthy uses 2×CycleInterval as the scheduler-staleness
	// threshold, so a single missed tick is tolerated; two missed
	// ticks in a row trip the probe.
	CycleInterval   time.Duration
	LivenessTimeout time.Duration // (legacy, unused — kept for env-var back-compat; remove next refactor)
	QueryTimeout    time.Duration // DUCKLAKE_QUERY_TIMEOUT, seconds; default 60
	// TenantRestartCooldown is how long a tenant goroutine waits after
	// its connect/run cycle exits (panic, runner exit, runner.New
	// failure) before re-entering. Defaults to 5 min; configurable via
	// DUCKLAKE_TENANT_RESTART_COOLDOWN (seconds). connectWithBackoff
	// already loops forever on transient connect failures, so this
	// cooldown only fires after a "the cycle gave up" event.
	TenantRestartCooldown time.Duration
	// MaxQueryRows is a per-query result-set ceiling. The catalog
	// fails the query (and the runner bumps query_errors_total) past
	// this row count rather than materialize an unbounded result set.
	// Default 10000 — large enough for ducklake_files_per_partition_top20
	// + the worst-case ducklake_config rollout, small enough to protect
	// the pod from a runaway operator YAML. Configurable via
	// DUCKLAKE_MAX_QUERY_ROWS; 0 disables the cap.
	MaxQueryRows int
}

// Load reads env (and optionally a tenants YAML file) and returns a
// validated Config. Errors loudly on missing required values so the
// daemon doesn't crash-loop trying to connect to "" instead of telling
// the operator what's missing.
func Load() (Config, error) {
	c := Config{
		UserQueriesPath:       os.Getenv("DUCKLAKE_METRICS_CONFIG"),
		DisabledQueries:       parseCSVSet(os.Getenv("DUCKLAKE_METRICS_DISABLE")),
		CycleInterval:         parseSeconds("DUCKLAKE_CYCLE_INTERVAL", 300*time.Second),
		LivenessTimeout:       parseSeconds("DUCKLAKE_METRICS_LIVENESS_TIMEOUT", 300*time.Second),
		QueryTimeout:          parseSeconds("DUCKLAKE_QUERY_TIMEOUT", 60*time.Second),
		TenantRestartCooldown: parseSeconds("DUCKLAKE_TENANT_RESTART_COOLDOWN", 300*time.Second),
		MaxQueryRows:          parseInt("DUCKLAKE_MAX_QUERY_ROWS", 10000),
		HTTPPort:              parseInt("DUCKLAKE_METRICS_PORT", 9100),
	}

	// Multi-tenant path: a YAML file lists N tenants, each with a
	// passwordEnv referencing where the password lives in the process
	// env (typically populated by a K8s Secret mount).
	tenantsPath := os.Getenv("DUCKLAKE_TENANTS_CONFIG")
	if tenantsPath != "" {
		tenants, err := LoadTenants(tenantsPath)
		if err != nil {
			return Config{}, err
		}
		c.Tenants = tenants
	} else {
		// Single-tenant fallback for local dev / smoke testing — the
		// historical millpond Python-daemon shape. Exactly one entry,
		// keyed by DUCKLAKE_TENANT.
		t, err := loadSingleTenant()
		if err != nil {
			return Config{}, err
		}
		c.Tenants = map[string]catalog.Conn{t.id: t.conn}
	}

	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

type singleTenant struct {
	id   string
	conn catalog.Conn
}

// loadSingleTenant reads the DUCKLAKE_TENANT + DUCKLAKE_RDS_* env vars
// the Python daemon used. Errors with the full list of missing vars so
// operators don't have to fix one-at-a-time.
func loadSingleTenant() (singleTenant, error) {
	t := singleTenant{
		id: os.Getenv("DUCKLAKE_TENANT"),
		conn: catalog.Conn{
			Host:     os.Getenv("DUCKLAKE_RDS_HOST"),
			Port:     parseInt("DUCKLAKE_RDS_PORT", 5432),
			Database: os.Getenv("DUCKLAKE_RDS_DATABASE"),
			Username: os.Getenv("DUCKLAKE_RDS_USERNAME"),
			Password: os.Getenv("DUCKLAKE_RDS_PASSWORD"),
		},
	}
	var missing []string
	if t.id == "" {
		missing = append(missing, "DUCKLAKE_TENANT")
	}
	if t.conn.Host == "" {
		missing = append(missing, "DUCKLAKE_RDS_HOST")
	}
	if t.conn.Database == "" {
		missing = append(missing, "DUCKLAKE_RDS_DATABASE")
	}
	if t.conn.Username == "" {
		missing = append(missing, "DUCKLAKE_RDS_USERNAME")
	}
	if t.conn.Password == "" {
		missing = append(missing, "DUCKLAKE_RDS_PASSWORD")
	}
	if len(missing) > 0 {
		return singleTenant{}, fmt.Errorf("missing required env vars (or set DUCKLAKE_TENANTS_CONFIG to use the multi-tenant YAML path): %s", strings.Join(missing, ", "))
	}
	return t, nil
}

func (c Config) validate() error {
	if len(c.Tenants) == 0 {
		return errors.New("no tenants configured")
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return errors.New("DUCKLAKE_METRICS_PORT out of range")
	}
	// QueryTimeout must be strictly less than LivenessTimeout. Otherwise
	// a query that takes its full QueryTimeout to fail would also trip
	// the /-/healthy probe (the runner's currentQueryStart is set for
	// the query's full duration), causing kubelet to restart the pod on
	// every slow-but-not-wedged query. The Python daemon had the same
	// constraint as an unwritten convention; we enforce it here.
	if c.QueryTimeout >= c.CycleInterval {
		return fmt.Errorf("DUCKLAKE_QUERY_TIMEOUT (%v) must be < DUCKLAKE_CYCLE_INTERVAL (%v); otherwise a single slow query fills the entire cycle window and overruns get killed every interval", c.QueryTimeout, c.CycleInterval)
	}
	if c.MaxQueryRows < 0 {
		return errors.New("DUCKLAKE_MAX_QUERY_ROWS must be >= 0 (0 disables the cap)")
	}
	return nil
}

func parseCSVSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func parseSeconds(envvar string, def time.Duration) time.Duration {
	v := os.Getenv(envvar)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	return time.Duration(n) * time.Second
}

func parseInt(envvar string, def int) int {
	v := os.Getenv(envvar)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
