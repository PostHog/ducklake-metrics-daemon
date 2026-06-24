package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
	"gopkg.in/yaml.v3"
)

// Tenant is one row in the daemon's tenant inventory. id is the value
// stamped on every Prometheus sample's `tenant` label (so dashboards
// distinguish tenants by it); passwordEnv names the env var the daemon
// reads at startup to populate catalog.Conn.Password — kept out of the
// YAML so the file can sit on disk un-encrypted and operators only have
// to manage secrets via K8s Secrets / ESO / etc.
type Tenant struct {
	ID          string `yaml:"id"`
	Host        string `yaml:"host"`
	Port        int    `yaml:"port,omitempty"` // default 5432
	Database    string `yaml:"database"`
	Username    string `yaml:"username"`
	PasswordEnv string `yaml:"passwordEnv"`
}

// tenantsDoc is the top-level YAML shape for the tenants config file.
type tenantsDoc struct {
	Tenants []Tenant `yaml:"tenants"`
}

// tenantIDRE constrains tenant IDs to DNS-1035 + underscores so they're
// safe everywhere they land (Prometheus label values, log fields,
// env-var lookup is loose on this but match the K8s-resource-name shape
// for future compatibility with per-tenant resources).
var tenantIDRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// LoadTenants reads a YAML tenants inventory and returns one
// catalog.Conn per tenant, keyed by tenant ID. Passwords are pulled
// from os.Getenv at load time so a tenant whose PasswordEnv is set but
// empty fails loud rather than silently connecting with an empty
// password.
//
// Returns nil + nil error when path is "" so the caller can use this
// unconditionally and fall through to the single-tenant env path.
func LoadTenants(path string) (map[string]catalog.Conn, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var doc tenantsDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%s: parse: %w", path, err)
	}
	if len(doc.Tenants) == 0 {
		return nil, fmt.Errorf("%s: tenants must be non-empty", path)
	}
	out := make(map[string]catalog.Conn, len(doc.Tenants))
	for i, t := range doc.Tenants {
		if err := validateTenant(t); err != nil {
			return nil, fmt.Errorf("%s: tenants[%d] (%q): %w", path, i, t.ID, err)
		}
		if _, dup := out[t.ID]; dup {
			return nil, fmt.Errorf("%s: tenants[%d]: duplicate id %q", path, i, t.ID)
		}
		password := os.Getenv(t.PasswordEnv)
		if password == "" {
			return nil, fmt.Errorf("%s: tenants[%d] (%q): env %s not set or empty", path, i, t.ID, t.PasswordEnv)
		}
		port := t.Port
		if port == 0 {
			port = 5432
		}
		out[t.ID] = catalog.Conn{
			Host:     t.Host,
			Port:     port,
			Database: t.Database,
			Username: t.Username,
			Password: password,
		}
	}
	return out, nil
}

func validateTenant(t Tenant) error {
	if !tenantIDRE.MatchString(t.ID) {
		return fmt.Errorf("id %q must match %s", t.ID, tenantIDRE.String())
	}
	if t.Host == "" {
		return errors.New("host must be set")
	}
	if t.Database == "" {
		return errors.New("database must be set")
	}
	if t.Username == "" {
		return errors.New("username must be set")
	}
	if t.PasswordEnv == "" {
		return errors.New("passwordEnv must be set (name of the env var holding the password)")
	}
	return nil
}
