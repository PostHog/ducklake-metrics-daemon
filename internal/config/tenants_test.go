package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempTenants(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tenants.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write tempfile: %v", err)
	}
	return path
}

func TestLoadTenantsEmptyPath(t *testing.T) {
	got, err := LoadTenants("")
	if err != nil {
		t.Errorf("err=%v, want nil", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil map", got)
	}
}

func TestLoadTenantsHappy(t *testing.T) {
	path := writeTempTenants(t, `tenants:
  - id: alpha
    host: alpha.example.com
    database: alpha_db
    username: alpha
    passwordEnv: ALPHA_PW
  - id: beta
    host: beta.example.com
    port: 6543
    database: beta_db
    username: beta
    passwordEnv: BETA_PW
`)
	t.Setenv("ALPHA_PW", "alphasecret")
	t.Setenv("BETA_PW", "betasecret")
	got, err := LoadTenants(path)
	if err != nil {
		t.Fatalf("LoadTenants: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	a, ok := got["alpha"]
	if !ok {
		t.Fatal("missing alpha")
	}
	if a.Port != 5432 {
		t.Errorf("alpha Port=%d, want default 5432", a.Port)
	}
	if a.Password != "alphasecret" {
		t.Errorf("alpha Password=%q, want pulled from env", a.Password)
	}
	b := got["beta"]
	if b.Port != 6543 {
		t.Errorf("beta Port=%d, want 6543 (YAML override)", b.Port)
	}
	if b.Password != "betasecret" {
		t.Errorf("beta Password=%q", b.Password)
	}
}

func TestLoadTenantsMissingPasswordEnv(t *testing.T) {
	path := writeTempTenants(t, `tenants:
  - id: alpha
    host: alpha.example.com
    database: alpha_db
    username: alpha
    passwordEnv: UNSET_VAR_XYZ
`)
	os.Unsetenv("UNSET_VAR_XYZ")
	_, err := LoadTenants(path)
	if err == nil {
		t.Fatal("want error for missing password env")
	}
	if !strings.Contains(err.Error(), "UNSET_VAR_XYZ") {
		t.Errorf("err=%v, want env var name in message", err)
	}
}

func TestLoadTenantsDuplicateID(t *testing.T) {
	path := writeTempTenants(t, `tenants:
  - id: dup
    host: a.example.com
    database: a
    username: a
    passwordEnv: A_PW
  - id: dup
    host: b.example.com
    database: b
    username: b
    passwordEnv: B_PW
`)
	t.Setenv("A_PW", "x")
	t.Setenv("B_PW", "x")
	_, err := LoadTenants(path)
	if err == nil {
		t.Fatal("want error for duplicate id")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("err=%v, want 'duplicate' in message", err)
	}
}

func TestLoadTenantsBadID(t *testing.T) {
	cases := []string{
		"UPPERCASE",
		"with.dots",
		"with space",
		"-leading-dash",
		"",
	}
	for _, bad := range cases {
		t.Run(bad, func(t *testing.T) {
			path := writeTempTenants(t, `tenants:
  - id: `+bad+`
    host: a.example.com
    database: a
    username: a
    passwordEnv: PW
`)
			t.Setenv("PW", "x")
			_, err := LoadTenants(path)
			if err == nil {
				t.Fatalf("id=%q: want validation error", bad)
			}
		})
	}
}

func TestLoadTenantsMissingRequired(t *testing.T) {
	cases := map[string]string{
		"missing host": `tenants:
  - id: alpha
    database: a
    username: a
    passwordEnv: PW
`,
		"missing database": `tenants:
  - id: alpha
    host: a.example.com
    username: a
    passwordEnv: PW
`,
		"missing username": `tenants:
  - id: alpha
    host: a.example.com
    database: a
    passwordEnv: PW
`,
		"missing passwordEnv": `tenants:
  - id: alpha
    host: a.example.com
    database: a
    username: a
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeTempTenants(t, body)
			t.Setenv("PW", "x")
			_, err := LoadTenants(path)
			if err == nil {
				t.Fatal("want validation error")
			}
		})
	}
}

func TestLoadTenantsEmpty(t *testing.T) {
	path := writeTempTenants(t, "tenants: []\n")
	_, err := LoadTenants(path)
	if err == nil {
		t.Fatal("want error for empty tenants list")
	}
}

func TestLoadTenantsBadFile(t *testing.T) {
	_, err := LoadTenants("/nonexistent/tenants.yaml")
	if err == nil {
		t.Fatal("want read error")
	}
}
