package queries

import (
	"errors"
	"strings"
	"testing"
)

// TestLoadBuiltinsParses pins the embedded YAML — if a future edit
// breaks builtin.yaml, every prod daemon would crash-loop at startup.
// Better to fail in CI.
func TestLoadBuiltinsParses(t *testing.T) {
	qs, err := LoadBuiltins()
	if err != nil {
		t.Fatalf("LoadBuiltins: %v", err)
	}
	if len(qs) == 0 {
		t.Fatal("builtin.yaml parsed to zero queries — embed broken?")
	}
	// Spot-check one specific query that the dashboards depend on.
	var found bool
	for _, q := range qs {
		if q.Name == "ducklake_data_files" {
			found = true
		}
	}
	if !found {
		t.Error("ducklake_data_files missing from builtins — dashboard would break")
	}
}

// TestBuiltinsNoLeakedAlias is a guard against accidentally porting a
// query that still uses the DuckLake-extension-only schema alias
// (which would fail at runtime against raw Postgres).
func TestBuiltinsNoLeakedAlias(t *testing.T) {
	if strings.Contains(string(BuiltinYAML), "__ducklake_metadata_lake") {
		t.Error("builtin.yaml references __ducklake_metadata_lake — this is the DuckDB-side alias; rewrite to public.* for Postgres")
	}
}

// TestPartitionValueCorruptionCountsPositions guards the ncols computation
// in ducklake_partition_value_corruption. events/persons partition by
// multiple transforms (year/month/day/hour) on ONE source column, so all
// positions share a column_id; counting DISTINCT column_id collapses a
// 4-position partition to 1 and false-flags every file (this shipped once
// and reported 17M "corrupt" files on megaduck.events). ncols must count
// partition_key_index (the position), not column_id.
func TestPartitionValueCorruptionCountsPositions(t *testing.T) {
	qs, err := LoadBuiltins()
	if err != nil {
		t.Fatalf("LoadBuiltins: %v", err)
	}
	var sql string
	for _, q := range qs {
		if q.Name == "ducklake_partition_value_corruption" {
			sql = q.SQL
		}
	}
	if sql == "" {
		t.Fatal("ducklake_partition_value_corruption not found in builtins")
	}
	if !strings.Contains(sql, "COUNT(DISTINCT pc.partition_key_index)") {
		t.Error("ncols must be COUNT(DISTINCT pc.partition_key_index) — counting positions, not columns")
	}
	if strings.Contains(sql, "DISTINCT pi.table_id, pc.column_id") {
		t.Error("ncols must NOT count DISTINCT column_id — collapses multi-transform-on-one-column partitions (year/month/day/hour), false-flagging every file")
	}
}

func TestLoadFileEmptyPath(t *testing.T) {
	qs, err := LoadFile("")
	if err != nil {
		t.Errorf("LoadFile(\"\") err=%v, want nil", err)
	}
	if qs != nil {
		t.Errorf("LoadFile(\"\") returned %d queries, want nil", len(qs))
	}
}

func TestLoadFileReadError(t *testing.T) {
	qs, err := LoadFile("/nonexistent/path/queries.yaml")
	if err == nil {
		t.Fatal("LoadFile(nonexistent) returned nil error")
	}
	if qs != nil {
		t.Errorf("LoadFile error case returned non-nil queries: %v", qs)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		q       Query
		wantErr string
	}{
		{"happy", Query{Name: "foo", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"}, ""},
		{"bad name dot", Query{Name: "foo.bar", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"}, "name"},
		{"bad name dash", Query{Name: "foo-bar", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"}, "name"},
		{"bad name leading digit", Query{Name: "1foo", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"}, "name"},
		{"empty help", Query{Name: "foo", Values: []string{"v"}, SQL: "SELECT 1"}, "help"},
		{"no values", Query{Name: "foo", Help: "h", SQL: "SELECT 1"}, "values"},
		{"empty sql", Query{Name: "foo", Help: "h", Values: []string{"v"}}, "sql"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validate(&tc.q)
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("want nil, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err=%v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestMergeUserWinsOnCollision(t *testing.T) {
	builtins := []Query{
		{Name: "foo", Help: "from-builtin", Values: []string{"v"}, SQL: "SELECT 1"},
		{Name: "bar", Help: "from-builtin", Values: []string{"v"}, SQL: "SELECT 1"},
	}
	user := []Query{
		{Name: "foo", Help: "from-user", Values: []string{"v"}, SQL: "SELECT 2"},
	}
	got := Merge(builtins, user, nil)
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	for _, q := range got {
		if q.Name == "foo" && q.Help != "from-user" {
			t.Errorf("foo not overridden by user: %+v", q)
		}
		if q.Name == "bar" && q.Help != "from-builtin" {
			t.Errorf("bar unexpectedly modified: %+v", q)
		}
	}
}

func TestMergeDisableDrops(t *testing.T) {
	builtins := []Query{
		{Name: "foo", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"},
		{Name: "bar", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"},
	}
	got := Merge(builtins, nil, map[string]bool{"foo": true})
	if len(got) != 1 {
		t.Fatalf("len=%d, want 1", len(got))
	}
	if got[0].Name != "bar" {
		t.Errorf("got %q, want bar", got[0].Name)
	}
}

func TestMergeSortedDeterministic(t *testing.T) {
	// Reproducibility: heap order at startup depends on slice order.
	builtins := []Query{
		{Name: "zzz", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"},
		{Name: "aaa", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"},
		{Name: "mmm", Help: "h", Values: []string{"v"}, SQL: "SELECT 1"},
	}
	got := Merge(builtins, nil, nil)
	for i := 1; i < len(got); i++ {
		if got[i-1].Name >= got[i].Name {
			t.Errorf("not sorted at %d: %q >= %q", i, got[i-1].Name, got[i].Name)
		}
	}
}

func TestLoadParseError(t *testing.T) {
	_, err := load([]byte("this is: not: valid: yaml:::\n  - x\n  - y"), "test.yaml")
	if err == nil {
		t.Fatal("want parse error, got nil")
	}
	if !strings.Contains(err.Error(), "test.yaml") {
		t.Errorf("err=%v, want source filename in message", err)
	}
}

func TestLoadFileViaStub(t *testing.T) {
	// readFile is a package var so tests can stub it; pin that contract.
	orig := readFile
	defer func() { readFile = orig }()
	readFile = func(name string) ([]byte, error) {
		if name != "stub.yaml" {
			return nil, errors.New("unexpected path")
		}
		// Note: interval_mins is in the YAML on purpose — it's an
		// unknown field for back-compat with operator YAML written
		// against the Python daemon. yaml.v3 silently ignores it.
		return []byte("queries:\n  - name: my_metric\n    help: test\n    interval_mins: 1\n    values: [count]\n    sql: SELECT 1\n"), nil
	}
	qs, err := LoadFile("stub.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(qs) != 1 || qs[0].Name != "my_metric" {
		t.Errorf("got %+v, want one query named my_metric", qs)
	}
}
