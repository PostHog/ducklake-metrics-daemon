// Package queries loads and validates the daemon's catalog-side query
// definitions from YAML (embedded built-ins + optional operator-supplied
// extensions), and exposes them to the runner as a flat []Query.
//
// The schema mirrors the Python daemon's BUILTIN_YAML format exactly so
// existing operator YAML carries over verbatim. The one mechanical change
// is that the SQL references `public.ducklake_*` (the real Postgres
// schema) instead of `__ducklake_metadata_lake.ducklake_*` (the
// DuckLake-extension-only alias the Python daemon used via DuckDB ATTACH).
// On raw Postgres the catalog tables live in `public`.
package queries

import (
	_ "embed"
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"
)

// BuiltinYAML is the embedded library of catalog-side queries the daemon
// ships with. Lifted verbatim from millpond's tools/ducklake_metrics.py
// BUILTIN_YAML (with schema rewrite). Treat as the source of truth for
// metric names exposed by the daemon — any change here is a metric-name
// change that downstream Grafana dashboards depend on.
//
//go:embed builtin.yaml
var BuiltinYAML []byte

// Query is one entry in the YAML library. Each Query produces N Prometheus
// gauges named `<Name>_<value>` for each `value` in Values; if Labels is
// non-empty those become Prometheus label dimensions (always with `tenant`
// prepended at sample-emit time, the same way the Python daemon did it).
//
// Per-query intervals are gone: the daemon runs every query every
// `DUCKLAKE_CYCLE_INTERVAL` (default 5 min). The YAML schema is checked
// against unknown fields strictly enough that a leftover `interval_mins`
// in an operator file would NOT fail — yaml.v3's default is to silently
// ignore unknown keys, and we keep that behavior so existing YAML keeps
// loading.
type Query struct {
	Name   string   `yaml:"name"`
	Help   string   `yaml:"help"`
	Labels []string `yaml:"labels,omitempty"`
	Values []string `yaml:"values"`
	SQL    string   `yaml:"sql"`
}

// yamlDoc is the top-level YAML shape.
type yamlDoc struct {
	Queries []Query `yaml:"queries"`
}

// nameRE constrains query names to a metric-safe identifier shape (matches
// the Python daemon's _NAME_RE so user YAML doesn't pass here and fail in
// the Python loader).
var nameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// LoadBuiltins parses the embedded YAML library and returns the resulting
// queries. Errors here are programming errors in the embedded file —
// surface them loud at daemon startup.
func LoadBuiltins() ([]Query, error) {
	return load(BuiltinYAML, "builtin.yaml")
}

// LoadFile reads and parses an operator-supplied query YAML, intended for
// the DUCKLAKE_METRICS_CONFIG env var. Returns an empty slice when path is
// "" so callers can pass it through unconditionally.
func LoadFile(path string) ([]Query, error) {
	if path == "" {
		return nil, nil
	}
	// Defer to a helper so the test suite can use Load() directly without
	// a tempfile.
	data, err := readFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return load(data, path)
}

// Merge composes built-in + user queries with user-wins-on-name-collision
// semantics and a final filter dropping any name in `disable`. Mirrors
// the Python daemon's load_queries() composition order exactly. Returns a
// stable slice (sorted by name) so the scheduler's heap order is
// reproducible across restarts.
func Merge(builtins, user []Query, disable map[string]bool) []Query {
	byName := map[string]Query{}
	for _, q := range builtins {
		byName[q.Name] = q
	}
	for _, q := range user {
		byName[q.Name] = q // user wins on collision
	}
	out := make([]Query, 0, len(byName))
	for _, q := range byName {
		if disable[q.Name] {
			continue
		}
		out = append(out, q)
	}
	sortByName(out)
	return out
}

func load(data []byte, src string) ([]Query, error) {
	var doc yamlDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%s: parse: %w", src, err)
	}
	for i := range doc.Queries {
		q := &doc.Queries[i]
		if err := validate(q); err != nil {
			return nil, fmt.Errorf("%s: queries[%d] (%q): %w", src, i, q.Name, err)
		}
	}
	return doc.Queries, nil
}

func validate(q *Query) error {
	if !nameRE.MatchString(q.Name) {
		return fmt.Errorf("name %q must match %s", q.Name, nameRE.String())
	}
	if q.Help == "" {
		return fmt.Errorf("help must be set")
	}
	if len(q.Values) == 0 {
		return fmt.Errorf("values must be non-empty")
	}
	if q.SQL == "" {
		return fmt.Errorf("sql must be set")
	}
	return nil
}
