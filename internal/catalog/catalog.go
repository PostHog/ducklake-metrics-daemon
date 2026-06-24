// Package catalog wraps pgx connection lifecycle + query execution against
// the DuckLake catalog Postgres. One [Catalog] per tenant; the runner
// holds them in a map keyed by tenant id.
//
// Connections are pooled (pgxpool) so concurrent queries against the same
// tenant don't serialize at the connection layer. Per-query timeout is a
// hard ceiling enforced via context.WithTimeout; a query that exceeds
// that bound is canceled (Postgres receives a `CancelRequest`) and the
// row scan returns ctx.Err().
package catalog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Conn defines connection inputs the daemon needs to reach a tenant's
// catalog. Mirrors the env-var contract from the Python daemon
// (DUCKLAKE_RDS_*) so operators can carry config over verbatim. Password
// is held as a string here because that's how pgx wants it; callers
// should pull the value from a Secret-mounted file or env at startup
// and not log it.
type Conn struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

// DSN returns the pgx connection string for c. Built via libpq-style
// keyword/value form (not URI) with EVERY value single-quoted and
// embedded ' / \ escaped per libpq rules so passwords containing those
// characters round-trip safely. URL-form DSN would force the same
// concern as percent-encoding; kv-form with explicit quoting is the
// safer default.
func (c Conn) DSN() string {
	// sslmode=require is the safe default for RDS — they all support TLS
	// and the cluster's CA bundle is already in the runtime image.
	// `require` doesn't validate the cert chain; for full verification a
	// future flag could swap to `verify-full` and mount the RDS CA bundle.
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=require connect_timeout=10",
		quoteKV(c.Host),
		c.Port,
		quoteKV(c.Database),
		quoteKV(c.Username),
		quoteKV(c.Password),
	)
}

// quoteKV escapes a value for libpq kv-form DSN: wraps in single quotes
// and backslash-escapes embedded ' and \. Per libpq docs, this is what
// allows passwords containing quotes, spaces, or backslashes to parse
// safely.
func quoteKV(v string) string {
	var b strings.Builder
	b.Grow(len(v) + 2)
	b.WriteByte('\'')
	for _, r := range v {
		if r == '\'' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('\'')
	return b.String()
}

// Catalog is a per-tenant connection pool + tenant identity. Use [Open] to
// construct; closes idempotently via [Catalog.Close].
type Catalog struct {
	Tenant string
	pool   *pgxpool.Pool
}

// Open creates a pool against c and verifies reachability via an
// initial Ping. The pool is lazy past the first connection — actual
// pgx connections are created on demand up to PoolMaxConns. Defaults
// to MaxConns=4 which is plenty for the daemon's query rate (one
// query at a time per tenant by the scheduler design); raise if a
// future scheduler runs per-tenant queries concurrently.
func Open(ctx context.Context, tenant string, c Conn) (*Catalog, error) {
	cfg, err := pgxpool.ParseConfig(c.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse pool config for tenant %q: %w", tenant, err)
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 5 * time.Minute
	// application_name surfaces on the Postgres side (pg_stat_activity)
	// so an operator chasing a wedged catalog can attribute connections
	// to this daemon vs. other clients.
	cfg.ConnConfig.RuntimeParams["application_name"] = "ducklake-metrics-daemon/" + tenant
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool for tenant %q: %w", tenant, err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping catalog for tenant %q: %w", tenant, err)
	}
	return &Catalog{Tenant: tenant, pool: pool}, nil
}

// Close releases the connection pool. Safe to call multiple times — pgx
// pool's Close is idempotent.
func (c *Catalog) Close() {
	if c == nil || c.pool == nil {
		return
	}
	c.pool.Close()
}

// Stats returns a snapshot of pgxpool's stats. Exposed so the runner
// can mirror them into Prometheus gauges (acquired/idle/total conns,
// cumulative empty-acquire count) without giving callers a handle to
// the pool itself.
type Stats struct {
	AcquiredConns     int32
	IdleConns         int32
	TotalConns        int32
	EmptyAcquireCount int64
}

// Stat returns the current pool statistics for Prometheus gauges. Cheap
// — pgx's pool tracks these as int32/int64 fields, no IO.
func (c *Catalog) Stat() Stats {
	if c == nil || c.pool == nil {
		return Stats{}
	}
	s := c.pool.Stat()
	return Stats{
		AcquiredConns:     s.AcquiredConns(),
		IdleConns:         s.IdleConns(),
		TotalConns:        s.TotalConns(),
		EmptyAcquireCount: s.EmptyAcquireCount(),
	}
}

// ErrRowCapExceeded is returned by [Catalog.Query] when a result set
// exceeds the per-query row cap. Callers (the runner) treat this as a
// query error — the partial result is discarded so a runaway query
// can't poison gauges with truncated data.
var ErrRowCapExceeded = fmt.Errorf("query result row cap exceeded")

// Row is one result row from [Catalog.Query]. Keys are column names
// (from pgx FieldDescriptions); values are pgx's decoded Go types.
// Numeric values come back as int64 / float64 / pgtype.Numeric depending
// on the underlying Postgres type — callers (the runner) convert to
// float64 for Prometheus gauges using the same coercion path the Python
// daemon did via float().
type Row map[string]any

// Query runs sql against the catalog with a per-query timeout and a
// per-query row cap. Returns rows + column names (in source order; used
// by the runner to map result columns to the query's declared labels/values
// without depending on map iteration order).
//
// maxRows is a hard ceiling on materialized result-set size. An operator
// who ships a query without LIMIT (or a runaway top-N that explodes)
// would otherwise OOM the pod; we error out past the cap and let the
// runner bump query_errors_total. 0 disables the cap (use with caution).
//
// On error: the pgx error is wrapped; the caller (runner) bumps
// query_errors_total and leaves last-known gauge values in place.
func (c *Catalog) Query(ctx context.Context, sql string, timeout time.Duration, maxRows int) ([]Row, []string, error) {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := c.pool.Query(qctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = string(f.Name)
	}
	var out []Row
	for rows.Next() {
		if maxRows > 0 && len(out) >= maxRows {
			return nil, nil, fmt.Errorf("%w: >%d rows", ErrRowCapExceeded, maxRows)
		}
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("row decode: %w", err)
		}
		row := make(Row, len(cols))
		for i, name := range cols {
			row[name] = vals[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration: %w", err)
	}
	return out, cols, nil
}
