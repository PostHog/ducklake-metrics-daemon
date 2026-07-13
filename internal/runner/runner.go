// Package runner owns the per-tenant scheduling loop. Each tenant has
// one long-lived scheduler goroutine that fires fresh cycle goroutines
// on a ticker; each cycle runs the configured queries serially against
// the tenant's catalog and sends each Result to the publisher channel,
// then dies. If the previous cycle's goroutine is still alive when the
// next tick fires, it gets canceled and the kill is metered.
//
// The scheduler does NOT write Prometheus gauges directly — that's the
// publisher's job. This separation eliminates the cross-tenant gauge-
// wipe class of bug entirely; the runner is a producer only.
//
// Liveness here is a single atomic timestamp per tenant: the last time
// the scheduler fired a cycle. /-/healthy checks `time.Since(lastFire)`
// against 2×interval. The old multi-field state machine
// (schedulerStarted / currentQueryStart / lastTick / restarting) is
// gone — there's nothing for it to track now that we don't intermix
// per-query progress with per-tenant lifecycle.
package runner

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/publisher"
	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
)

// Liveness is the per-tenant scheduler-health record. One atomic
// timestamp (last scheduler fire) plus the interval used to compute
// "is this still firing on schedule." Snapshot is lock-free.
type Liveness struct {
	interval time.Duration
	lastFire atomic.Int64 // unix nanoseconds; 0 = never fired
}

// NewLiveness constructs a Liveness with the cycle interval that
// /-/healthy uses for the staleness threshold (currently 2×interval).
func NewLiveness(interval time.Duration) *Liveness {
	return &Liveness{interval: interval}
}

// MarkFire records that the scheduler just fired a cycle. Called once
// per ticker tick (and once at startup). Lock-free.
func (l *Liveness) MarkFire() {
	l.lastFire.Store(time.Now().UnixNano())
}

// Snapshot returns the current state for the HTTP handler. lastFire=0
// (never fired) maps to the zero time.Time, not Unix epoch — callers
// check IsZero() to detect the never-fired state.
func (l *Liveness) Snapshot() LivenessSnapshot {
	ns := l.lastFire.Load()
	var t time.Time
	if ns > 0 {
		t = time.Unix(0, ns)
	}
	return LivenessSnapshot{
		Interval: l.interval,
		LastFire: t,
	}
}

// LivenessSnapshot is the value the HTTP handler reads.
type LivenessSnapshot struct {
	Interval time.Duration
	LastFire time.Time // zero == scheduler hasn't fired yet
}

// Config bundles the scheduler's inputs.
type Config struct {
	Tenant string
	// Catalog is the per-tenant pgxpool wrapper. The scheduler hands
	// it to each cycle goroutine; pgx auto-reconnects on transient
	// pool failures, so the scheduler never needs to rebuild it.
	Catalog *catalog.Catalog
	// Queries is the deduplicated, merged, sorted query list.
	Queries []queries.Query
	// Self is shared self-metrics (cycle counters live here).
	Self *metrics.Self
	// Liveness is updated on every ticker fire.
	Liveness *Liveness
	// Results is the publisher's inbox. Cycle goroutines send into
	// this channel; the publisher is the single consumer.
	Results chan<- publisher.Result
	// Interval is how often the scheduler fires a new cycle.
	Interval time.Duration
	// QueryTimeout bounds an individual Catalog.Query call inside a
	// cycle. The cycle's overall deadline is enforced separately by
	// the scheduler (next ticker fire cancels the running cycle).
	QueryTimeout time.Duration
	// MaxQueryRows is passed through to Catalog.Query as the row cap.
	MaxQueryRows int
	Logger       *slog.Logger
}

// Run blocks until ctx is canceled, firing one cycle goroutine per
// interval tick. If the previous cycle is still running when the next
// tick fires, it's canceled (the cancel propagates into the cycle's
// queries via ctx) and ducklake_metrics_cycle_killed_total is bumped.
//
// Initial stagger: each tenant starts at a deterministic offset
// derived from its id, so 50 tenants don't all hit RDS at the same
// instant on pod startup.
func Run(ctx context.Context, cfg Config) {
	tlog := cfg.Logger.With("tenant", cfg.Tenant)
	// Initial stagger.
	offset := stagger(cfg.Tenant, cfg.Interval)
	if offset > 0 {
		tlog.Info("scheduler stagger", "delay_s", int(offset.Seconds()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(offset):
		}
	}

	// cancelPrev cancels the currently-running cycle (if any). On the
	// first fire it's a no-op; on every subsequent fire it kills the
	// prior cycle if it's still in flight.
	cancelPrev := func() {}
	defer func() { cancelPrev() }() // shutdown: kill in-flight cycle

	// Per-query last-run tracking for queries that opt into their own
	// cadence via interval_seconds. Owned solely by this Run goroutine
	// (mutated only inside fire, which the scheduler invokes serially),
	// so no synchronization is needed — the due set is computed HERE
	// and handed to the cycle goroutine as its own slice, never shared.
	lastRun := make(map[string]time.Time, len(cfg.Queries))

	fire := func() {
		// If the prior cycle is still running, this cancel signals it
		// to abort; its deferred metrics observe ctx.Err() and bump
		// cycle_killed_total. We don't wait for it — the goroutine
		// will unwind on its own.
		cancelPrev()
		cctx, cancel := context.WithCancel(ctx)
		cancelPrev = cancel
		cfg.Liveness.MarkFire()
		cfg.Self.SchedulerFiresTotal.WithLabelValues(cfg.Tenant).Inc()

		// Select the queries due this tick. interval_seconds<=0 runs
		// every cycle; a positive interval runs only once its window
		// has elapsed since the last run. lastRun is empty at startup,
		// so every query (including slow-cadence ones) runs on the
		// first fire — /metrics populates promptly. QueryTimeout still
		// bounds each execution, so a slow-cadence query can't overrun
		// the cycle.
		cycleCfg := cfg
		cycleCfg.Queries = dueQueries(cfg.Queries, lastRun, time.Now())
		go runCycle(cctx, cycleCfg, tlog)
	}

	fire() // fire immediately on startup so /metrics populates fast
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fire()
		}
	}
}

// runCycle is one cycle: serial queries → publisher → die. Deferred
// metrics fire on every exit path (clean completion, ctx-cancel from
// overrun-kill, panic). Panic recovery is here so a poison query can't
// take down the scheduler goroutine.
func runCycle(ctx context.Context, cfg Config, tlog *slog.Logger) {
	start := time.Now()
	completed := false
	defer func() {
		if rec := recover(); rec != nil {
			tlog.Error("cycle panic recovered",
				"panic", fmt.Sprintf("%v", rec),
				"stack", string(debug.Stack()),
			)
			cfg.Self.QueryPanicsTotal.WithLabelValues(cfg.Tenant, "_cycle").Inc()
		}
		cfg.Self.CycleDurationSeconds.WithLabelValues(cfg.Tenant).Set(time.Since(start).Seconds())
		if completed {
			cfg.Self.CycleCompletedTotal.WithLabelValues(cfg.Tenant).Inc()
		} else if ctx.Err() != nil {
			cfg.Self.CycleKilledTotal.WithLabelValues(cfg.Tenant).Inc()
		}
		// If neither completed nor ctx canceled: a panic; counter
		// already bumped above.
	}()

	for _, q := range cfg.Queries {
		if ctx.Err() != nil {
			return
		}
		qStart := time.Now()
		rows, cols, err := cfg.Catalog.Query(ctx, q.SQL, cfg.QueryTimeout, cfg.MaxQueryRows)
		dur := time.Since(qStart)
		cfg.Self.QueryDurationSeconds.WithLabelValues(cfg.Tenant, q.Name).Set(dur.Seconds())
		if err == nil {
			cfg.Self.QueryLastSuccessSeconds.WithLabelValues(cfg.Tenant, q.Name).SetToCurrentTime()
		}
		result := publisher.Result{
			Kind:   publisher.KindQuery,
			Tenant: cfg.Tenant,
			Query:  q.Name,
			Rows:   rows,
			Cols:   cols,
			Err:    err,
		}
		select {
		case cfg.Results <- result:
		case <-ctx.Done():
			return
		}
	}

	// End-of-cycle pool stats — one extra message per cycle, batched
	// with the queries so we don't need a separate pollPoolStats
	// goroutine.
	select {
	case cfg.Results <- publisher.Result{
		Kind:   publisher.KindPoolStat,
		Tenant: cfg.Tenant,
		Stats:  cfg.Catalog.Stat(),
	}:
	case <-ctx.Done():
		return
	}

	completed = true
}

// dueQueries returns the subset of qs to run this tick and records their
// run time in lastRun (mutated in place). A query with IntervalSeconds<=0
// is always due; one with a positive interval is due only once that many
// seconds have elapsed since its last recorded run (and always on its
// first-ever run, since lastRun has no entry). When no query opts into a
// custom interval, qs is returned unchanged (no allocation) — the common
// case. Not safe for concurrent use; the scheduler calls it from a single
// goroutine.
func dueQueries(qs []queries.Query, lastRun map[string]time.Time, now time.Time) []queries.Query {
	anyInterval := false
	for i := range qs {
		if qs[i].IntervalSeconds > 0 {
			anyInterval = true
			break
		}
	}
	if !anyInterval {
		return qs
	}
	due := make([]queries.Query, 0, len(qs))
	for _, q := range qs {
		if q.IntervalSeconds > 0 {
			if last, ok := lastRun[q.Name]; ok &&
				now.Sub(last) < time.Duration(q.IntervalSeconds)*time.Second {
				continue
			}
		}
		lastRun[q.Name] = now
		due = append(due, q)
	}
	return due
}

// stagger derives a deterministic per-tenant initial offset in
// [0, interval). Same tenant always gets the same offset, so log/metric
// timing is reproducible across restarts. fnv-1a 64-bit hash is plenty
// for spreading; cryptographic strength isn't needed. Modulo on the
// unsigned hash so the offset can't be negative (signed-overflow would
// happen otherwise).
func stagger(tenant string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenant))
	return time.Duration(h.Sum64() % uint64(interval))
}
