// Command ducklake-metrics-daemon is the long-lived process that runs
// catalog-side queries against a DuckLake's Postgres metadata store and
// exposes the results as Prometheus gauges over HTTP. Multi-tenant from
// the start: one connection per tenant, one scheduler goroutine per
// tenant, one publisher actor across all tenants.
//
// Architecture (see AGENT.md for details):
//
//	Per tenant:
//	  - connectWithBackoff once at startup → *catalog.Catalog
//	  - runner.Run loops forever, firing one cycle goroutine per
//	    interval. If the previous cycle is still running, it gets
//	    canceled and the kill is metered.
//	  - Each cycle goroutine runs the configured queries serially
//	    against the tenant's catalog and sends every Result to the
//	    publisher's inbox channel, then dies.
//
//	Across tenants:
//	  - One publisher.Publisher goroutine consumes Results and is the
//	    SOLE writer to Prometheus per-query gauges. Eliminates the
//	    cross-tenant gauge-wipe race that the inline-write architecture
//	    suffered from.
//	  - HTTP server exposes /metrics, /-/healthy (scheduler-fire
//	    liveness), /-/ready (any-tenant-connected).
//
//	Lifecycle:
//	  - SIGTERM → ctx cancel → tenant goroutines + publisher drain →
//	    HTTP server Shutdown (with timeout) → exit.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
	"github.com/posthog/ducklake-metrics-daemon/internal/config"
	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/publisher"
	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
	"github.com/posthog/ducklake-metrics-daemon/internal/runner"
	"github.com/posthog/ducklake-metrics-daemon/internal/server"
)

// splitCSV is a small helper used by the --list-queries short-circuit.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// version is stamped at build time via -ldflags '-X main.version=...'.
var version = "dev"

// httpShutdownTimeout bounds graceful HTTP shutdown.
const httpShutdownTimeout = 5 * time.Second

// publisherDrainTimeout bounds how long we wait for the publisher to
// drain its inbox after all tenant senders have exited.
const publisherDrainTimeout = 10 * time.Second

// inboxCapacity is the buffer between N tenant cycle goroutines and the
// single publisher. Large enough to absorb a brief publisher hiccup
// without blocking senders; small enough that runaway producers can't
// hide memory growth indefinitely.
const inboxCapacity = 1024

func main() {
	listQueries := flag.Bool("list-queries", false, "Print resolved query list and exit (validates YAML without connecting)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if *listQueries {
		runListQueries(log)
		return
	}

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "err", err)
		os.Exit(2)
	}

	builtins, err := queries.LoadBuiltins()
	if err != nil {
		log.Error("load builtin queries", "err", err)
		os.Exit(2)
	}
	user, err := queries.LoadFile(cfg.UserQueriesPath)
	if err != nil {
		log.Error("load user queries", "path", cfg.UserQueriesPath, "err", err)
		os.Exit(2)
	}
	qs := queries.Merge(builtins, user, cfg.DisabledQueries)

	tenantIDs := sortedKeys(cfg.Tenants)
	log.Info("starting",
		"version", version,
		"tenants", len(cfg.Tenants),
		"tenant_ids", strings.Join(tenantIDs, ","),
		"queries", len(qs),
		"http_port", cfg.HTTPPort,
		"cycle_interval_s", int(cfg.CycleInterval.Seconds()),
		"query_timeout_s", int(cfg.QueryTimeout.Seconds()),
		"max_query_rows", cfg.MaxQueryRows,
	)

	reg, self, gauges := metrics.Register(qs)

	// Per-tenant Liveness records. Snapshotted on every /-/healthy.
	livenesses := make(map[string]*runner.Liveness, len(cfg.Tenants))
	for id := range cfg.Tenants {
		livenesses[id] = runner.NewLiveness(cfg.CycleInterval)
	}
	snapshots := func() map[string]runner.LivenessSnapshot {
		out := make(map[string]runner.LivenessSnapshot, len(livenesses))
		for t, l := range livenesses {
			out[t] = l.Snapshot()
		}
		return out
	}

	// Per-tenant readiness. Flips true after connectWithBackoff returns
	// successfully; never goes back to false (intentional asymmetry
	// with Up — readiness gates external scrape, which we want to keep
	// even during transient pool reconnects).
	var readyMu sync.RWMutex
	readyByTenant := make(map[string]bool, len(cfg.Tenants))
	isReady := func(t string) bool {
		readyMu.RLock()
		defer readyMu.RUnlock()
		return readyByTenant[t]
	}
	markReady := func(t string) {
		readyMu.Lock()
		readyByTenant[t] = true
		readyMu.Unlock()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// HTTP server constructed before tenants spin up so /-/healthy and
	// /-/ready answer during the connect-with-backoff window.
	httpAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	srv := server.New(server.Config{
		Addr:      httpAddr,
		Registry:  reg,
		Self:      self,
		Snapshots: snapshots,
		Ready:     isReady,
	})
	var httpErr error
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		log.Info("http listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Error("http server", "err", err)
			httpErr = err
			cancel()
		}
	}()

	// Publisher: single-writer actor. Inbox is buffered so cycle
	// goroutines don't block on send during normal scrape pacing.
	inbox := make(chan publisher.Result, inboxCapacity)
	pub := publisher.New(self, gauges, qs, log)
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("publisher panic (outer)",
					"panic", fmt.Sprintf("%v", rec),
					"stack", string(debug.Stack()),
				)
			}
		}()
		pub.Run(ctx, inbox)
	}()

	// Per-tenant lifecycle goroutines. Each one: connect with backoff,
	// then enter the scheduler loop (which fires cycle goroutines).
	// Exits only on ctx cancel; connectWithBackoff retries forever on
	// transient errors so we don't need a per-tenant restart loop.
	var wg sync.WaitGroup
	for _, id := range tenantIDs {
		conn := cfg.Tenants[id]
		wg.Add(1)
		go func(tenant string, conn catalog.Conn) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("tenant goroutine panic (outer)",
						"tenant", tenant,
						"panic", fmt.Sprintf("%v", rec),
						"stack", string(debug.Stack()),
					)
					self.TenantRestartsTotal.WithLabelValues(tenant, "panic_outer").Inc()
				}
			}()
			runTenant(ctx, tenant, conn, qs, self, livenesses[tenant], inbox, markReady, cfg, log)
		}(id, conn)
	}

	// Wait for shutdown. Drain order matters:
	//   1. ctx cancel (signal or HTTP error)
	//   2. tenant goroutines exit (their cycle goroutines abort, in-
	//      flight results are dropped on ctx.Done in the cycle's
	//      select; they don't reach the publisher)
	//   3. close(inbox) — tells publisher no more results coming
	//   4. publisher drains its buffered inbox and returns
	//   5. HTTP server shutdown
	<-ctx.Done()
	wg.Wait()
	close(inbox)
	select {
	case <-publisherDone:
		log.Info("publisher drained")
	case <-time.After(publisherDrainTimeout):
		log.Warn("publisher drain timeout; proceeding to shutdown",
			"timeout_s", int(publisherDrainTimeout.Seconds()))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	<-httpDone
	if httpErr != nil {
		log.Error("exiting with HTTP error", "err", httpErr)
		os.Exit(1)
	}
	log.Info("shutdown clean")
}

// runListQueries is the --list-queries short-circuit. Validates YAML
// without connecting; reads just the env vars it needs.
func runListQueries(log *slog.Logger) {
	builtins, err := queries.LoadBuiltins()
	if err != nil {
		log.Error("load builtin queries", "err", err)
		os.Exit(2)
	}
	user, err := queries.LoadFile(os.Getenv("DUCKLAKE_METRICS_CONFIG"))
	if err != nil {
		log.Error("load user queries", "err", err)
		os.Exit(2)
	}
	disabled := map[string]bool{}
	for _, p := range splitCSV(os.Getenv("DUCKLAKE_METRICS_DISABLE")) {
		disabled[p] = true
	}
	for _, q := range queries.Merge(builtins, user, disabled) {
		fmt.Printf("%s\t%s\n", q.Name, q.Help)
	}
}

// runTenant is the per-tenant lifecycle: connect with backoff, then run
// the scheduler forever. The scheduler fires fresh cycle goroutines on
// the cycle interval; killing overrunning cycles is the scheduler's
// concern, not ours.
//
// Connect failure recovery: pgxpool auto-reconnects under the hood, so
// once we have a *Catalog it stays usable across transient RDS blips.
// Individual query failures are metered (QueryErrorsTotal) but don't
// trigger a tenant-level restart — the scheduler keeps firing on
// schedule and /-/healthy stays 200.
//
// If the scheduler itself dies (panic past recover, runtime hang),
// LastFire stops advancing, /-/healthy goes 503 within 2× interval,
// kubelet restarts the whole pod. That's the recovery path.
func runTenant(
	ctx context.Context,
	tenant string,
	conn catalog.Conn,
	qs []queries.Query,
	self *metrics.Self,
	liveness *runner.Liveness,
	inbox chan<- publisher.Result,
	markReady func(string),
	cfg config.Config,
	log *slog.Logger,
) {
	tlog := log.With("tenant", tenant)
	cat, err := connectWithBackoff(ctx, tenant, conn, self, tlog)
	if err != nil {
		// ctx-cancel only — connectWithBackoff loops forever otherwise.
		return
	}
	defer cat.Close()
	self.Up.WithLabelValues(tenant).Set(1)
	defer self.Up.WithLabelValues(tenant).Set(0)
	markReady(tenant)

	runner.Run(ctx, runner.Config{
		Tenant:       tenant,
		Catalog:      cat,
		Queries:      qs,
		Self:         self,
		Liveness:     liveness,
		Results:      inbox,
		Interval:     cfg.CycleInterval,
		QueryTimeout: cfg.QueryTimeout,
		MaxQueryRows: cfg.MaxQueryRows,
		Logger:       tlog,
	})
}

// connectWithBackoff retries pgx Pool open with exponential backoff up
// to 60s between attempts. Every failed attempt bumps
// ConnectFailuresTotal{tenant,reason} so operators can distinguish
// auth/dial/timeout etc.
func connectWithBackoff(ctx context.Context, tenant string, conn catalog.Conn, self *metrics.Self, log *slog.Logger) (*catalog.Catalog, error) {
	delay := time.Second
	maxDelay := 60 * time.Second
	for {
		cat, err := catalog.Open(ctx, tenant, conn)
		if err == nil {
			log.Info("connected")
			return cat, nil
		}
		reason := classifyConnectError(err)
		self.ConnectFailuresTotal.WithLabelValues(tenant, reason).Inc()
		log.Warn("connect failed; retrying", "reason", reason, "in_s", int(delay.Seconds()), "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// classifyConnectError turns a pgx/net error into a stable reason
// label.
func classifyConnectError(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01", "28000":
			return "auth"
		case "3D000":
			return "invalid_database"
		default:
			return "postgres_" + pgErr.Code
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if netErr.Op == "dial" {
			return "dial"
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dial"
	}
	return "other"
}

func sortedKeys(m map[string]catalog.Conn) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
