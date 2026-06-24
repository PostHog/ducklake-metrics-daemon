// Package server is the daemon's HTTP surface — /metrics for Prometheus
// scrape, /-/healthy as the K8s liveness probe target, /-/ready as the
// readiness probe target.
//
// /-/healthy reflects ACTUAL scheduler progress, not just "process is
// answering HTTP." A wedged pgx call leaves `liveness.currentQueryStart`
// set; if it's been set for > timeout the handler returns 503 so kubelet
// restarts the pod. The Python daemon's PostHog/millpond#97 decision
// matrix is mirrored here verbatim plus a `restarting` reason that the
// outer tenant lifecycle sets during cooldown — without it, a stale
// lastTick would age past timeout during the cooldown and trip 503 on a
// healthy-but-restarting tenant.
//
// Multi-tenant: the daemon runs one Runner goroutine per tenant, each
// with its own Liveness. The /-/healthy decision is "ANY tenant
// unhealthy → 503" — kubelet restarting the pod is the only recovery
// from a wedged scheduler, and a single wedge takes the whole process
// with it. /-/ready reports 200 once AT LEAST ONE tenant has connected
// (so Prometheus starts scraping the tenants that ARE up, instead of
// waiting on the slowest one).
package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/runner"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LivenessReason is the stable enum of /-/healthy outcomes. Keep these
// strings stable — they're metric label values on
// ducklake_metrics_liveness_failures_total{reason=...} that operators
// alert on.
type LivenessReason string

const (
	ReasonStarting      LivenessReason = "starting"
	ReasonOK            LivenessReason = "ok"
	ReasonStaleSchedule LivenessReason = "stale_schedule"
)

// staleScheduleMultiplier is how many cycle intervals can pass without
// a fire before /-/healthy flips to 503. 2× gives one missed-tick of
// slack (e.g. a busy GC pause); 3× would mask a single-tenant scheduler
// hang for 15 minutes at the default 5-min interval.
const staleScheduleMultiplier = 2

// LivenessStatus is the pure decision function — given a snapshot of
// one tenant's scheduler state and a clock reading, return whether
// that tenant is alive and the reason code.
//
// Healthy means "scheduler is firing cycle goroutines on schedule and
// pushing results to the publisher." It does NOT mean "queries are
// succeeding" — per-query failures are tracked separately by
// ducklake_metrics_query_errors_total. The Python daemon conflated
// these; this version doesn't.
func LivenessStatus(s runner.LivenessSnapshot, now time.Time) (alive bool, reason LivenessReason, msg string) {
	if s.LastFire.IsZero() {
		// Scheduler hasn't fired its first cycle yet — process is up
		// but the per-tenant startup (connect-with-backoff + stagger)
		// hasn't completed. K8s startup grace covers this.
		return true, ReasonStarting, "starting"
	}
	age := now.Sub(s.LastFire)
	threshold := time.Duration(staleScheduleMultiplier) * s.Interval
	if age > threshold {
		// Scheduler hasn't fired in too long — goroutine is dead
		// (silent panic upstream of recovery, deadlock, runtime
		// hang). Only kubelet pod restart can recover.
		return false, ReasonStaleSchedule, fmt.Sprintf("no scheduler fire in %s (threshold %s)", age.Round(time.Second), threshold)
	}
	return true, ReasonOK, "ok"
}

// SnapshotsFunc returns a fresh map of tenant-id → liveness snapshot,
// called once per probe scrape. Pulled behind a function so tests can
// inject canned snapshots without spinning up a Runner.
type SnapshotsFunc func() map[string]runner.LivenessSnapshot

// Server bundles the runtime pieces the HTTP handlers need.
type Server struct {
	registry  *prometheus.Registry
	self      *metrics.Self
	snapshots SnapshotsFunc
	ready     func(tenant string) bool // per-tenant readiness predicate
	// httpSrv is constructed eagerly in New() so Shutdown() never races
	// with a parallel ListenAndServe() that hasn't assigned it yet.
	httpSrv *http.Server
}

// Config inputs for the server's New.
type Config struct {
	// Addr is the TCP bind address (e.g. ":9100"). Pinned at
	// construction so the http.Server can be built eagerly — without
	// this, Shutdown() racing with ListenAndServe() in another goroutine
	// would see a nil httpSrv and silently no-op while the listener
	// stays up.
	Addr     string
	Registry *prometheus.Registry
	Self     *metrics.Self
	// Snapshots is called on every /-/healthy scrape to gather a fresh
	// view of every tenant's liveness state. Returning a nil/empty map
	// reports "no tenants" — the handler will 200 with a zero-tenant
	// body, which mirrors the pre-tenant state at process startup.
	Snapshots SnapshotsFunc
	// Ready answers whether a given tenant has finished its first
	// catalog connect. /-/ready returns 200 once ANY tenant is ready.
	Ready func(tenant string) bool
}

// New constructs a Server. The HTTP listener is up to the caller (see
// [Server.Mux] / [Server.ListenAndServe]).
func New(cfg Config) *Server {
	s := &Server{
		registry:  cfg.Registry,
		self:      cfg.Self,
		snapshots: cfg.Snapshots,
		ready:     cfg.Ready,
	}
	s.httpSrv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.Mux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Mux returns an *http.ServeMux wired with the three endpoints. Callers
// can use it directly or compose it into a larger mux.
func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{Registry: s.registry}))
	mux.HandleFunc("/-/healthy", s.handleHealthy)
	mux.HandleFunc("/-/ready", s.handleReady)
	return mux
}

// ListenAndServe binds the configured Addr and serves the daemon's
// three endpoints. Blocks until the underlying http.Server returns.
// Wraps the nil-vs-ErrServerClosed dance so callers can treat shutdown
// as a no-error path.
func (s *Server) ListenAndServe() error {
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully closes the HTTP server, draining in-flight
// requests up to ctx's deadline. Safe even if ListenAndServe hasn't
// been called yet — http.Server.Shutdown on a never-bound server is a
// no-op that returns nil.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

// handleHealthy returns 200 if every tenant's scheduler is healthy, 503
// if ANY tenant is wedged. The failure-reason counter is incremented for
// each unhealthy tenant individually, so alert queries can attribute the
// 503 to the specific tenant + reason. The response body lists every
// unhealthy tenant for human readability of kubelet logs.
func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	snaps := map[string]runner.LivenessSnapshot{}
	if s.snapshots != nil {
		snaps = s.snapshots()
	}
	tenants := sortedKeys(snaps)
	var failures []string
	for _, t := range tenants {
		alive, reason, msg := LivenessStatus(snaps[t], now)
		if !alive {
			s.self.LivenessFailuresTotal.WithLabelValues(t, string(reason)).Inc()
			failures = append(failures, fmt.Sprintf("%s: %s (%s)", t, msg, reason))
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, strings.Join(failures, "\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "ok (%d tenant(s))\n", len(tenants))
}

// handleReady returns 200 once ANY tenant has reported ready. Prefers
// availability over completeness — a single fast tenant should not be
// held back from scraping by a slow one. Lists not-ready tenants in the
// response body for human readability.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	snaps := map[string]runner.LivenessSnapshot{}
	if s.snapshots != nil {
		snaps = s.snapshots()
	}
	tenants := sortedKeys(snaps)
	var notReady []string
	anyReady := false
	for _, t := range tenants {
		if s.ready != nil && s.ready(t) {
			anyReady = true
		} else {
			notReady = append(notReady, t)
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if anyReady {
		w.WriteHeader(http.StatusOK)
		if len(notReady) > 0 {
			_, _ = fmt.Fprintf(w, "ok (still connecting: %s)\n", strings.Join(notReady, ", "))
			return
		}
		_, _ = fmt.Fprint(w, "ok\n")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, "not ready (%d tenant(s) still connecting)\n", len(tenants))
}

// sortedKeys returns the tenant id set in stable order for deterministic
// iteration in handler output.
func sortedKeys(m map[string]runner.LivenessSnapshot) []string {
	out := make([]string, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
