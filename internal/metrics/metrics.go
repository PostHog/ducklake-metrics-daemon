// Package metrics owns the Prometheus registry, the per-query gauges,
// and the daemon's self-metrics. Metric names + label shapes mirror the
// Python daemon (millpond/tools/ducklake_metrics.py) so the existing
// Grafana dashboards (managed-warehouse/ducklake-state.json) work
// unchanged against either implementation.
//
// The only metric this package owns by name is everything matching
// `ducklake_*` and `ducklake_metrics_*`. Per-query gauges are constructed
// lazily from the loaded [queries.Query] slice at [Register] time.
package metrics

import (
	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
	"github.com/prometheus/client_golang/prometheus"
)

// Self holds the daemon's self-metrics. The label set on each mirrors the
// Python daemon for the five metrics that existed there (so alerts on
// `ducklake_metrics_up{tenant=...}` keep firing against either
// implementation). The three resilience counters (ConnectFailuresTotal,
// TenantRestartsTotal, QueryPanicsTotal) are Go-daemon additions for
// multi-tenant visibility — they're how operators identify a single
// troublesome destination without it taking down the pod.
type Self struct {
	Up                      *prometheus.GaugeVec   // ducklake_metrics_up
	QueryDurationSeconds    *prometheus.GaugeVec   // ducklake_metrics_query_duration_seconds
	QueryLastSuccessSeconds *prometheus.GaugeVec   // ducklake_metrics_query_last_success_timestamp
	QueryErrorsTotal        *prometheus.CounterVec // ducklake_metrics_query_errors_total
	LivenessFailuresTotal   *prometheus.CounterVec // ducklake_metrics_liveness_failures_total
	ConnectFailuresTotal    *prometheus.CounterVec // ducklake_metrics_connect_failures_total
	TenantRestartsTotal     *prometheus.CounterVec // ducklake_metrics_tenant_restarts_total
	QueryPanicsTotal        *prometheus.CounterVec // ducklake_metrics_query_panics_total
	PoolAcquiredConns       *prometheus.GaugeVec   // ducklake_metrics_pool_acquired_conns
	PoolIdleConns           *prometheus.GaugeVec   // ducklake_metrics_pool_idle_conns
	PoolTotalConns          *prometheus.GaugeVec   // ducklake_metrics_pool_total_conns
	PoolEmptyAcquireTotal   *prometheus.CounterVec // ducklake_metrics_pool_empty_acquire_total
	// Cycle-level metrics — one cycle = one tick of the per-tenant
	// scheduler. Replace the inline scheduler-tick metrics from the
	// previous architecture.
	SchedulerFiresTotal  *prometheus.CounterVec // ducklake_metrics_scheduler_fires_total
	CycleDurationSeconds *prometheus.GaugeVec   // ducklake_metrics_cycle_duration_seconds
	CycleCompletedTotal  *prometheus.CounterVec // ducklake_metrics_cycle_completed_total
	CycleKilledTotal     *prometheus.CounterVec // ducklake_metrics_cycle_killed_total
}

// QueryGauges is the per-query, per-value gauge family. Outer key is
// query name (e.g. "ducklake_data_files"); inner key is value column
// (e.g. "files", "bytes", "rows"). The resulting metric is
// `<query>_<value>`, e.g. `ducklake_data_files_files`.
//
// Labels are always `["tenant"] + query.Labels`. The tenant prepend
// matches the Python daemon's _build_query_gauges shape.
type QueryGauges map[string]map[string]*prometheus.GaugeVec

// Register constructs a fresh Prometheus registry, the self-metrics, and
// one gauge per (query, value) pair. Returns the registry (for the HTTP
// handler to scrape) alongside the gauges. The registry is process-local;
// the daemon doesn't use the prometheus client's global default registry
// so tests can construct isolated instances.
func Register(qs []queries.Query) (*prometheus.Registry, *Self, QueryGauges) {
	reg := prometheus.NewRegistry()
	self := &Self{
		Up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_up",
			Help: "1 while the daemon has a live catalog connection; 0 during reconnect.",
		}, []string{"tenant"}),
		QueryDurationSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_query_duration_seconds",
			Help: "Wall-clock duration of the most recent run for each query.",
		}, []string{"tenant", "query"}),
		QueryLastSuccessSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_query_last_success_timestamp",
			Help: "Unix timestamp of the most recent successful run for each query.",
		}, []string{"tenant", "query"}),
		QueryErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_query_errors_total",
			Help: "Cumulative count of failed query runs.",
		}, []string{"tenant", "query"}),
		LivenessFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_liveness_failures_total",
			Help: "Cumulative count of /-/healthy responses that returned 503, by reason. " +
				"`in_flight` = a single query exceeded the liveness timeout; " +
				"`stale_tick` = no scheduler tick in that long. Alert on rate(...) > 0 " +
				"to catch a wedged daemon BEFORE kubelet's failureThreshold restarts the pod.",
		}, []string{"tenant", "reason"}),
		ConnectFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_connect_failures_total",
			Help: "Cumulative count of pgxpool open failures during connect-with-backoff, " +
				"by reason. `auth` = credentials rejected (28P01/28000); " +
				"`invalid_database` = database does not exist (3D000); " +
				"`dial` = TCP connect failure (DNS, refused, no route); " +
				"`timeout` = context deadline exceeded; `postgres_<sqlstate>` = other " +
				"Postgres-side errors with SQLSTATE; `other` = anything else. " +
				"Sustained `auth`/`invalid_database` rate indicates misconfiguration; " +
				"sustained `dial`/`timeout` indicates network or RDS-side issue.",
		}, []string{"tenant", "reason"}),
		TenantRestartsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_tenant_restarts_total",
			Help: "Cumulative count of tenant goroutine restart cycles, by reason. " +
				"`runner_new_failed` = programmer error (query/gauge mismatch); " +
				"`runner_exit` = scheduler returned unexpectedly; " +
				"`panic` = goroutine recovered from a panic. " +
				"A healthy tenant restarts 0 times; alert on increase(...) > 0.",
		}, []string{"tenant", "reason"}),
		QueryPanicsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_query_panics_total",
			Help: "Cumulative count of panics caught inside a query run. " +
				"Scheduler keeps ticking; the panic is logged with stack and the " +
				"next interval retries. Non-zero indicates a daemon bug worth filing.",
		}, []string{"tenant", "query"}),
		PoolAcquiredConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_pool_acquired_conns",
			Help: "Currently-acquired pgxpool connections per tenant. Should be 0-1 " +
				"in steady state (scheduler is single-goroutine per tenant). " +
				"Persistent >0 with no in-flight query indicates a leaked acquire.",
		}, []string{"tenant"}),
		PoolIdleConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_pool_idle_conns",
			Help: "Currently-idle pgxpool connections per tenant.",
		}, []string{"tenant"}),
		PoolTotalConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_pool_total_conns",
			Help: "Total pgxpool connections per tenant (acquired + idle). Capped at " +
				"the pool's MaxConns; persistent saturation indicates concurrency " +
				"contention worth raising MaxConns for.",
		}, []string{"tenant"}),
		PoolEmptyAcquireTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_pool_empty_acquire_total",
			Help: "Cumulative count of acquire calls that had to wait for a free " +
				"connection (pool was empty). Should be 0 with the single-goroutine " +
				"scheduler; non-zero indicates the scheduler is contending with " +
				"itself somehow or MaxConns needs raising.",
		}, []string{"tenant"}),
		SchedulerFiresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_scheduler_fires_total",
			Help: "Cumulative count of cycle-fire events per tenant. /-/healthy " +
				"derives liveness from the most recent fire timestamp; this " +
				"counter lets dashboards plot the fire rate (expected: 1 per " +
				"interval per tenant). A flat counter on one tenant means its " +
				"scheduler goroutine is dead.",
		}, []string{"tenant"}),
		CycleDurationSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ducklake_metrics_cycle_duration_seconds",
			Help: "Wall-clock duration of the most recent cycle (all queries " +
				"serially plus the end-of-cycle pool stat emit). If this " +
				"approaches the cycle interval the scheduler will start killing " +
				"overruns; alert at >0.8 × interval.",
		}, []string{"tenant"}),
		CycleCompletedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_cycle_completed_total",
			Help: "Cumulative count of cycles that ran every query and emitted " +
				"pool stats before the next ticker tick. Should track " +
				"scheduler_fires_total closely; a gap is killed cycles.",
		}, []string{"tenant"}),
		CycleKilledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ducklake_metrics_cycle_killed_total",
			Help: "Cumulative count of cycles canceled because the next ticker " +
				"tick fired before the previous cycle finished. Indicates the " +
				"cycle interval is too short for this tenant's query workload " +
				"(or the catalog is slow).",
		}, []string{"tenant"}),
	}
	reg.MustRegister(
		self.Up,
		self.QueryDurationSeconds,
		self.QueryLastSuccessSeconds,
		self.QueryErrorsTotal,
		self.LivenessFailuresTotal,
		self.ConnectFailuresTotal,
		self.TenantRestartsTotal,
		self.QueryPanicsTotal,
		self.PoolAcquiredConns,
		self.PoolIdleConns,
		self.PoolTotalConns,
		self.PoolEmptyAcquireTotal,
		self.SchedulerFiresTotal,
		self.CycleDurationSeconds,
		self.CycleCompletedTotal,
		self.CycleKilledTotal,
	)

	gauges := QueryGauges{}
	for _, q := range qs {
		// Label set is `["tenant"] + q.Labels`. Tenant prepend matches
		// the Python daemon's _build_query_gauges shape so existing
		// dashboards' label selectors work unchanged.
		labels := append([]string{"tenant"}, q.Labels...)
		gauges[q.Name] = map[string]*prometheus.GaugeVec{}
		for _, v := range q.Values {
			name := q.Name + "_" + v
			g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: name,
				Help: q.Help,
			}, labels)
			reg.MustRegister(g)
			gauges[q.Name][v] = g
		}
	}
	return reg, self, gauges
}
