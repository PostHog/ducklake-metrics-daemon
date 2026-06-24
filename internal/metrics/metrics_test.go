package metrics

import (
	"testing"

	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
)

func TestRegisterProducesGauges(t *testing.T) {
	qs := []queries.Query{
		{Name: "foo_query", Help: "test", Labels: []string{"band"}, Values: []string{"count", "bytes"}, SQL: "SELECT 1"},
	}
	reg, self, gauges := Register(qs)
	if reg == nil || self == nil || gauges == nil {
		t.Fatal("Register returned nil")
	}
	if _, ok := gauges["foo_query"]; !ok {
		t.Fatal("gauges missing foo_query")
	}
	if _, ok := gauges["foo_query"]["count"]; !ok {
		t.Error("gauges[foo_query] missing count value")
	}
	if _, ok := gauges["foo_query"]["bytes"]; !ok {
		t.Error("gauges[foo_query] missing bytes value")
	}
	// Verify the gauges accept the expected label set: ["tenant", "band"].
	g := gauges["foo_query"]["count"]
	g.WithLabelValues("tenant-a", "band-1").Set(7)

	// Read back via Gather to confirm the metric round-trips through the
	// registry (would fail if Register forgot to MustRegister it).
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "foo_query_count" {
			found = true
			if len(mf.GetMetric()) != 1 {
				t.Errorf("want 1 sample, got %d", len(mf.GetMetric()))
			}
			if got := mf.GetMetric()[0].GetGauge().GetValue(); got != 7 {
				t.Errorf("value=%v, want 7", got)
			}
		}
	}
	if !found {
		t.Error("foo_query_count missing from gathered metrics")
	}
}

func TestRegisterSelfMetricNames(t *testing.T) {
	// Pin every self-metric name — these are the contract with Grafana
	// (and `<name>_total` for the resilience counters).
	reg, self, _ := Register(nil)
	// Force every metric to register a sample so Gather emits its
	// MetricFamily (a vec with no labels-applied is empty in Gather).
	self.Up.WithLabelValues("t").Set(0)
	self.QueryDurationSeconds.WithLabelValues("t", "q").Set(0)
	self.QueryLastSuccessSeconds.WithLabelValues("t", "q").Set(0)
	self.QueryErrorsTotal.WithLabelValues("t", "q").Inc()
	self.LivenessFailuresTotal.WithLabelValues("t", "r").Inc()
	self.ConnectFailuresTotal.WithLabelValues("t", "auth").Inc()
	self.TenantRestartsTotal.WithLabelValues("t", "r").Inc()
	self.QueryPanicsTotal.WithLabelValues("t", "q").Inc()
	self.PoolAcquiredConns.WithLabelValues("t").Set(0)
	self.PoolIdleConns.WithLabelValues("t").Set(0)
	self.PoolTotalConns.WithLabelValues("t").Set(0)
	self.PoolEmptyAcquireTotal.WithLabelValues("t").Inc()
	self.SchedulerFiresTotal.WithLabelValues("t").Inc()
	self.CycleDurationSeconds.WithLabelValues("t").Set(0)
	self.CycleCompletedTotal.WithLabelValues("t").Inc()
	self.CycleKilledTotal.WithLabelValues("t").Inc()

	want := map[string]bool{
		"ducklake_metrics_up":                           false,
		"ducklake_metrics_query_duration_seconds":       false,
		"ducklake_metrics_query_last_success_timestamp": false,
		"ducklake_metrics_query_errors_total":           false,
		"ducklake_metrics_liveness_failures_total":      false,
		"ducklake_metrics_connect_failures_total":       false,
		"ducklake_metrics_tenant_restarts_total":        false,
		"ducklake_metrics_query_panics_total":           false,
		"ducklake_metrics_pool_acquired_conns":          false,
		"ducklake_metrics_pool_idle_conns":              false,
		"ducklake_metrics_pool_total_conns":             false,
		"ducklake_metrics_pool_empty_acquire_total":     false,
		"ducklake_metrics_scheduler_fires_total":        false,
		"ducklake_metrics_cycle_duration_seconds":       false,
		"ducklake_metrics_cycle_completed_total":        false,
		"ducklake_metrics_cycle_killed_total":           false,
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("self-metric %q missing from registry", name)
		}
	}
}

func TestRegisterPerQueryGaugeName(t *testing.T) {
	qs := []queries.Query{
		{Name: "ducklake_data_files", Help: "h", Values: []string{"files", "bytes"}, SQL: "SELECT 1"},
	}
	reg, _, gauges := Register(qs)
	gauges["ducklake_data_files"]["files"].WithLabelValues("alpha").Set(1)
	gauges["ducklake_data_files"]["bytes"].WithLabelValues("alpha").Set(99)

	want := map[string]float64{
		"ducklake_data_files_files": 1,
		"ducklake_data_files_bytes": 99,
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		if v, ok := want[mf.GetName()]; ok {
			for _, m := range mf.GetMetric() {
				got[mf.GetName()] = m.GetGauge().GetValue()
				_ = v
			}
		}
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s=%v, want %v", name, got[name], w)
		}
	}
}

func TestRegisterTenantLabelPrepended(t *testing.T) {
	qs := []queries.Query{
		{Name: "foo", Help: "h", Labels: []string{"a", "b"}, Values: []string{"v"}, SQL: "SELECT 1"},
	}
	_, _, gauges := Register(qs)
	g := gauges["foo"]["v"]
	// Should accept exactly ["tenant", "a", "b"] label values.
	g.WithLabelValues("t1", "av", "bv").Set(1)
	// Calling with the wrong arity panics — that's how we prove tenant
	// is prepended (declared was ["a","b"] = 2; with tenant we expect 3).
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for wrong label arity")
			}
		}()
		g.WithLabelValues("t1", "av").Set(1)
	}()
}
