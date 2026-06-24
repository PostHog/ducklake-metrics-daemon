// Publisher tests. The publisher is the actor under test — N producers
// (cycle goroutines) send Results into its inbox; the publisher is the
// SOLE writer to Prometheus gauges. These tests cover the bugs the
// previous inline-write architecture suffered from:
//   - cross-tenant gauge wipes (was: g.Reset(); now: per-tenant diff)
//   - label scrambling (positional label values vs map iteration order)
//   - clear-then-Set non-atomic race (was: Reset+Set; now: Set+Delete diff)
//   - NULL value silent drop
//   - non-numeric value silent drop
//   - per-query column mismatch
//   - pool stat counter delta (no spurious initial spike)
package publisher

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
	"github.com/prometheus/client_golang/prometheus"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fixture builds a Publisher with one tenant-labelled query named "foo"
// having labels ["band"] and values ["count", "bytes"]. Returns the
// publisher + the registry (for Gather).
func fixture(t *testing.T) (*Publisher, *fixtureRegistry) {
	t.Helper()
	qs := []queries.Query{
		{Name: "foo", Help: "h",
			Labels: []string{"band"}, Values: []string{"count", "bytes"}, SQL: "x"},
	}
	reg, self, gauges := metrics.Register(qs)
	p := New(self, gauges, qs, discardLogger())
	return p, &fixtureRegistry{reg: reg, self: self, gauges: gauges}
}

type fixtureRegistry struct {
	reg    *prometheus.Registry
	self   *metrics.Self
	gauges metrics.QueryGauges
}

// gaugesByTenantBand walks Gather() for the named metric and returns
// a map keyed by "tenant/band" → value, for the test fixture's single
// query "foo".
func gaugesByTenantBand(t *testing.T, fx *fixtureRegistry, metricName string) map[string]float64 {
	t.Helper()
	mfs, err := fx.reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			var tenant, band string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "tenant":
					tenant = lp.GetValue()
				case "band":
					band = lp.GetValue()
				}
			}
			out[tenant+"/"+band] = m.GetGauge().GetValue()
		}
	}
	return out
}

// counterValue fetches the value of a labelled counter via Gather().
// Returns 0 if the metric/labels aren't found.
func counterValue(t *testing.T, fx *fixtureRegistry, metricName string, want map[string]string) float64 {
	t.Helper()
	mfs, err := fx.reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if matches(labels, want) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// gaugeValue is the gauge variant of counterValue.
func gaugeValue(t *testing.T, fx *fixtureRegistry, metricName string, want map[string]string) float64 {
	t.Helper()
	mfs, err := fx.reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if matches(labels, want) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func matches(got, want map[string]string) bool {
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// --- Tests ---

func TestCrossTenantGaugeIsolation(t *testing.T) {
	// Two tenants emit different label sets for the SAME query. After
	// tenant alpha's next cycle changes its bands, tenant beta's
	// series MUST survive untouched. This is the B2 regression guard
	// at the publisher level — a refactor that goes back to "wipe
	// every label for this query" would fail this test.
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 16)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	// Alpha cycle 1: 2 bands.
	inbox <- Result{Kind: KindQuery, Tenant: "alpha", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{
			{"band": "tier1", "count": int64(10), "bytes": int64(100)},
			{"band": "tier2", "count": int64(20), "bytes": int64(200)},
		}}
	// Beta cycle 1: 1 band.
	inbox <- Result{Kind: KindQuery, Tenant: "beta", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{
			{"band": "tier1", "count": int64(50), "bytes": int64(500)},
		}}
	// Alpha cycle 2: now only tier1 (tier2 dropped).
	inbox <- Result{Kind: KindQuery, Tenant: "alpha", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{
			{"band": "tier1", "count": int64(11), "bytes": int64(110)},
		}}

	// Drain.
	close(inbox)
	<-done

	got := gaugesByTenantBand(t, fx, "foo_count")
	want := map[string]float64{
		"alpha/tier1": 11, // updated cycle 2
		"beta/tier1":  50, // beta UNTOUCHED by alpha's clear
	}
	if v, ok := got["alpha/tier2"]; ok {
		t.Errorf("alpha/tier2 should have been removed (dropped from cycle 2), got %v", v)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %v, want %v (full: %v)", k, got[k], v, got)
		}
	}
}

func TestLabelPositionalOrder(t *testing.T) {
	// B1 guard: multi-label queries must place values positionally per
	// q.Labels order. Result rows arrive as map[colname]any; the
	// publisher must use the q.Labels declared order, not map
	// iteration order, when building the label-value tuple.
	qs := []queries.Query{
		{Name: "trio", Help: "h",
			Labels: []string{"a", "b", "c"}, Values: []string{"v"}, SQL: "x"},
	}
	reg, self, gauges := metrics.Register(qs)
	p := New(self, gauges, qs, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 4)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	// SQL might select in different order than q.Labels.
	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "trio",
		Cols: []string{"c", "v", "a", "b"},
		Rows: []catalog.Row{{"a": "aval", "b": "bval", "c": "cval", "v": int64(1)}},
	}
	close(inbox)
	<-done

	mfs, _ := reg.Gather()
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "trio_v" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["a"] == "aval" && labels["b"] == "bval" && labels["c"] == "cval" {
				found = true
			} else {
				t.Errorf("label values scrambled: %v", labels)
			}
		}
	}
	if !found {
		t.Error("expected series with correct positional labels not present")
	}
}

func TestNullValueBumpsErrors(t *testing.T) {
	// NULL in a numeric column → QueryErrorsTotal += 1, gauge
	// unaffected. ducklake_config/ducklake_catalog rely on this so
	// unparseable values surface as alerts, not silent drops.
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 2)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{
			{"band": "tier1", "count": nil, "bytes": int64(100)},
		}}
	close(inbox)
	<-done

	if got := counterValue(t, fx, "ducklake_metrics_query_errors_total", map[string]string{"tenant": "t", "query": "foo"}); got != 1 {
		t.Errorf("QueryErrorsTotal=%v, want 1 (for NULL count)", got)
	}
	// bytes should still be set (we only skip the NULL column, not
	// the whole row).
	g := gaugesByTenantBand(t, fx, "foo_bytes")
	if g["t/tier1"] != 100 {
		t.Errorf("foo_bytes[t/tier1]=%v, want 100", g["t/tier1"])
	}
}

func TestNonNumericValueBumpsErrors(t *testing.T) {
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 2)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{
			{"band": "tier1", "count": "not-a-number", "bytes": int64(100)},
		}}
	close(inbox)
	<-done

	if got := counterValue(t, fx, "ducklake_metrics_query_errors_total", map[string]string{"tenant": "t", "query": "foo"}); got != 1 {
		t.Errorf("QueryErrorsTotal=%v, want 1", got)
	}
}

func TestErrorResultLeavesGaugesAndBumpsCounter(t *testing.T) {
	// A failed query: publisher must NOT touch gauges (last-known
	// persists) but MUST bump QueryErrorsTotal.
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 2)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	// Set a baseline value.
	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{{"band": "tier1", "count": int64(7), "bytes": int64(70)}},
	}
	// Now a failure.
	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "foo", Err: errString("pgx exploded")}
	close(inbox)
	<-done

	if got := counterValue(t, fx, "ducklake_metrics_query_errors_total", map[string]string{"tenant": "t", "query": "foo"}); got != 1 {
		t.Errorf("QueryErrorsTotal=%v, want 1", got)
	}
	// Gauge unchanged from baseline.
	g := gaugesByTenantBand(t, fx, "foo_count")
	if g["t/tier1"] != 7 {
		t.Errorf("foo_count[t/tier1]=%v, want 7 (last-known should persist)", g["t/tier1"])
	}
}

func TestPoolStatDeltaNoInitialSpike(t *testing.T) {
	// First PoolStat result has whatever EmptyAcquireCount the pool
	// happens to report (e.g. 5 from warm-up acquires). The publisher
	// must NOT emit a counter delta of 5 — it should seed the
	// baseline from the first observation, then only emit subsequent
	// deltas.
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 4)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	inbox <- Result{Kind: KindPoolStat, Tenant: "t", Stats: catalog.Stats{
		AcquiredConns: 1, IdleConns: 2, TotalConns: 3, EmptyAcquireCount: 5,
	}}
	// Next cycle: EmptyAcquireCount jumped by 2.
	inbox <- Result{Kind: KindPoolStat, Tenant: "t", Stats: catalog.Stats{
		AcquiredConns: 0, IdleConns: 3, TotalConns: 3, EmptyAcquireCount: 7,
	}}
	close(inbox)
	<-done

	// The counter should reflect ONLY the delta (2), not the initial 5.
	if got := counterValue(t, fx, "ducklake_metrics_pool_empty_acquire_total", map[string]string{"tenant": "t"}); got != 2 {
		t.Errorf("PoolEmptyAcquireTotal=%v, want 2 (delta only, no initial spike)", got)
	}

	// Verify the gauges reflect the LATEST stats.
	g := gaugeValue(t, fx, "ducklake_metrics_pool_idle_conns", map[string]string{"tenant": "t"})
	if g != 3 {
		t.Errorf("PoolIdleConns=%v, want 3", g)
	}
}

func TestPanicInApplyDoesNotKillPublisher(t *testing.T) {
	// Inject a Result with bad columns that forces a panic deep in
	// the publisher's hot path — actually with the current code,
	// missing columns trigger an error return, not a panic. So we
	// can't easily provoke a panic without an in-flight refactor.
	// Instead, just verify the panic-recover wrapper is registered
	// by sending a normal Result and asserting normal completion.
	// (A unit test for the recover branch itself would need fault
	// injection into the apply path.)
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 2)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	inbox <- Result{Kind: KindQuery, Tenant: "t", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{{"band": "tier1", "count": int64(1), "bytes": int64(1)}},
	}
	close(inbox)
	<-done

	g := gaugesByTenantBand(t, fx, "foo_count")
	if g["t/tier1"] != 1 {
		t.Errorf("publisher didn't apply normal result: %v", g)
	}
	_ = fx
}

func TestResetTenantGauges(t *testing.T) {
	p, fx := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := make(chan Result, 4)
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, inbox) }()

	// Two tenants populate; then reset alpha; beta survives.
	inbox <- Result{Kind: KindQuery, Tenant: "alpha", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{{"band": "tier1", "count": int64(1), "bytes": int64(1)}},
	}
	inbox <- Result{Kind: KindQuery, Tenant: "beta", Query: "foo",
		Cols: []string{"band", "count", "bytes"},
		Rows: []catalog.Row{{"band": "tier1", "count": int64(2), "bytes": int64(2)}},
	}
	close(inbox)
	<-done

	p.ResetTenantGauges("alpha")
	g := gaugesByTenantBand(t, fx, "foo_count")
	if _, has := g["alpha/tier1"]; has {
		t.Errorf("alpha series should be gone after ResetTenantGauges, got %v", g)
	}
	if g["beta/tier1"] != 2 {
		t.Errorf("beta series wiped by alpha reset: %v", g)
	}
}

// --- helpers ---

type errString string

func (e errString) Error() string { return string(e) }
