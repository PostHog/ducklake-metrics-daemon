// Package publisher is the single-writer actor that owns every
// Prometheus per-query gauge write in the daemon. N per-tenant cycle
// goroutines produce [Result] messages; this package's Publisher
// consumes them serially and applies them to gauges.
//
// Why single-writer: with shared cross-tenant gauges (every series
// carries a `tenant` label), inline writes from N tenant goroutines
// risk wiping each other's series on the per-query clear step. Single-
// writer makes that bug class impossible by construction — the
// publisher is the only thing touching gauges, so the clear-then-
// repopulate sequence is serial against itself.
//
// Per-(tenant, query) the publisher tracks the label tuples it last
// emitted and computes a symmetric diff per Result: it deletes only
// tuples that dropped out and writes the new ones. There's no
// observable "all zero rows" window for a scraping Prometheus, unlike
// a naive clear-then-set.
package publisher

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/posthog/ducklake-metrics-daemon/internal/catalog"
	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
	"github.com/prometheus/client_golang/prometheus"
)

// Kind identifies what shape of Result a producer sent.
type Kind int

const (
	// KindQuery: Rows/Cols/Err describe one query's outcome.
	KindQuery Kind = iota
	// KindPoolStat: Stats is the pgxpool snapshot from end-of-cycle.
	KindPoolStat
)

// Result is the message N tenant cycle goroutines send to the
// publisher. KindQuery results carry the query rows + columns; the
// publisher does the label/value extraction. KindPoolStat results
// carry pool counters; the publisher mirrors them into gauges.
type Result struct {
	Kind   Kind
	Tenant string
	Query  string        // KindQuery only — the query name
	Rows   []catalog.Row // KindQuery only
	Cols   []string      // KindQuery only — source order from FieldDescriptions
	Err    error         // KindQuery only — non-nil means the query failed
	Stats  catalog.Stats // KindPoolStat only
}

// Publisher is the actor. Construct via [New], invoke [Publisher.Run].
type Publisher struct {
	self   *metrics.Self
	gauges metrics.QueryGauges
	qs     []queries.Query // for label/value column lookups
	log    *slog.Logger
	// lastLabels[tenant][query][sig] = the label-value slice we
	// emitted last cycle for that signature. We keep the slice form
	// (not just a set of sigs) so DeleteLabelValues can be called
	// positionally when a tuple drops out. Sig is a stable
	// canonicalization of the slice (NUL-joined).
	lastLabels map[string]map[string]map[string][]string
	// emptyAcquireBaselines tracks last-observed EmptyAcquireCount per
	// tenant so we emit only the DELTA into the Prometheus counter.
	// emptyAcquireSeen[tenant] gates emission: the very first
	// observation seeds the baseline without emitting (otherwise an
	// initial counter of 5 from pool warm-up would spike the
	// counter on startup).
	emptyAcquireBaselines map[string]int64
	emptyAcquireSeen      map[string]bool
	// querySpec is a precomputed q.Name → spec lookup so the hot path
	// doesn't linear-scan qs every Result.
	querySpec map[string]queries.Query
}

// New constructs a Publisher. self/gauges/qs are the same triple
// metrics.Register returns.
func New(self *metrics.Self, gauges metrics.QueryGauges, qs []queries.Query, log *slog.Logger) *Publisher {
	spec := make(map[string]queries.Query, len(qs))
	for _, q := range qs {
		spec[q.Name] = q
	}
	return &Publisher{
		self:                  self,
		gauges:                gauges,
		qs:                    qs,
		log:                   log,
		lastLabels:            map[string]map[string]map[string][]string{},
		emptyAcquireBaselines: map[string]int64{},
		emptyAcquireSeen:      map[string]bool{},
		querySpec:             spec,
	}
}

// Run consumes from inbox until it's closed or ctx is canceled.
// Returns when the input is drained — main waits on this before
// shutting down the HTTP server so any in-flight scrapes see the
// final state.
//
// Panic-safe: a panic inside Apply (programmer bug — bad gauge label
// arity, etc.) bumps QueryPanicsTotal with query="_publisher" and the
// loop continues. The publisher is single-point-of-failure for every
// tenant's gauges; we cannot afford to die.
func (p *Publisher) Run(ctx context.Context, inbox <-chan Result) {
	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-inbox:
			if !ok {
				return // sender side closed; clean drain
			}
			p.safeApply(r)
		}
	}
}

func (p *Publisher) safeApply(r Result) {
	defer func() {
		if rec := recover(); rec != nil {
			p.log.Error("publisher apply panic recovered",
				"tenant", r.Tenant,
				"query", r.Query,
				"panic", fmt.Sprintf("%v", rec),
				"stack", string(debug.Stack()),
			)
			p.self.QueryPanicsTotal.WithLabelValues(r.Tenant, "_publisher").Inc()
		}
	}()
	p.apply(r)
}

func (p *Publisher) apply(r Result) {
	switch r.Kind {
	case KindQuery:
		p.applyQuery(r)
	case KindPoolStat:
		p.applyPoolStat(r)
	default:
		p.log.Warn("unknown result kind", "kind", r.Kind, "tenant", r.Tenant)
	}
}

func (p *Publisher) applyQuery(r Result) {
	if r.Err != nil {
		p.self.QueryErrorsTotal.WithLabelValues(r.Tenant, r.Query).Inc()
		// Leave last-known gauge values in place — a transient flap
		// shouldn't blank the dashboard.
		return
	}
	q, ok := p.querySpec[r.Query]
	if !ok {
		p.log.Warn("result for unknown query (no spec)", "tenant", r.Tenant, "query", r.Query)
		return
	}
	queryGauges, ok := p.gauges[r.Query]
	if !ok {
		p.log.Warn("result for query with no registered gauges", "tenant", r.Tenant, "query", r.Query)
		return
	}

	labelIdx, err := indexLabelColumns(r.Cols, q.Labels)
	if err != nil {
		p.log.Error("column mismatch (label)", "tenant", r.Tenant, "query", r.Query, "err", err)
		p.self.QueryErrorsTotal.WithLabelValues(r.Tenant, r.Query).Inc()
		return
	}
	valueIdx, err := indexValueColumns(r.Cols, q.Values)
	if err != nil {
		p.log.Error("column mismatch (value)", "tenant", r.Tenant, "query", r.Query, "err", err)
		p.self.QueryErrorsTotal.WithLabelValues(r.Tenant, r.Query).Inc()
		return
	}

	// Build the new label-tuple set for this tenant+query. Each tuple
	// is the canonical signature `tenant\x00labelval1\x00labelval2...`
	// — used both as the diff key and as the storage key in lastLabels.
	newTuples := make(map[string][]string, len(r.Rows))
	for _, row := range r.Rows {
		vals := make([]string, 0, 1+len(labelIdx))
		vals = append(vals, r.Tenant)
		for _, i := range labelIdx {
			vals = append(vals, toLabelString(row[r.Cols[i]]))
		}
		newTuples[strings.Join(vals, "\x00")] = vals
	}

	// Diff against last-emitted: delete dropped tuples, then write the
	// current snapshot. No "all zero rows" window because Set happens
	// before Delete? Actually Set first, then Delete: Prometheus
	// gather() is point-in-time, so the order matters. We Set new
	// values first (so they're visible to a scrape), then Delete
	// removed labels.
	last := p.lastLabels[r.Tenant]
	if last == nil {
		last = map[string]map[string][]string{}
		p.lastLabels[r.Tenant] = last
	}
	prev := last[r.Query]

	// Set current values first.
	for _, row := range r.Rows {
		vals := make([]string, 0, 1+len(labelIdx))
		vals = append(vals, r.Tenant)
		for _, i := range labelIdx {
			vals = append(vals, toLabelString(row[r.Cols[i]]))
		}
		for vname, idx := range valueIdx {
			raw := row[r.Cols[idx]]
			if raw == nil {
				// NULL value treated as a query error — operators alert
				// on it. ducklake_catalog/ducklake_config emit NULL for
				// unparseable values, and without this the bad value
				// silently disappears from the dashboard.
				p.log.Warn("null value in numeric column",
					"tenant", r.Tenant, "query", r.Query, "column", vname)
				p.self.QueryErrorsTotal.WithLabelValues(r.Tenant, r.Query).Inc()
				continue
			}
			v, ok := toFloat(raw)
			if !ok {
				p.log.Warn("non-numeric value",
					"tenant", r.Tenant, "query", r.Query,
					"column", vname, "value_type", fmt.Sprintf("%T", raw))
				p.self.QueryErrorsTotal.WithLabelValues(r.Tenant, r.Query).Inc()
				continue
			}
			queryGauges[vname].WithLabelValues(vals...).Set(v)
		}
	}

	// Delete tuples that were present last cycle but not this one.
	for sig, prevVals := range prev {
		if _, stillPresent := newTuples[sig]; stillPresent {
			continue
		}
		// Use the slice of label values for Delete (positional API).
		for _, g := range queryGauges {
			g.DeleteLabelValues(prevVals...)
		}
	}

	// Update tracking with the new set (keep slice form so the next
	// cycle's diff can DeleteLabelValues positionally).
	last[r.Query] = newTuples
	_ = prev // silence linter; used above for the diff
}

func (p *Publisher) applyPoolStat(r Result) {
	p.self.PoolAcquiredConns.WithLabelValues(r.Tenant).Set(float64(r.Stats.AcquiredConns))
	p.self.PoolIdleConns.WithLabelValues(r.Tenant).Set(float64(r.Stats.IdleConns))
	p.self.PoolTotalConns.WithLabelValues(r.Tenant).Set(float64(r.Stats.TotalConns))
	// EmptyAcquireCount is a monotonic pool counter; we mirror it
	// directly to a Prometheus counter via the delta from the last
	// snapshot. Track per-tenant.
	p.bumpEmptyAcquireCounter(r.Tenant, r.Stats.EmptyAcquireCount)
}

func (p *Publisher) bumpEmptyAcquireCounter(tenant string, current int64) {
	if !p.emptyAcquireSeen[tenant] {
		// First observation: seed the baseline, emit nothing.
		// Otherwise an initial pool-warm-up count (e.g. 5 empty
		// acquires from Ping/initial connect) would spike the
		// counter on startup.
		p.emptyAcquireSeen[tenant] = true
		p.emptyAcquireBaselines[tenant] = current
		return
	}
	last := p.emptyAcquireBaselines[tenant]
	if delta := current - last; delta > 0 {
		p.self.PoolEmptyAcquireTotal.WithLabelValues(tenant).Add(float64(delta))
		p.emptyAcquireBaselines[tenant] = current
	} else if current < last {
		// Pool was recreated (fresh pool, counter reset). Re-seed
		// baseline so we don't subtract from a higher number on the
		// next observation.
		p.emptyAcquireBaselines[tenant] = current
	}
}

// ResetTenantGauges deletes every series the publisher has emitted for
// the given tenant (per-query gauges + pool stats). Called by main on
// tenant teardown / connect-loop give-up so dashboards don't show
// stale values for a tenant that no longer has a live pool.
func (p *Publisher) ResetTenantGauges(tenant string) {
	tlabel := prometheus.Labels{"tenant": tenant}
	// Per-query gauges
	for _, perQ := range p.gauges {
		for _, g := range perQ {
			g.DeletePartialMatch(tlabel)
		}
	}
	// Pool stats
	p.self.PoolAcquiredConns.DeletePartialMatch(tlabel)
	p.self.PoolIdleConns.DeletePartialMatch(tlabel)
	p.self.PoolTotalConns.DeletePartialMatch(tlabel)
	// Clear local tracking too — next cycle re-seeds pool-stat
	// baselines (so a reconnect doesn't subtract from a stale value).
	delete(p.lastLabels, tenant)
	delete(p.emptyAcquireBaselines, tenant)
	delete(p.emptyAcquireSeen, tenant)
}

// indexLabelColumns returns positional indices in cols matching each
// name in want, in declared order. Label values are placed positionally
// by their q.Labels order so multi-label queries don't scramble.
func indexLabelColumns(cols, want []string) ([]int, error) {
	byName := make(map[string]int, len(cols))
	for i, c := range cols {
		byName[c] = i
	}
	out := make([]int, 0, len(want))
	for _, name := range want {
		i, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("missing column %q in result; got %v", name, cols)
		}
		out = append(out, i)
	}
	return out, nil
}

// indexValueColumns returns a name → column-index map. Iteration order
// doesn't matter for values (each maps to its own gauge by name).
func indexValueColumns(cols, want []string) (map[string]int, error) {
	byName := make(map[string]int, len(cols))
	for i, c := range cols {
		byName[c] = i
	}
	out := make(map[string]int, len(want))
	for _, name := range want {
		i, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("missing column %q in result; got %v", name, cols)
		}
		out[name] = i
	}
	return out, nil
}

// toLabelString converts a pgx-decoded value to a Prometheus label
// string. Nulls render as empty string.
func toLabelString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// toFloat coerces a pgx-decoded value to float64 for Prometheus gauges.
func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	case int16:
		return float64(t), true
	case int:
		return float64(t), true
	case float64:
		if math.IsNaN(t) {
			return 0, false
		}
		return t, true
	case float32:
		return float64(t), true
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	default:
		// pgtype.Numeric arrives here; round-trip via fmt as a fallback.
		s := fmt.Sprintf("%v", v)
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
}
