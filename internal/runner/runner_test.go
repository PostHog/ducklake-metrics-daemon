// Tests for the per-tenant scheduler.
//
// runCycle hitting catalog.Catalog.Query requires a real Postgres and
// is deferred to integration tests. What we cover here:
//   - Liveness (atomic timestamp; lock-free reads under concurrent writes)
//   - stagger() determinism
//
// The scheduler's outer loop (Run) is tested by spinning up Run with a
// nil Catalog would crash on the first cycle. Without catalog mock
// extraction it's not unit-testable; covered by smoke tests in main.
package runner

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/posthog/ducklake-metrics-daemon/internal/queries"
)

func TestLivenessInitial(t *testing.T) {
	l := NewLiveness(5 * time.Minute)
	s := l.Snapshot()
	if s.Interval != 5*time.Minute {
		t.Errorf("Interval=%v, want 5m", s.Interval)
	}
	if !s.LastFire.IsZero() {
		t.Errorf("LastFire=%v, want zero", s.LastFire)
	}
}

func TestLivenessMarkFire(t *testing.T) {
	l := NewLiveness(5 * time.Minute)
	before := time.Now()
	l.MarkFire()
	after := time.Now()
	s := l.Snapshot()
	if s.LastFire.Before(before) || s.LastFire.After(after) {
		t.Errorf("LastFire=%v not in [%v, %v]", s.LastFire, before, after)
	}
}

func TestLivenessSnapshotConcurrent(t *testing.T) {
	// Race-detector canary. MarkFire writes lastFire atomically;
	// Snapshot reads atomically. Both should be lock-free and safe
	// under arbitrary interleaving. -race catches any data race.
	prevCPUs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prevCPUs)

	l := NewLiveness(7 * time.Second)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := l.Snapshot()
					if s.Interval != 7*time.Second {
						t.Errorf("Interval mutated: %v", s.Interval)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.MarkFire()
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestStaggerDeterministic(t *testing.T) {
	// Same tenant id should always produce the same offset across
	// runs; different tenants should land at (probably) different
	// offsets. The point is to spread N tenants across the interval
	// without colliding on the same wall-clock instant on every pod
	// restart.
	interval := 5 * time.Minute
	a1 := stagger("alpha", interval)
	a2 := stagger("alpha", interval)
	if a1 != a2 {
		t.Errorf("stagger(alpha) not deterministic: %v vs %v", a1, a2)
	}
	if a1 < 0 || a1 >= interval {
		t.Errorf("stagger out of [0, interval): %v (interval %v)", a1, interval)
	}

	b := stagger("beta", interval)
	if b == a1 {
		t.Logf("alpha and beta happen to hash to the same offset (%v); not a bug, but unlikely with fnv1a", a1)
	}
}

func TestStaggerZeroInterval(t *testing.T) {
	if got := stagger("anything", 0); got != 0 {
		t.Errorf("stagger(_, 0)=%v, want 0", got)
	}
}

func TestDueQueriesNoIntervalReturnsAllUnchanged(t *testing.T) {
	qs := []queries.Query{{Name: "a"}, {Name: "b"}}
	last := map[string]time.Time{}
	got := dueQueries(qs, last, time.Now())
	if len(got) != 2 {
		t.Fatalf("want all 2 queries, got %d", len(got))
	}
	// No-interval path must not record run times (and must not allocate a
	// new slice — it returns the input).
	if len(last) != 0 {
		t.Errorf("no-interval path should not touch lastRun, got %d entries", len(last))
	}
}

func TestDueQueriesRespectsInterval(t *testing.T) {
	qs := []queries.Query{
		{Name: "fast"},                        // every cycle
		{Name: "slow", IntervalSeconds: 3600}, // hourly
	}
	last := map[string]time.Time{}
	t0 := time.Unix(1_000_000, 0)

	// First fire: both due (slow has no prior run).
	got := dueQueries(qs, last, t0)
	if len(got) != 2 {
		t.Fatalf("first fire: want 2, got %d", len(got))
	}

	// 5 min later: only fast is due; slow's window hasn't elapsed.
	got = dueQueries(qs, last, t0.Add(5*time.Minute))
	if len(got) != 1 || got[0].Name != "fast" {
		t.Fatalf("second fire: want [fast], got %+v", names(got))
	}

	// 61 min after t0: slow is due again.
	got = dueQueries(qs, last, t0.Add(61*time.Minute))
	if len(got) != 2 {
		t.Fatalf("hourly fire: want 2, got %+v", names(got))
	}
}

func names(qs []queries.Query) []string {
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q.Name
	}
	return out
}

func TestDueQueriesHourlyOverManyCycles(t *testing.T) {
	// 5-min cycles, one hourly query: over 24 cycles (2h) it must fire
	// exactly twice (cycle 0 and cycle 12), never in between.
	qs := []queries.Query{{Name: "fast"}, {Name: "slow", IntervalSeconds: 3600}}
	last := map[string]time.Time{}
	base := time.Unix(2_000_000, 0)
	slowFires := 0
	for i := 0; i < 24; i++ {
		due := dueQueries(qs, last, base.Add(time.Duration(i)*5*time.Minute))
		for _, q := range due {
			if q.Name == "slow" {
				slowFires++
			}
		}
	}
	if slowFires != 2 {
		t.Fatalf("hourly query over 24×5min cycles: want 2 fires, got %d", slowFires)
	}
}

func TestDueQueriesNoIntervalReturnsSameBackingArray(t *testing.T) {
	// The common (no custom interval) path must return the input slice
	// itself — no per-cycle allocation.
	qs := []queries.Query{{Name: "a"}, {Name: "b"}}
	got := dueQueries(qs, map[string]time.Time{}, time.Now())
	if len(got) != len(qs) || &got[0] != &qs[0] {
		t.Fatalf("no-interval path must return the same backing array, got a copy")
	}
}
