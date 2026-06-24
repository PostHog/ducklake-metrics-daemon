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
