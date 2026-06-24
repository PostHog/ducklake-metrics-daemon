package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/ducklake-metrics-daemon/internal/metrics"
	"github.com/posthog/ducklake-metrics-daemon/internal/runner"
)

func TestLivenessStatusStates(t *testing.T) {
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	interval := 5 * time.Minute
	threshold := time.Duration(staleScheduleMultiplier) * interval

	cases := []struct {
		name       string
		snap       runner.LivenessSnapshot
		wantAlive  bool
		wantReason LivenessReason
	}{
		{
			"starting (scheduler hasn't fired yet)",
			runner.LivenessSnapshot{Interval: interval},
			true, ReasonStarting,
		},
		{
			"ok: fired recently",
			runner.LivenessSnapshot{Interval: interval, LastFire: now.Add(-1 * time.Minute)},
			true, ReasonOK,
		},
		{
			"ok: fired exactly at threshold edge (still alive — strict >)",
			runner.LivenessSnapshot{Interval: interval, LastFire: now.Add(-threshold)},
			true, ReasonOK,
		},
		{
			"stale_schedule: threshold + 1ns",
			runner.LivenessSnapshot{Interval: interval, LastFire: now.Add(-threshold - time.Nanosecond)},
			false, ReasonStaleSchedule,
		},
		{
			"stale_schedule: scheduler dead for an hour",
			runner.LivenessSnapshot{Interval: interval, LastFire: now.Add(-1 * time.Hour)},
			false, ReasonStaleSchedule,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alive, reason, _ := LivenessStatus(tc.snap, now)
			if alive != tc.wantAlive {
				t.Errorf("alive=%v, want %v", alive, tc.wantAlive)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%v, want %v", reason, tc.wantReason)
			}
		})
	}
}

type readyMap struct {
	mu sync.Mutex
	m  map[string]bool
}

func (r *readyMap) ready(t string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[t]
}

func snapshotsFrom(snaps map[string]runner.LivenessSnapshot) SnapshotsFunc {
	return func() map[string]runner.LivenessSnapshot {
		out := make(map[string]runner.LivenessSnapshot, len(snaps))
		for k, v := range snaps {
			out[k] = v
		}
		return out
	}
}

func newServer(t *testing.T, snaps map[string]runner.LivenessSnapshot, ready map[string]bool) *Server {
	t.Helper()
	reg, self, _ := metrics.Register(nil)
	rm := &readyMap{m: ready}
	return New(Config{
		Addr:      ":0",
		Registry:  reg,
		Self:      self,
		Snapshots: snapshotsFrom(snaps),
		Ready:     rm.ready,
	})
}

// helpers — fresh snapshots in named states.
func snapStarting() runner.LivenessSnapshot {
	return runner.LivenessSnapshot{Interval: 5 * time.Minute}
}

func snapOK(now time.Time) runner.LivenessSnapshot {
	return runner.LivenessSnapshot{Interval: 5 * time.Minute, LastFire: now.Add(-30 * time.Second)}
}

func snapStale(now time.Time) runner.LivenessSnapshot {
	return runner.LivenessSnapshot{Interval: 5 * time.Minute, LastFire: now.Add(-30 * time.Minute)}
}

func TestHandleHealthyAllTenantsOK(t *testing.T) {
	s := newServer(t,
		map[string]runner.LivenessSnapshot{
			"alpha": snapStarting(),
			"beta":  snapOK(time.Now()),
		},
		map[string]bool{"alpha": true, "beta": true})

	w := httptest.NewRecorder()
	s.handleHealthy(w, httptest.NewRequest(http.MethodGet, "/-/healthy", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "2 tenant") {
		t.Errorf("body=%q, want tenant count", w.Body.String())
	}
}

func TestHandleHealthyOneTenantStale(t *testing.T) {
	now := time.Now()
	s := newServer(t,
		map[string]runner.LivenessSnapshot{
			"alpha": snapOK(now),
			"beta":  snapStale(now),
		},
		map[string]bool{"alpha": true, "beta": true})

	w := httptest.NewRecorder()
	s.handleHealthy(w, httptest.NewRequest(http.MethodGet, "/-/healthy", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "beta:") || !strings.Contains(body, "stale_schedule") {
		t.Errorf("body=%q, want beta + stale_schedule", body)
	}
	if strings.Contains(body, "alpha:") {
		t.Errorf("body=%q, healthy alpha leaked into failure list", body)
	}
}

func TestHandleHealthyMultipleFailuresListed(t *testing.T) {
	now := time.Now()
	s := newServer(t,
		map[string]runner.LivenessSnapshot{
			"alpha": snapStale(now),
			"beta":  snapStale(now),
			"gamma": snapOK(now),
		},
		map[string]bool{"alpha": true, "beta": true, "gamma": true})

	w := httptest.NewRecorder()
	s.handleHealthy(w, httptest.NewRequest(http.MethodGet, "/-/healthy", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "alpha:") || !strings.Contains(body, "beta:") {
		t.Errorf("body=%q, want both stale tenants listed", body)
	}
	if strings.Contains(body, "gamma:") {
		t.Errorf("body=%q, healthy gamma should not appear", body)
	}
}

func TestHandleReadyNoneReady(t *testing.T) {
	s := newServer(t,
		map[string]runner.LivenessSnapshot{
			"alpha": snapStarting(),
			"beta":  snapStarting(),
		},
		map[string]bool{})

	w := httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/-/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReadyAnyReady(t *testing.T) {
	s := newServer(t,
		map[string]runner.LivenessSnapshot{
			"alpha": snapOK(time.Now()),
			"beta":  snapStarting(),
		},
		map[string]bool{"alpha": true})

	w := httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/-/ready", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "beta") {
		t.Errorf("body=%q, want beta listed as still-connecting", w.Body.String())
	}
}

func TestHandleReadyAllReady(t *testing.T) {
	s := newServer(t,
		map[string]runner.LivenessSnapshot{"alpha": snapOK(time.Now())},
		map[string]bool{"alpha": true})

	w := httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/-/ready", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d, want 200", w.Code)
	}
	if strings.Contains(w.Body.String(), "still connecting") {
		t.Errorf("body=%q, should not mention still-connecting", w.Body.String())
	}
}

func TestMuxRoutes(t *testing.T) {
	s := newServer(t,
		map[string]runner.LivenessSnapshot{"alpha": snapOK(time.Now())},
		map[string]bool{"alpha": true})

	srv := httptest.NewServer(s.Mux())
	defer srv.Close()

	for path, wantStatus := range map[string]int{
		"/metrics":   http.StatusOK,
		"/-/healthy": http.StatusOK,
		"/-/ready":   http.StatusOK,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Errorf("GET %s: %v", path, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != wantStatus {
			t.Errorf("GET %s: status=%d, want %d", path, resp.StatusCode, wantStatus)
		}
	}
}
