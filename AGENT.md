# ducklake-metrics-daemon — Agent Guidance

## What this is

A long-lived Go daemon that runs catalog-side SQL queries against a DuckLake's Postgres metadata store and exposes the results as Prometheus gauges over HTTP. Direct Postgres client (`pgx`); no DuckDB. The Go rewrite of millpond's `tools/ducklake_metrics.py`.

Same env-var contract, same query library (YAML format mirrored), same Prometheus metric names — so the existing Grafana dashboards (`grafana-dashboards/managed-warehouse/ducklake-state.json`) work unchanged against either implementation.

## Pre-push checklist

```bash
just fmt-check
just vet
just lint            # golangci-lint (brew install golangci-lint)
just test            # go test -race ./...
```

All must pass. `just ci` runs them in order; use that.

## Why Go, why now, why drop DuckDB

The Python+DuckDB implementation worked for n=1 catalog but doesn't scale past ~20-50 (DuckDB's ATTACH-time in-memory catalog model is ~50-100Mi per tenant; with one Deployment per catalog the pod-count overhead grows linearly). The architectural lever isn't language — it's that **none of the metric queries need DuckDB**. They're plain SQL aggregates against `public.ducklake_*` in Postgres; DuckLake's `__ducklake_metadata_lake.*` schema is a DuckDB-side alias that DuckLake's extension translates to `public.*` at parse time. Dropping DuckDB drops the per-tenant footprint two orders of magnitude. Once that decision is made, Go is the natural fit: tiny static binary (~20MB), cheap concurrency via goroutines for the eventual multi-tenant rotation, no GIL.

The Python implementation is still in `millpond/tools/ducklake_metrics.py` and continues to run (per-tenant Deployments via the viaduck chart) until this daemon replaces it.

## Schema rewrite — important when adding queries

The Python daemon's built-in queries reference `__ducklake_metadata_lake.<table>` — the DuckLake-extension-provided alias that's only valid inside a DuckDB session with the lake ATTACHed. On raw Postgres the catalog tables live in `public.*`. **Any new query you add to `internal/queries/builtin.yaml` (or operator YAML) must use `public.ducklake_*`, not the `__ducklake_metadata_lake.*` alias.**

This is enforced at runtime only — pgx will fail with a schema-not-found error if a query uses the alias. The user-facing YAML format does NOT translate; what the operator writes is what pgx receives.

When porting a new built-in query from millpond's `BUILTIN_YAML`, the only change needed is the schema rewrite — everything else (name, help, interval_mins, labels, values) lifts over verbatim.

## Architecture

Actor-style. Three roles, none of them touch each other's state directly:

```
                                                 ┌────────────┐
  per-tenant scheduler (runner.Run)              │ publisher  │
  ┌─────────────────────────────────────────┐    │  (single   │
  │ ticker fires every CycleInterval       ─┼──► │  writer    │
  │ → kills previous cycle if still running │    │  to gauges)│
  │ → launches fresh cycle goroutine        │    └─────┬──────┘
  └─────────────────────────────────────────┘          │
                       │                                ▼
                       ▼                          Prometheus registry
       per-cycle goroutine (one per tick):              ▲
       ┌──────────────────────────────────┐             │
       │ for q in queries:                │             │
       │   cat.Query(ctx, q.SQL)          │             │
       │   send Result → publisher inbox ─┼─────────────┘
       │ send PoolStat → publisher inbox  │
       │ (then die)                       │
       └──────────────────────────────────┘
```

Why single-writer for gauges: with shared cross-tenant gauges (every series carries a `tenant` label), inline writes from N tenant goroutines used to risk wiping each other's series on the per-query clear step. With one writer this bug class is impossible. The publisher tracks per-(tenant, query) label tuples it last emitted and computes a symmetric diff per Result — atomic-from-the-scraper's-perspective; no all-zero-rows window.

Why kill-previous-cycle: the scheduler ticker has no idea how long the previous cycle took. If it's still running when the next tick fires (overrun), we cancel it (`context.WithCancel` per cycle) and the kill is metered (`cycle_killed_total`). Cycle's deferred cleanup observes `ctx.Err()` and finishes its bookkeeping. The new cycle starts immediately on the fresh tick.

Why per-tenant scheduler stagger: 50 tenants firing at `t=0, t=300s, t=600s` would dog-pile RDS. Deterministic per-tenant offset (`fnv1a(tenant) % interval`) spreads them across the interval window; same tenant always gets the same offset, so logs are reproducible.

## Package layout

```
cmd/ducklake-metrics-daemon/main.go    entry point; signal handling; backoff connect; wires scheduler + publisher + HTTP
internal/config/                       env-var loading + validation
internal/queries/                      YAML loader + builtin.yaml (embedded)
internal/catalog/                      pgxpool wrapper; per-tenant
internal/runner/                       per-tenant scheduler (Run + runCycle); Liveness (atomic lastFire timestamp)
internal/publisher/                    single-writer actor that owns every gauge Set/Delete
internal/metrics/                      Prometheus registry + per-query gauge construction
internal/server/                       HTTP /metrics + /-/healthy + /-/ready
```

`internal/` is Go's "private package" convention — nothing outside this module can import these. We don't have a `pkg/` because nothing here is meant for external consumption.

## Self-metrics

| Name | Type | Labels | What it tracks |
|---|---|---|---|
| `ducklake_metrics_up` | gauge | tenant | 1 while connected, 0 during reconnect; flips on connect-with-backoff success/failure |
| `ducklake_metrics_query_duration_seconds` | gauge | tenant, query | Wall-clock of most recent run per query |
| `ducklake_metrics_query_last_success_timestamp` | gauge | tenant, query | Unix ts of most recent successful run |
| `ducklake_metrics_query_errors_total` | counter | tenant, query | Cumulative failed query runs (incl. NULL/non-numeric value-cells) |
| `ducklake_metrics_query_panics_total` | counter | tenant, query | Panics caught inside a query or cycle |
| `ducklake_metrics_connect_failures_total` | counter | tenant, reason | Failed `catalog.Open` attempts, by reason (see below) |
| `ducklake_metrics_tenant_restarts_total` | counter | tenant, reason | Reserved for outer-panic recovery; unused in normal operation |
| `ducklake_metrics_liveness_failures_total` | counter | tenant, reason | /-/healthy 503 occurrences |
| `ducklake_metrics_scheduler_fires_total` | counter | tenant | Cycle-fire events; should track ticker rate (1 per interval per tenant) |
| `ducklake_metrics_cycle_duration_seconds` | gauge | tenant | Wall-clock of most recent cycle (all queries + pool stats) |
| `ducklake_metrics_cycle_completed_total` | counter | tenant | Cycles that finished within their interval window |
| `ducklake_metrics_cycle_killed_total` | counter | tenant | Cycles canceled because the next tick fired first |
| `ducklake_metrics_pool_acquired_conns` | gauge | tenant | pgxpool acquired connections |
| `ducklake_metrics_pool_idle_conns` | gauge | tenant | pgxpool idle connections |
| `ducklake_metrics_pool_total_conns` | gauge | tenant | pgxpool total connections |
| `ducklake_metrics_pool_empty_acquire_total` | counter | tenant | pgxpool acquire-on-empty events (delta-only, no startup spike) |

### Operationally important alerting signals

- `rate(ducklake_metrics_connect_failures_total{reason="auth"}[5m]) > 0` — operator misconfig (bad creds). Won't recover without intervention.
- `rate(ducklake_metrics_connect_failures_total{reason=~"dial|timeout"}[5m]) > 0` for >10min — RDS network/availability issue.
- `increase(ducklake_metrics_cycle_killed_total[1h]) > 0` — cycle is overrunning its interval; either tenant has too many queries or some query is consistently slow.
- `time() - ducklake_metrics_query_last_success_timestamp > 4 * cycle_interval` — query has been failing for multiple cycles.
- `rate(ducklake_metrics_query_panics_total[1h]) > 0` — daemon bug, file an issue.

Per-query gauge name shape is `<query_name>_<value>` with labels `["tenant"] + query.Labels`. The publisher is the sole writer; it tracks per-(tenant, query) label tuples and computes a diff per Result so dropped tuples are deleted in the same Apply step as new tuples are Set.

## Scheduler shape

One long-lived goroutine per tenant (`runner.Run`). On every `CycleInterval` ticker tick, it launches a fresh cycle goroutine via `go runCycle(...)`. If the previous cycle is still running when the next tick fires, the scheduler `cancelPrev()` calls — the prior cycle's `ctx.Done` fires, its in-flight `pgx.Query` returns `context.Canceled`, deferred metrics bump `cycle_killed_total`, and the goroutine unwinds while the new cycle starts.

Within a cycle, queries run **serially**. Operationally for a SELECT-only workload against one Postgres there's no benefit to parallelism (the pool can serve more concurrency than a single tenant generates), and serial-within-cycle keeps the publisher inbox order deterministic per tenant.

Pool stats (`pgxpool.Stat()`) are emitted as the final action of every cycle, batched with the queries — no separate `pollPoolStats` goroutine to manage.

Initial stagger: on first startup, each tenant waits `fnv1a(tenant) % CycleInterval` before its first fire. This spreads N tenants across the interval window instead of all firing at `t=0`.

`runner.Run` blocks until `ctx` is canceled. Per-cycle goroutines die after sending their last Result (`KindPoolStat`) or being canceled by overrun. There's no explicit join on overrun cancellation — the canceled cycle's `ctx.Done` propagates through pgx and its deferred metrics finalize asynchronously; we don't wait because we trust pgx to release pool connections promptly.

## Liveness probe — what /-/healthy actually checks

`/-/healthy` answers: **is the per-tenant scheduler firing on schedule?** Not "are queries succeeding" — query failures are tracked separately by `query_errors_total` and don't trip the probe. RDS being down isn't the daemon's fault; the daemon's job is to keep firing cycle goroutines and pushing results.

| Snapshot state | HTTP status | Reason label |
|---|---|---|
| Scheduler hasn't fired its first cycle yet | 200 | `starting` |
| Last fire within 2× CycleInterval | 200 | `ok` |
| Last fire older than 2× CycleInterval | 503 | `stale_schedule` |

`Liveness` is one `atomic.Int64` per tenant — the unix-nano timestamp of the most recent scheduler fire. No mutex; lock-free reads. Updated once per ticker tick by the scheduler; read once per probe by the HTTP handler.

`2×` is one full cycle of slack — a single missed tick (GC pause, brief CPU throttle) doesn't trip the probe; two missed ticks in a row does. At the default 5-min interval that's 10 minutes; kubelet's `failureThreshold` × `periodSeconds` should be set to accommodate.

Multi-tenant aggregation: the handler snapshots every tenant's Liveness and returns 503 if ANY tenant is stale. The pod is the unit of kubelet restart, so one stale scheduler takes the whole pod with it. Per-tenant attribution is preserved in `liveness_failures_total{tenant,reason}` for alerting.

`/-/ready` returns 200 once AT LEAST ONE tenant has finished its first catalog connect. Prefers availability over completeness: a slow tenant doesn't gate the others from being scraped. Once a tenant flips to ready it stays ready (asymmetric with `Up{tenant}` which can flip to 0 during reconnect) — readiness gates external Prometheus scrape, and we want scrape to continue for tenants whose pool is cycling so their existing gauges remain visible.

## YAML query format

```yaml
queries:
  - name: ducklake_pending_deletes        # metric prefix; matches ^[a-zA-Z_][a-zA-Z0-9_]*$
    help: |                                # Prometheus HELP
      ...
    labels: [band]                         # optional; column names that become Prometheus labels
    values: [count, bytes]                 # required; column names that become metric values
    sql: |                                 # SQL; must return columns named in labels+values
      SELECT ... FROM public.ducklake_*
```

All queries run on every cycle (`DUCKLAKE_CYCLE_INTERVAL`); there's no per-query interval. Legacy `interval_mins` in operator YAML is silently ignored.

Operator YAML (via `DUCKLAKE_METRICS_CONFIG`) extends the built-ins by name; user wins on collision. `DUCKLAKE_METRICS_DISABLE` is a CSV of names to skip from the final set. Same semantics as the Python daemon.

## Failure-mode handling

| Failure | Where | Visible via | Recovery |
|---|---|---|---|
| Initial connect transient | `connectWithBackoff` loop | `ConnectFailuresTotal{tenant,reason}` rises, `Up{tenant}=0` | Loops forever; pgxpool auto-reconnects on first success |
| Initial connect permanent (bad creds, missing DB) | same | `reason=auth` or `reason=invalid_database` rises continuously | Operator alert; no auto-recovery |
| Query returns error | `runCycle` → publisher | `QueryErrorsTotal{tenant,query}` | Next cycle retries; last-known gauge values persist |
| NULL/non-numeric value in a numeric column | publisher | `QueryErrorsTotal` + Warn log | Operator fixes the bad row OR `COALESCE`s it in their YAML |
| pgxpool conn dies mid-run | pgx auto-reconnect | `QueryErrorsTotal` for the failed query | Automatic |
| Cycle exceeds CycleInterval | next ticker fires `cancelPrev()` | `CycleKilledTotal{tenant}` + `cycle_duration_seconds` near interval | Next cycle starts fresh; tune `DUCKLAKE_CYCLE_INTERVAL` or `DUCKLAKE_QUERY_TIMEOUT` |
| Query panic | cycle defer-recover | `QueryPanicsTotal{tenant,"_cycle"}` + stack log | Cycle exits, next cycle starts on schedule |
| Publisher panic | publisher defer-recover | `QueryPanicsTotal{tenant,"_publisher"}` + stack log | Continues consuming; one bad Result dropped |
| Scheduler goroutine dies (panic past recover) | nothing — silent | `scheduler_fires_total` flatlines; /-/healthy → 503 after 2×interval | Kubelet restarts pod |
| ctx canceled (SIGTERM) | every loop checks | clean shutdown | N/A |

## Tenant configuration

Two ways to feed tenants into the daemon:

| Path | When | Env |
|---|---|---|
| **Single-tenant env** | local dev, smoke tests, the existing per-catalog Deployment shape inherited from millpond | `DUCKLAKE_TENANT`, `DUCKLAKE_RDS_HOST`, `DUCKLAKE_RDS_DATABASE`, `DUCKLAKE_RDS_USERNAME`, `DUCKLAKE_RDS_PASSWORD` |
| **Multi-tenant YAML** | one pod fans out across many tenants | `DUCKLAKE_TENANTS_CONFIG=/path/to/tenants.yaml`; each entry's `passwordEnv` names the env var the daemon reads at startup |

`DUCKLAKE_TENANTS_CONFIG` wins over the env-var path when both are set (the env vars are simply ignored).

Tenants YAML shape (`internal/config/tenants.go`):

```yaml
tenants:
  - id: megaduck-mw-prod-us
    host: megaduck.cluster-xxx.us-east-1.rds.amazonaws.com
    port: 5432                       # optional; default 5432
    database: megaduck
    username: megaduck
    passwordEnv: MEGADUCK_PASSWORD   # daemon reads from os.Getenv at load
  - id: portola-mw-prod-us
    host: portola.cluster-xxx.us-east-1.rds.amazonaws.com
    database: portola
    username: portola
    passwordEnv: PORTOLA_PASSWORD
```

Tenant id is constrained to `^[a-z0-9][a-z0-9_-]*$` (Prometheus-label-safe + K8s-resource-name-compatible). Duplicates and missing password envs fail loud at startup — the daemon won't crash-loop trying to talk to "".

## Scaling further

Today: static config (env or YAML). The pod owns its tenant set for its lifetime.

Stepping stones (see also memory `project_ducklake_metrics_scaling`):

1. **Discovery-API tenants**: instead of a static file, poll the duckgres control plane (the API is at `controlPlane.roleArn` per `charts/argocd/crossplane-config/values/values.managed-warehouse-prod-us.yaml`) to discover tenants. Add/remove tenants live, no restart. Add a tenant manager around the per-tenant goroutine spawn that reconciles the live set against the discovered set.
2. **Sharding**: if n > ~1000, split tenants across N pods by hash; same daemon, ShardSpec env var that filters the discovered tenant set.
3. **Per-query goroutines within a tenant**: if a single tenant's query workload outgrows serial execution (slow queries pushing later ones past their interval), make the Runner's per-query firing async. pgxpool is already sized for it (`MaxConns: 4`).

## Pgx specifics

- `pgxpool.Pool.MaxConns: 4` — plenty for the single-goroutine scheduler; raise if a future change runs per-tenant queries concurrently.
- `application_name=ducklake-metrics-daemon/<tenant>` set on every connection so `pg_stat_activity` shows who's connected during catalog investigations.
- `sslmode=require` — RDS supports TLS unconditionally and the distroless/static base image ships ca-certificates. Switch to `verify-full` only when we mount the RDS CA bundle.
- `connect_timeout=10` so `pgxpool.New`'s initial connection doesn't block forever on a broken endpoint.
- DSN is built with libpq kv-form, every value single-quoted with `'`/`\` escaped (see `catalog.quoteKV`) so passwords containing quotes/backslashes/spaces round-trip safely.
- Per-query row cap (`DUCKLAKE_MAX_QUERY_ROWS`, default 10000) bounds the worst-case result-set materialization in `Catalog.Query`. An operator YAML query without `LIMIT` would otherwise OOM the pod; past the cap the query fails fast and bumps `ducklake_metrics_query_errors_total`.
- Pool stats (`AcquiredConns`, `IdleConns`, `TotalConns`, `EmptyAcquireCount`) are emitted as the final action of each per-tenant cycle, batched with the queries — no separate poller goroutine. `EmptyAcquireCount` is converted to a Prometheus counter via per-tenant delta tracking in the publisher (initial observation seeds the baseline, doesn't emit, so startup pool-warm-up doesn't spike).

## Config invariants enforced at load time

- `DUCKLAKE_QUERY_TIMEOUT < DUCKLAKE_CYCLE_INTERVAL` — a single query taking its full timeout would consume the entire cycle window and the next ticker would kill the cycle every interval. Defaults (60s/300s) leave ample headroom.
- `DUCKLAKE_METRICS_PORT` in valid TCP port range.
- `DUCKLAKE_MAX_QUERY_ROWS >= 0`.
- At least one tenant configured (env-vars OR YAML).

## What's intentionally NOT here

- **DuckDB**. Period. Every query is plain Postgres SQL. If a future query genuinely needs DuckLake's extension machinery (e.g. partition-value translation that lives in the extension and not in the catalog tables), it goes in the maintenance daemon (`millpond/tools/ducklake_maintenance.py`), not here.
- **Catalog mutation**. This daemon issues `SELECT` only. The eventual per-customer read-only role is just defense in depth.
- **Per-query intervals**. Every query in every tenant runs once per `DUCKLAKE_CYCLE_INTERVAL`. The YAML's `interval_mins` field is no longer part of the schema; yaml.v3 silently ignores unknown keys, so operator YAML written against the old Python daemon still loads (the field just doesn't do anything).
- **`/metrics` push-gateway support**. The daemon is pull-scraped via the pod's `prometheus.io/scrape` annotation, same as everything else in `managed-warehouse/`.

## Dockerfile + distroless

The image is multi-stage: golang:1.26-alpine builds a fully-static linux/arm64 binary (CGO disabled, pure-Go pgx so no libpq), then we copy into `gcr.io/distroless/static-debian12:nonroot`. Final image is ~20MB, runs as UID 65532, no shell.

If you need to debug inside the container, swap the base to `gcr.io/distroless/static-debian12:debug-nonroot` temporarily — that ships a busybox shell. Don't ship debug images to prod.

## Pinning + reproducibility

- `go.mod` pins direct dependencies; `go.sum` pins transitive hashes. Both must be committed.
- Dockerfile builder pins `golang:1.26-alpine` (major.minor); patch versions float so we get security updates without manual bumps.
- Runtime base is `gcr.io/distroless/static-debian12:nonroot` (debian12-pinned).
