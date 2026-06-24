# ducklake-metrics-daemon

Long-lived Go daemon that runs catalog-side SQL queries against a DuckLake's Postgres metadata store and exposes the results as Prometheus gauges on `:9100`.

Go rewrite of [millpond's `tools/ducklake_metrics.py`](https://github.com/PostHog/millpond/blob/main/tools/ducklake_metrics.py), with one architectural change: no DuckDB. The metric queries are plain SQL aggregates against `public.ducklake_*` tables in Postgres, so the daemon talks to Postgres directly via `pgx` — drops ~50-100Mi per-tenant of DuckDB ATTACH overhead and replaces the GIL-constrained Python daemon's `ThreadPoolExecutor` with cheap goroutines.

The query library + Prometheus metric names + env-var contract are kept identical to the Python implementation, so the existing [`managed-warehouse/ducklake-state.json`](https://github.com/PostHog/grafana-dashboards/blob/master/managed-warehouse/ducklake-state.json) Grafana dashboard works unchanged against either daemon.

## Quick start

```bash
# Required env
export DUCKLAKE_TENANT=tenant-a-mw-prod-us
export DUCKLAKE_RDS_HOST=...
export DUCKLAKE_RDS_DATABASE=tenant-a
export DUCKLAKE_RDS_USERNAME=tenant-a
export DUCKLAKE_RDS_PASSWORD=...

# Optional
export DUCKLAKE_RDS_PORT=5432               # default 5432
export DUCKLAKE_METRICS_PORT=9100           # default 9100
export DUCKLAKE_METRICS_LIVENESS_TIMEOUT=300 # default 300

just run
# in another shell:
curl localhost:9100/metrics | head
```

`just list-queries` prints the resolved set without connecting — fast YAML validation.

## Endpoints

| Path | What |
|---|---|
| `/metrics` | Prometheus exposition |
| `/-/healthy` | K8s liveness probe; 503 when scheduler is wedged (`in_flight` or `stale_tick`) |
| `/-/ready` | K8s readiness probe; 200 once first catalog connect succeeds |

## See also

- [`AGENT.md`](./AGENT.md) — agent guidance, design rationale, gotchas
- [`internal/queries/builtin.yaml`](./internal/queries/builtin.yaml) — the embedded library of queries the daemon ships with
- millpond's [`tools/ducklake_metrics.py`](https://github.com/PostHog/millpond/blob/main/tools/ducklake_metrics.py) — Python implementation this rewrites
