# ducklake-metrics-daemon

[![Test](https://github.com/PostHog/ducklake-metrics-daemon/actions/workflows/test.yml/badge.svg)](https://github.com/PostHog/ducklake-metrics-daemon/actions/workflows/test.yml)
[![Container Image CD](https://github.com/PostHog/ducklake-metrics-daemon/actions/workflows/cd-image.yml/badge.svg)](https://github.com/PostHog/ducklake-metrics-daemon/actions/workflows/cd-image.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Long-lived Go daemon that runs catalog-side SQL queries against a DuckLake's Postgres metadata store and exposes the results as Prometheus gauges on `:9100`. Multi-tenant: one connection per tenant, one scheduler goroutine per tenant firing fresh cycle goroutines every `DUCKLAKE_CYCLE_INTERVAL`, one publisher actor consuming results and writing gauges.

Go rewrite of [millpond's `tools/ducklake_metrics.py`](https://github.com/PostHog/millpond/blob/main/tools/ducklake_metrics.py) — drops DuckDB entirely (every metric query is plain SQL against `public.ducklake_*` tables in Postgres; the daemon talks to Postgres directly via `pgx`).

## Quick start (single-tenant, local dev)

```bash
export DUCKLAKE_TENANT=tenant-a
export DUCKLAKE_RDS_HOST=...
export DUCKLAKE_RDS_DATABASE=tenant-a
export DUCKLAKE_RDS_USERNAME=tenant-a
export DUCKLAKE_RDS_PASSWORD=...

just run
# in another shell:
curl localhost:9100/metrics | head
```

`just list-queries` prints the resolved query set without connecting (fast YAML validation).

## Multi-tenant (production shape)

Point at a YAML inventory:

```bash
export DUCKLAKE_TENANTS_CONFIG=/etc/ducklake/tenants.yaml
# Each tenant's `passwordEnv:` names the env var the daemon reads its password from.
export TENANT_A_PASSWORD=...
export TENANT_B_PASSWORD=...

just run
```

```yaml
# /etc/ducklake/tenants.yaml
tenants:
  - id: tenant-a
    host: tenant-a.cluster-xxx.us-east-1.rds.amazonaws.com
    database: tenant-a
    username: tenant-a
    passwordEnv: TENANT_A_PASSWORD
  - id: tenant-b
    host: tenant-b.cluster-xxx.us-east-1.rds.amazonaws.com
    database: tenant-b
    username: tenant-b
    passwordEnv: TENANT_B_PASSWORD
```

`DUCKLAKE_TENANTS_CONFIG` takes precedence over `DUCKLAKE_TENANT` + `DUCKLAKE_RDS_*`.

## Endpoints

| Path | What |
|---|---|
| `/metrics` | Prometheus exposition |
| `/-/healthy` | K8s liveness probe; 503 once a tenant's scheduler hasn't fired in `2 × DUCKLAKE_CYCLE_INTERVAL` |
| `/-/ready` | K8s readiness probe; 200 once AT LEAST ONE tenant has connected |

## Configuration

| Env var | Default | What |
|---|---|---|
| `DUCKLAKE_TENANTS_CONFIG` | (unset) | Path to multi-tenant YAML inventory. Wins over the single-tenant env path |
| `DUCKLAKE_TENANT` + `DUCKLAKE_RDS_*` | required if no YAML | Single-tenant fallback shape |
| `DUCKLAKE_METRICS_PORT` | `9100` | HTTP bind port |
| `DUCKLAKE_CYCLE_INTERVAL` | `300` | Seconds between scheduler fires per tenant |
| `DUCKLAKE_QUERY_TIMEOUT` | `60` | Per-query timeout. Must be `<` cycle interval |
| `DUCKLAKE_MAX_QUERY_ROWS` | `10000` | Per-query result-set cap (0 disables) |
| `DUCKLAKE_METRICS_CONFIG` | (unset) | Operator YAML extending the built-in queries |
| `DUCKLAKE_METRICS_DISABLE` | (unset) | CSV of built-in query names to skip |
| `DUCKLAKE_TENANT_RESTART_COOLDOWN` | `300` | (Legacy; unused in the actor architecture — kept for env-var back-compat) |

## Deployment

Production deployment is via the [`charts/ducklake-metrics-daemon`](https://github.com/PostHog/charts/tree/main/charts/ducklake-metrics-daemon) Helm chart in `PostHog/charts`. Image is published to `ghcr.io/posthog/ducklake-metrics-daemon` on every push to `main`.

## Pre-push checklist

```bash
just ci   # fmt-check + vet + lint + race tests
```

## See also

- [`AGENT.md`](./AGENT.md) — design rationale, architecture deep-dive, gotchas
- [`internal/queries/builtin.yaml`](./internal/queries/builtin.yaml) — embedded query library
- millpond's [`tools/ducklake_metrics.py`](https://github.com/PostHog/millpond/blob/main/tools/ducklake_metrics.py) — Python implementation this replaces (no longer deployed; preserved for reference)

## License

[MIT](./LICENSE)
