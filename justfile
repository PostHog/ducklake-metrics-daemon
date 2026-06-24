# ducklake-metrics-daemon — Go rewrite of millpond/tools/ducklake_metrics.py.
# Mirrors millpond's justfile structure (dev / test / build / docker groups).
#
# Required env vars for `run`:
#   DUCKLAKE_TENANT, DUCKLAKE_RDS_HOST, DUCKLAKE_RDS_DATABASE,
#   DUCKLAKE_RDS_USERNAME, DUCKLAKE_RDS_PASSWORD
# Optional:
#   DUCKLAKE_RDS_PORT (default 5432), DUCKLAKE_METRICS_PORT (default 9100),
#   DUCKLAKE_METRICS_CONFIG (user YAML extending the built-ins),
#   DUCKLAKE_METRICS_DISABLE (CSV of built-in names to skip),
#   DUCKLAKE_METRICS_LIVENESS_TIMEOUT (seconds; default 300),
#   DUCKLAKE_QUERY_TIMEOUT (seconds; default 60).

default:
    @just --list

# ─── dev ────────────────────────────────────────────────────────────────────

[group('dev')]
run:
    go run ./cmd/ducklake-metrics-daemon

[group('dev')]
list-queries:
    go run ./cmd/ducklake-metrics-daemon -list-queries

[group('dev')]
fmt:
    gofmt -w .

[group('dev')]
fmt-check:
    @diff -u <(echo -n) <(gofmt -d .)

[group('dev')]
vet:
    go vet ./...

# golangci-lint is the de-facto lint umbrella for Go projects. Install via
# `brew install golangci-lint` if you don't have it locally; CI installs
# the same version.
[group('dev')]
lint:
    golangci-lint run

[group('dev')]
lint-fix:
    golangci-lint run --fix

# ─── test ───────────────────────────────────────────────────────────────────

[group('test')]
test:
    go test -race ./...

[group('test')]
test-verbose:
    go test -race -v ./...

[group('test')]
test-cover:
    go test -race -cover ./...

# CI gate. Run before pushing.
[group('test')]
ci: fmt-check vet lint test

# ─── build ──────────────────────────────────────────────────────────────────

# Local binary; stamps `version` from `git describe`. Output binary lands
# in ./bin to keep the repo root clean.
[group('build')]
build:
    @mkdir -p bin
    go build -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/ducklake-metrics-daemon ./cmd/ducklake-metrics-daemon

[group('build')]
clean:
    rm -rf bin

# Static cross-compile for the Linux/arm64 production target. Used by the
# Dockerfile builder stage; also useful for ad-hoc deploy testing.
[group('build')]
build-linux-arm64:
    @mkdir -p bin
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
        -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
        -o bin/ducklake-metrics-daemon-linux-arm64 \
        ./cmd/ducklake-metrics-daemon

# ─── docker ─────────────────────────────────────────────────────────────────

[group('docker')]
docker-build tag="ducklake-metrics-daemon:dev":
    docker build -t {{ tag }} .

# Run the built image against a local DuckLake catalog. Caller supplies
# the DUCKLAKE_* env via shell — keeps secrets out of the recipe text.
[group('docker')]
docker-run tag="ducklake-metrics-daemon:dev":
    docker run --rm -p 9100:9100 \
        -e DUCKLAKE_TENANT \
        -e DUCKLAKE_RDS_HOST -e DUCKLAKE_RDS_PORT \
        -e DUCKLAKE_RDS_DATABASE -e DUCKLAKE_RDS_USERNAME -e DUCKLAKE_RDS_PASSWORD \
        -e DUCKLAKE_METRICS_PORT -e DUCKLAKE_METRICS_CONFIG -e DUCKLAKE_METRICS_DISABLE \
        -e DUCKLAKE_METRICS_LIVENESS_TIMEOUT -e DUCKLAKE_QUERY_TIMEOUT \
        -e DUCKLAKE_TENANT_RESTART_COOLDOWN -e DUCKLAKE_MAX_QUERY_ROWS \
        -e DUCKLAKE_TENANTS_CONFIG \
        {{ tag }}

# Pop a busybox shell inside the built image — same image as production,
# but with the entrypoint swapped for /busybox/sh. Useful for sanity-
# checking the image layout (PATH, file ownership, what's actually in
# /usr/local/bin) without spinning up a pod.
[group('docker')]
docker-shell tag="ducklake-metrics-daemon:dev":
    docker run --rm -it --entrypoint /busybox/sh {{ tag }}

# ─── dependencies ──────────────────────────────────────────────────────────

[group('dev')]
deps-update:
    go get -u ./...
    go mod tidy

[group('dev')]
deps-tidy:
    go mod tidy
