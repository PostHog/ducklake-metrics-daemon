# syntax=docker/dockerfile:1.7
#
# Multi-stage build:
#   1. golang:alpine builder       — cross-compile static binary
#   2. busybox stager              — populate /busybox with binary + applet symlinks
#   3. distroless/static runtime   — ~20MB, no shell, no package manager,
#                                    nonroot UID 65532; busybox injected for
#                                    `kubectl exec -- sh` debugability
#
# CGO_ENABLED=0 + no use of net's cgo resolver means the resulting binary
# is fully static and runs on distroless/static without a libc. pgx itself
# is pure Go (no libpq), so this is safe.
#
# Busybox is shipped always-on (not gated to a debug variant) because the
# overhead is ~1MB, the daemon runs as nonroot in a network-namespaced pod
# with no setuid binaries, and the operational value of being able to
# `kubectl exec ... -- /busybox/sh` into a wedged pod outweighs the
# trivial attack-surface bump. Inspired by:
# https://amf3.github.io/articles/virtualization/inject_busybox/

FROM golang:1.26-alpine AS builder
WORKDIR /src
# Cache deps in their own layer so source-only changes don't re-download.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/ducklake-metrics-daemon \
      ./cmd/ducklake-metrics-daemon

# Stage a self-contained /busybox dir we can COPY wholesale into distroless.
# busybox is a single multi-call binary; argv[0] selects the applet, so we
# create a RELATIVE symlink per applet pointing at ./busybox. Relative
# symlinks survive the COPY into the final image regardless of the
# eventual mount path.
FROM busybox:musl AS busybox-stager
RUN mkdir -p /rootfs/busybox && \
    cp /bin/busybox /rootfs/busybox/busybox && \
    cd /rootfs/busybox && \
    for app in $(./busybox --list); do \
      [ "$app" = "busybox" ] || ln -s busybox "$app"; \
    done

# distroless/static includes ca-certificates for TLS to RDS and a passwd
# entry for the nonroot UID. The :nonroot tag pins UID 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/ducklake-metrics-daemon /usr/local/bin/ducklake-metrics-daemon
# Inject busybox + applets. `kubectl exec ... -- /busybox/sh` gives a real
# shell with the standard utility set (ls, cat, ps, netstat, nslookup, etc.)
# for live debugging.
COPY --from=busybox-stager /rootfs/busybox /busybox
ENV PATH="/busybox:${PATH}"
USER nonroot:nonroot
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/ducklake-metrics-daemon"]
