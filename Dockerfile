# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder stage: compile the release `topics` server binary.
#
# We build ONLY the server (`-p topics`), not the `topics-probe` test/bench
# CLI in the `probe` workspace member, to keep the image lean.
#
# The pinned `rust:1.96-bookworm` toolchain (matches rust-toolchain.toml) produces a glibc-linked binary that
# runs as-is on `debian:bookworm-slim` (same libc), so no static-musl dance is
# needed. `reqwest` here is rustls-only (no native OpenSSL), so the build needs
# no extra system libraries.
# ---------------------------------------------------------------------------
FROM rust:1.96-bookworm AS builder

WORKDIR /build

# 1) Copy just the manifests + lockfile first so dependency compilation can be
#    cached across builds that only touch source. The workspace declares the
#    `probe` member, so its manifest must be present for `cargo` to resolve the
#    workspace — we add a throwaway stub source for it (and for the server) so a
#    dependency-only build succeeds, then replace the stubs with real source.
COPY Cargo.toml Cargo.lock ./
COPY probe/Cargo.toml probe/Cargo.toml

RUN mkdir -p src probe/src benches \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > probe/src/main.rs \
    && echo 'fn main() {}' > benches/engine.rs

# Warm the dependency cache for the server crate only. `|| true` because the
# stub lib/main won't satisfy everything the manifest references (benches etc.);
# the point is purely to download + compile third-party deps into a cached layer.
RUN cargo build --release -p topics || true

# 2) Copy the real sources. Replacing the stubs invalidates only the crate's own
#    compilation, not the cached dependency layer above.
COPY src ./src
COPY benches ./benches
COPY probe/src ./probe/src

# Ensure cargo recompiles our crate (touch after restoring real sources).
RUN touch src/main.rs src/lib.rs \
    && cargo build --release -p topics \
    && strip target/release/topics

# ---------------------------------------------------------------------------
# Runtime stage: minimal Debian slim, non-root, server binary only.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# `curl` for the HEALTHCHECK; `ca-certificates` for good measure. Cleaned up to
# keep the layer small.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a dedicated non-root user that owns the data volume.
RUN groupadd --system --gid 10001 topics \
    && useradd --system --uid 10001 --gid topics --home-dir /data --no-create-home topics \
    && mkdir -p /data \
    && chown -R topics:topics /data

COPY --from=builder /build/target/release/topics /usr/local/bin/topics

# Defaults. NOTE: the server binds 0.0.0.0 here, and it REFUSES to start on a
# non-loopback bind with no API keys. The operator MUST supply
#   -e TOPICS_API_KEYS=key1,key2
# or, for local/dev ONLY:
#   -e TOPICS_ALLOW_INSECURE_NO_AUTH=1
# See RELEASING.md / README "Running with Docker".
ENV TOPICS_HOST=0.0.0.0 \
    TOPICS_PORT=4000 \
    TOPICS_DATA_DIR=/data

# Durable state (WAL + segments + snapshots) lives here.
VOLUME ["/data"]

EXPOSE 4000

USER topics

# Liveness probe — `/v0/health` is always 200 once the process is serving.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${TOPICS_PORT}/v0/health" || exit 1

ENTRYPOINT ["topics"]
