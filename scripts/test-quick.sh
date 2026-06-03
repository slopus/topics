#!/usr/bin/env bash
set -euo pipefail

# Basic, sub-minute confidence gate for routine work.
# Intentionally skips the broad unit/integration corpus, fault injection,
# failpoints, crash matrices, proptest/fuzz, benchmark sweeps, docs builds,
# and deeper queue/router/SSE/WebSocket matrices.

cargo fmt --check

# Compile the library and server binary without compiling every test target.
cargo check --lib --bin topics

# One in-process HTTP smoke covers create, append, get, diff, delete, router
# fan-out, watch, and node loop-prevention without binding sockets.
cargo test --test smoke
