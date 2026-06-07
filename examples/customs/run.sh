#!/usr/bin/env bash
# Customs live demo: start the local screening provider, then run the live
# pipeline (screener calls it over origin-scoped wasi:http).
#
#   examples/customs/run.sh
#
# Run from the repo root. Builds components + host + server if needed.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "==> building components, host, and screening-server"
just build >/dev/null 2>&1 || cargo build --release -p liminal-host -p customs-screening-server >/dev/null
cargo build --release -p customs-screening-server >/dev/null
cargo build --target wasm32-wasip2 --release -p customs-screener-http >/dev/null
cp target/wasm32-wasip2/release/customs_screener_http.wasm examples/customs/screener-http.wasm

echo "==> starting screening-server on :8088"
./target/release/screening-server &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
# Wait for health.
for _ in $(seq 1 50); do
  if curl -fsS http://localhost:8088/healthz >/dev/null 2>&1; then break; fi
  sleep 0.1
done

echo "==> running the live Customs pipeline"
./target/release/liminal run examples/customs/customs.live.pipeline.toml

echo "==> done (screening-server will be stopped)"
