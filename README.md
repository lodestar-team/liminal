# Liminal

**Polyglot, capability-isolated WASIp2 component runtime for streaming indexing pipelines on The Graph.**

Liminal is a third lane alongside Subgraphs and Substreams — not a replacement for either.

- **Use Subgraphs** when you need a GraphQL endpoint served by the decentralised network with Proof-of-Indexing.
- **Use Substreams** when you need high-throughput parallel historical backfill of pure-compute transformations.
- **Use Liminal** when your pipeline needs effectful middleware, capability-isolated plugins, polyglot stages, mid-block emission, hot-swap without source resync, or multiple sinks off a single cursor.

---

## Why Liminal

The pipelines Liminal targets exist today — they're just built by gluing four systems together:

- a Substreams or Subgraph for chain-side decoding
- a custom sink for the effectful step
- a sidecar service for whatever the sink can't do
- a reconciliation layer because cursors now live in three places

Liminal replaces that stack with a single WASIp3 component pipeline: one cursor, one process, one observable failure boundary. Capabilities (HTTP, key-value, filesystem) are granted per-component by the host, not per-pipeline — so untrusted middleware is a concrete engineering construct, not a code-review prayer.

Full rationale: [research.md](./research.md)

---

## Architecture

A Liminal pipeline is a DAG of WASIp2 components connected by typed channels defined in WIT:

```
source connector
    └─► decoder component
            └─► enricher component   (wasi:http granted here only)
                    ├─► sink-a component
                    └─► sink-b component
```

Each component is a Wasm binary with a WIT interface. The host (Wasmtime) loads, composes, and wires them. Capabilities are injected at composition time.

---

## Workspace

```
liminal/
├── liminal-host/           # Wasmtime pipeline runner (binary)
├── wit/                    # Shared WIT interface definitions
├── liminal-sdk/            # Rust helpers for component authors
└── examples/
    ├── uni-v3-swaps/       # Pipeline 1: Uniswap v3 swaps → USD enrichment → Postgres + Kafka
    │   ├── decoder/
    │   ├── price-enricher/
    │   ├── sink-postgres/
    │   └── sink-kafka/
    └── cross-dex-arb/      # Pipeline 2: Uni v3 + Balancer v2 → live arb dashboard
        ├── decoder/        # Decode both Swap event ABIs; normalise to common type
        ├── enricher/       # USD price + decimals via DeFiLlama
        ├── sink-json/      # JSON lines → stdout (captured by host → SSE broadcast)
        └── dashboard/      # Vanilla-JS SSE client; spread table + live feed
```

---

## Example 1: Uniswap v3 Swaps

Demonstrates effectful multi-sink fan-out that a Subgraph cannot do: decode Uniswap v3 Swap
events, enrich with live USD prices via DeFiLlama (HTTP granted only to the enricher component),
then fan out to Postgres and Kafka concurrently — single source connection, single cursor.

```bash
# Build
cargo build --target wasm32-wasip2 --release \
  -p uni-v3-decoder -p uni-v3-price-enricher \
  -p uni-v3-sink-postgres -p uni-v3-sink-kafka

cp target/wasm32-wasip2/release/uni_v3_decoder.wasm        examples/uni-v3-swaps/decoder.wasm
cp target/wasm32-wasip2/release/uni_v3_price_enricher.wasm examples/uni-v3-swaps/price-enricher.wasm
cp target/wasm32-wasip2/release/uni_v3_sink_postgres.wasm  examples/uni-v3-swaps/sink-postgres.wasm
cp target/wasm32-wasip2/release/uni_v3_sink_kafka.wasm     examples/uni-v3-swaps/sink-kafka.wasm

# Run (--database-url and --kafka-brokers are optional; sinks warn and skip if absent)
cargo run --release --bin liminal -- uni-v3 \
  --rpc wss://your-node \
  --limit 3
```

---

## Example 2: Cross-DEX Arbitrage Tracker

Demonstrates multi-protocol decoding and real-time SSE push from a single pipeline.
Subscribes to both Uniswap v3 and Balancer v2 Swap events simultaneously, decodes and
normalises them to a common type, enriches with USD prices, then broadcasts every swap as
a JSON line via Server-Sent Events to a live dashboard.

The dashboard shows a price-spread table (Uni v3 price vs Balancer v2 price per token pair,
ranked by spread %) alongside a live swap feed with protocol, pair, USD size, and block.

```bash
# Build
cargo build --target wasm32-wasip2 --release \
  -p arb-decoder -p arb-enricher -p arb-sink-json

cp target/wasm32-wasip2/release/arb_decoder.wasm   examples/cross-dex-arb/decoder.wasm
cp target/wasm32-wasip2/release/arb_enricher.wasm  examples/cross-dex-arb/enricher.wasm
cp target/wasm32-wasip2/release/arb_sink_json.wasm examples/cross-dex-arb/sink-json.wasm

# Run — dashboard at http://localhost:8080
cargo run --release --bin liminal -- arb \
  --rpc wss://your-node

# Custom port
cargo run --release --bin liminal -- arb \
  --rpc wss://your-node \
  --port 9090
```

Open `http://localhost:8080` in a browser. The left panel updates in real time as swap events
arrive; the right panel is a live feed with protocol tag, token pair, USD value, and block number.

---

## Status

Two working PoC pipelines, both running against live Ethereum mainnet.

**uni-v3-swaps** — decodes Uniswap v3 Swap events, enriches with USD prices via DeFiLlama,
fans out to Postgres and Kafka sinks from a single source cursor.

**cross-dex-arb** — decodes Uniswap v3 and Balancer v2 Swap events, normalises to a common
WIT type, enriches with USD prices and token decimals, streams to a live SSE dashboard showing
cross-DEX price spreads per token pair.

Built on Wasmtime 44 (WASIp2 / WASI 0.2.x). WASIp3 (async streams, structured concurrency)
is tracked upstream; Liminal will migrate once it stabilises in Wasmtime.

---

## References
- [WASI 0.2 / wasi.dev](https://wasi.dev)
- [Wasmtime](https://github.com/bytecodealliance/wasmtime)
- [Component Model spec](https://github.com/WebAssembly/component-model)
