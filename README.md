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
    └── uni-v3-swaps/       # PoC: Uniswap v3 swaps with USD enrichment + multi-sink
        ├── decoder/        # Decode Swap events from EVM logs
        ├── price-enricher/ # HTTP enrichment → USD prices via DeFiLlama
        ├── sink-postgres/  # Write enriched swaps to Postgres
        └── sink-kafka/     # Publish enriched swaps to Kafka
```

---

## PoC: Uniswap v3 Swaps

The first example pipeline demonstrates what a production Subgraph cannot do: enrich decoded on-chain swap events with live USD prices via an external HTTP API, then fan out to both a Postgres analytics store and a Kafka topic — all from a single source connection with a single cursor.

The equivalent today is three processes, three cursors, and a Debezium dependency. Here it is one pipeline.

---

## Running the PoC

Requires a WebSocket Ethereum RPC (Alchemy, Infura, Chainstack, etc.).

```bash
# Build the four WASIp2 components
cargo build --target wasm32-wasip2 --release \
  -p uni-v3-decoder -p uni-v3-price-enricher \
  -p uni-v3-sink-postgres -p uni-v3-sink-kafka

# Copy to default paths
cp target/wasm32-wasip2/release/uni_v3_decoder.wasm          examples/uni-v3-swaps/decoder.wasm
cp target/wasm32-wasip2/release/uni_v3_price_enricher.wasm   examples/uni-v3-swaps/price-enricher.wasm
cp target/wasm32-wasip2/release/uni_v3_sink_postgres.wasm    examples/uni-v3-swaps/sink-postgres.wasm
cp target/wasm32-wasip2/release/uni_v3_sink_kafka.wasm       examples/uni-v3-swaps/sink-kafka.wasm

# Run — processes 3 blocks then exits
RUST_LOG=liminal=debug cargo run --bin liminal -- \
  --rpc wss://your-node \
  --limit 3 \
  --database-url postgres://... \   # optional
  --kafka-brokers localhost:9092     # optional
```

Without `--database-url` / `--kafka-brokers` the sinks warn and skip; decoder and enricher still run.

---

## Status

Working PoC. The `uni-v3-swaps` example pipeline runs against live Ethereum mainnet:
decoded Uniswap v3 Swap events are enriched with USD prices via DeFiLlama and fanned out
to Postgres and Kafka sinks.

Built on Wasmtime 44 (WASIp2 / WASI 0.2.x). WASIp3 (async streams, structured concurrency)
is tracked upstream; Liminal will migrate once it stabilises in Wasmtime.

---

## References
- [WASI 0.2 / wasi.dev](https://wasi.dev)
- [Wasmtime](https://github.com/bytecodealliance/wasmtime)
- [Component Model spec](https://github.com/WebAssembly/component-model)
