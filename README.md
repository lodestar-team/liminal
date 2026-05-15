# Liminal

**Polyglot, capability-isolated WASIp3 component runtime for streaming indexing pipelines on The Graph.**

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

A Liminal pipeline is a DAG of WASIp3 components connected by typed channels defined in WIT:

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
├── liminal-wit/            # Shared WIT interface definitions
├── liminal-sdk/            # Rust helpers for component authors
└── examples/
    └── uni-v3-swaps/       # PoC: Uniswap v3 swaps with USD enrichment + multi-sink
        ├── decoder/        # Decode SwapRouter events from EVM blocks
        ├── price-enricher/ # HTTP enrichment → USD prices via DeFiLlama
        ├── sink-postgres/  # Write enriched swaps to Postgres
        └── sink-kafka/     # Publish enriched swaps to Kafka
```

---

## PoC: Uniswap v3 Swaps

The first example pipeline demonstrates what a production Subgraph cannot do: enrich decoded on-chain swap events with live USD prices via an external HTTP API, then fan out to both a Postgres analytics store and a Kafka topic — all from a single source connection with a single cursor.

The equivalent today is three processes, three cursors, and a Debezium dependency. Here it is one pipeline.

---

## Status

Early development. WASIp3 (`wasi:0.3.0`) is at release-candidate quality in Wasmtime 37+; APIs may shift before final stabilisation.

---

## References
- [WASIp3 / wasi.dev roadmap](https://wasi.dev)
- [Wasmtime `wasip3-prototyping`](https://github.com/bytecodealliance/wasmtime)
- [Component Model spec](https://github.com/WebAssembly/component-model)
