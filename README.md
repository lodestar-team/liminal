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

Liminal replaces that stack with a single WASIp2 component pipeline: one cursor, one process, one observable failure boundary. Capabilities (HTTP, key-value, filesystem) are granted per-component by the host, not per-pipeline — so untrusted middleware is a concrete engineering construct, not a code-review prayer.

Full rationale: [research.md](./research.md)

---

## How it works

A Liminal pipeline is a **DAG declared in a manifest**. The host is generic: it reads the
manifest, instantiates each component into its own capability-scoped sandbox, wires the graph,
and streams source messages through. Adding a new pipeline shape is a config change — the host
is never recompiled.

```
source ──▶ decoder ──▶ enricher ──▶ sink ──▶ (dashboard / db / queue)
   (bytes)     (bytes)      (bytes)
```

Every component — whatever it does — exports **one** function:

```wit
// wit/world.wit
interface node {
    transform: func(input: list<u8>) -> result<list<list<u8>>, string>;
}
```

The host pipes opaque bytes (JSON by convention) along the edges, blind to the payload. That
blindness is the point: the runtime moves bytes; the components give them meaning.

### The manifest

```toml
name = "cross-dex-arb"

[source]
type = "evm"
rpc = "${ETH_RPC_URL}"          # ${VAR} and ${VAR:-default} are interpolated
topics = ["0xc42079…", "0x2170c7…"]

[[nodes]]
id = "decoder"
wasm = "examples/cross-dex-arb/decoder.wasm"
# no capabilities → pure compute, cannot touch the network even if it tried

[[nodes]]
id = "enricher"
wasm = "examples/cross-dex-arb/enricher.wasm"
capabilities = ["http"]          # the ONLY node granted network egress
env = { ORACLE_URL = "https://coins.llama.fi" }

[[nodes]]
id = "sink"
wasm = "examples/cross-dex-arb/sink-json.wasm"
capabilities = ["stdout"]

[[edges]]
from = "source"
to = "decoder"
[[edges]]
from = "decoder"
to = "enricher"
[[edges]]
from = "enricher"
to = "sink"

[dashboard]                      # optional: terminal-node output → live SSE
port = 8080
html = "examples/cross-dex-arb/dashboard/index.html"
```

### Capability isolation, enforced by construction

Capabilities are **data**, granted per node. A node that imports a capability it wasn't granted
**fails to instantiate** — loudly, at load time. This isn't a convention; it's a test:

```
test runtime::tests::http_capability_is_enforced_at_load ... ok
```

The enricher imports `wasi:http`. It loads with `capabilities = ["http"]` and is refused
without it. The decoder and sinks have no network grant and physically cannot make an outbound
request.

### Authoring a component

A component is a typed closure. The `liminal-sdk` `node!` macro handles the WIT boundary and
JSON (de)serialisation — there is no bindgen boilerplate to write:

```rust
use liminal_sdk::{node, EvmLog};
use arb_types::NormalizedSwap;

node!(|log: EvmLog| -> Result<Vec<NormalizedSwap>, String> {
    // Ok(vec![])      → filter this message
    // Ok(vec![swap])  → emit downstream
    // Err(msg)        → recoverable per-message error (host logs, carries on)
    Ok(decode(log).into_iter().collect())
});
```

---

## Workspace

```
liminal/
├── liminal-host/           # Generic manifest-driven runtime (the `liminal` binary)
│   ├── manifest.rs         #   TOML manifest: parse, ${env} interpolation, DAG validation
│   ├── runtime.rs          #   load nodes (capability-scoped), wire DAG, stream messages
│   ├── source.rs           #   EVM WebSocket log source → canonical EvmLog
│   └── dashboard.rs        #   optional SSE dashboard fed by terminal-node output
├── liminal-sdk/            # `node!` macro + shared EvmLog wire type for component authors
├── wit/world.wit           # The one universal node interface
├── justfile                # build / test / run recipes
└── examples/
    ├── uni-v3-swaps/        # decoder → enricher → Postgres + Kafka fan-out
    │   ├── types/           #   shared serde wire types for this pipeline
    │   ├── {decoder,price-enricher,sink-postgres,sink-kafka}/
    │   └── pipeline.toml
    └── cross-dex-arb/       # Uni v3 + Balancer v2 → live arb dashboard
        ├── types/
        ├── {decoder,enricher,sink-json}/
        ├── dashboard/index.html
        └── pipeline.toml
```

---

## Building

```bash
# host (native) — note `cargo build` alone builds only natively-linkable crates
cargo build --release -p liminal-host

# components (wasm) + stage artifacts — or just `just build` for everything
just build
```

`just build` builds the host and all seven components for `wasm32-wasip2` and copies the `.wasm`
files next to their manifests. (Plain `cargo build` deliberately skips the component crates —
they're cdylibs with WIT exports that only link for the wasm target.)

---

## Running

Both pipelines are now just manifests handed to the same generic binary:

```bash
export ETH_RPC_URL=wss://your-node

# Cross-DEX arbitrage tracker → dashboard at http://localhost:8080
just run-arb
#   …or: cargo run --release -p liminal-host -- examples/cross-dex-arb/pipeline.toml

# Uniswap v3 swaps → Postgres + Kafka fan-out (stop after 5 messages)
just run-uni --limit 5
#   …or: cargo run --release -p liminal-host -- examples/uni-v3-swaps/pipeline.toml --limit 5
```

`DATABASE_URL` and `KAFKA_BROKERS` are optional — the uni-v3 manifest supplies `${VAR:-default}`
fallbacks, and the PoC sinks emit SQL / JSON to stdout rather than connecting to live services.

---

## Status

A **generic, manifest-driven runtime** with two pipelines expressed entirely as config, both
running against live Ethereum mainnet.

- **One interface** (`transform: bytes -> [bytes]`) for every component.
- **Pipelines are data** — `pipeline.toml`, not host code.
- **Capabilities are data** — granted per node, enforced at load time (with a test to prove it).
- **`node!` SDK macro** — components are typed closures, zero bindgen boilerplate.
- `cargo build` is clean; `cargo test` covers manifest validation and capability enforcement.

Built on Wasmtime 44 (WASIp2 / WASI 0.2.x). WASIp3 (async streams, structured concurrency) is
tracked upstream; Liminal will migrate once it stabilises in Wasmtime.

### Next

- Durable cursor / checkpointing for resume-after-restart and hot-swap without resync.
- Concurrent fan-out across sibling branches (today the DAG is walked breadth-first per message).
- Finer-grained capabilities (key-value, scoped filesystem, per-origin HTTP allow-lists).

---

## References
- [WASI 0.2 / wasi.dev](https://wasi.dev)
- [Wasmtime](https://github.com/bytecodealliance/wasmtime)
- [Component Model spec](https://github.com/WebAssembly/component-model)
