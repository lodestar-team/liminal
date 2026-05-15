# Why Conduit, Given We Already Have Subgraphs and Substreams

*A follow-up to GRC-007. Posting this because the question came up on the thread within the first day, in slightly different forms, from at least four people I respect. The framing of the question is correct: the burden of proof is on Conduit, not on the existing stack. This post tries to meet that burden honestly — including by being explicit about the cases where Conduit is the wrong answer.*

---

## TL;DR

1. **Subgraphs and Substreams are not going anywhere.** They are the right tool for the overwhelming majority of indexing workloads on The Graph today, and the 2026 roadmap doubles down on both. Conduit is not a replacement for either.
2. **There is, however, a real and growing class of indexing pipelines that neither tool serves well** — pipelines that need streaming semantics, capability-isolated middleware, polyglot composition, hot-swap, or effectful enrichment in the indexing path. Today these get built as bolted-together sidecar services with custom IPC. That tax is paid by every team that writes one.
3. **Conduit is a polyglot, capability-isolated WASIp3 component runtime for streaming indexing pipelines.** It is *worse* than Subgraphs at GraphQL-store mappings and *worse* than Substreams at parallel historical backfill. It is *better* than either at pipelines whose shape doesn't fit "AssemblyScript writes entities" or "pure-compute Rust module emits protobuf."

If you are deploying a standard event-handler subgraph, use Subgraphs. If you are doing high-throughput parallel reindex of pure-compute transformations, use Substreams. If you are building something that requires effectful middleware, untrusted plugin composition, or cross-language streaming with mid-block emission, that's the Conduit lane. Decision tree at the bottom.

---

## 1. Where Subgraphs and Substreams are still the right answer

Let me start by being explicit about what Conduit *cannot* and *should not* try to replace, because if I don't, I'll deserve the pushback.

### Subgraphs

Subgraphs are the most battle-tested indexing primitive in web3. The feature set you get for free by deploying one is enormous and would take Conduit years to match:

- A reference indexer (`graph-node`) that has been hardened in production since 2018, with a deterministic execution model that the network's economic security depends on. Indexing rewards on Horizon are gated by Proof-of-Indexing, and PoI requires bit-exact reproducibility — a property that the AssemblyScript+WASM mapping runtime delivers and that any new runtime has to earn by demonstration, not assertion. The graph-node v0.29 release notes explicitly call out determinism fixes and the fact that even narrow deviations require rewinding affected subgraphs from their start blocks; that is the bar.
- A managed GraphQL endpoint with derived fields, full-text search, time-travel queries, declarative `eth_calls`, file data sources, and a Studio UX that lets a developer go from contract address to deployed indexer in minutes.
- A **decentralized network of indexers** that already serves Subgraphs at scale — Messari's *State of The Graph Q4 2025* reports active Subgraphs at a new all-time high of 15.5K (up 3.0% QoQ), with Base alone generating 1.23B queries in the quarter (≈410M/month, up 11.0% QoQ), and total network volume in the ~1.8B-queries-per-month range.
- Economic alignment via curation, GraphTally micropayments, and the Rewards Eligibility Oracle.
- A growing path to Rust mappings via GRC-003 / GRC-004, which preserves the AssemblyScript ABI and graph-node compatibility — *exactly* the conservative, non-disruptive Rust onramp the ecosystem asked for. Graphite's own description states it bluntly: *"The compiled WASM is AssemblyScript-ABI-compatible — unmodified graph-node accepts it as a standard subgraph. Zero graph-node changes required."*

If your problem is "I have a contract, I want to map events to entities, I want a GraphQL endpoint, I want it served by the decentralized network," **Conduit is the wrong choice and will be for the foreseeable future.**

### Substreams

Substreams' tier1/tier2 architecture is genuinely the fastest way to do parallel historical backfill of pure-compute transformations on chain data. The numbers are real because the model is right for that workload. From The Graph's *Case Study: How The Graph's Substreams and Token API Made Multi-Chain Indexing 24x Faster*: *"These improvements represent a 2300% speed increase for Arbitrum and over 600% for Ethereum. Arbitrum One: Complete history processed in 15 hours instead of 15 days."* StreamingFast's own earlier Sparkle write-up reports *"sync time on that subgraph from weeks to around six hours."*

What makes those numbers possible:

- Block ranges are sharded into 1k-block segments and processed in parallel by tier2 workers; results are cached by module hash, so subsequent consumers of the same module pay zero recompute cost.
- The execution model is deterministic (pure Rust → pure Wasm; no I/O), which is what *makes* the parallel backfill safe.
- The sink ecosystem (`substreams-sink-sql`, `substreams-sink-files`, `substreams-sink-mq`, `substreams-sink-graph-load`) is mature and covers the most common output shapes.
- The 2026 roadmap puts a Substreams Data Service on Horizon. From the *GraphOps Update March 2026*: *"putting the MVP development at roughly 75% complete, and demonstrated Substreams data flowing between a Consumer and Indexer via the SDS gRPC protocol with trust-minimized incremental payments."*

If your problem is "I want to backfill all USDC transfers on eight chains in under an hour into a Postgres warehouse," **Conduit is the wrong choice.** Substreams will smoke it on raw throughput because Substreams is allowed to assume your transformation is pure, and Conduit cannot make that assumption (that's the whole point of Conduit).

This is the honest framing: Conduit trades determinism-driven parallelism for capability-scoped effectfulness. You don't get both.

---

## 2. The argument structure

Each tool's core competency as a single sentence:

- **Subgraphs:** "deploy a synchronous mapping that writes entities to a GraphQL store." Bad at: streaming, out-of-process state between handlers, multi-stage with intermediate consumers, cross-language, `await` semantics inside the mapping.
- **Substreams:** "high-throughput parallel backfill of pure-compute transformations over chain data, then emit protobuf to a sink." Bad at: cancellation observable in user code, capability-scoped untrusted middleware, cross-language within a single stage, hot-swappable without resyncing the source-side cursor, mid-block partial emission.
- **Conduit:** "polyglot, capability-isolated streaming pipelines with effectful middleware composed at runtime." Worse than both above at their core use cases.

---

## 3. Pipelines neither Subgraphs nor Substreams serve well today

### 3.1 Enrichment with off-chain HTTP APIs (NFT metadata, oracle prices, IPFS with gateway fallback)

**The problem.** `ipfs.cat` returns null when the pin isn't ready; the subgraph has no clean way to retry. `graph-node` issue #4198 is still open. File Data Sources are limited to IPFS/Arweave and explicitly cannot update chain-based entities from the file handler. Substreams can't make HTTP calls at all — pure-compute by design. The HTTP enrichment moves to a sidecar, paying IPC and serialization overhead per event.

**Why Conduit solves it.** WASIp3's Component Model lets the enrichment middleware be a separate component that imports `wasi:http`, while the core mapping imports neither. The host grants HTTP only to the enrichment component, and only to specific allow-listed origins.

```wit
interface enrich {
  use wasi:http/types.{request, response};
  enrich-token: func(token-id: u64, uri: string) -> result<metadata, enrich-error>;
}

world nft-pipeline {
  import wasi:http/outgoing-handler;   // granted ONLY to the enrichment component
  import enrich;                        // composed in by the host
  export indexer;                       // the core mapping; no HTTP capability
}
```

### 3.2 Marketplace of untrusted middleware

**The problem.** No safe way to compose a third-party transform into your pipeline without trusting its code wholesale. Subgraph AS mappings: no per-module isolation. Substreams: better (pure-compute limits trust surface) but can't isolate effectful third-party modules.

**Why Conduit solves it.** Capability-based security is the original Component Model design goal. A middleware component declares its WIT imports; the host grants only what it needs. Composition is type-checked. This makes "untrusted Wasm component" a concrete engineering construct.

### 3.3 Local cache (LRU/RocksDB) the mapping itself shouldn't see

**Subgraph workaround:** use the entity store as cache — every cache write is a journaled entity update paying PoI overhead. **Substreams workaround:** use a `store` module — constrained merge semantics, not RocksDB, not LRU.

**Why Conduit solves it.** A `wasi:keyvalue` interface granted to a dedicated dedup component that the mapping imports as a black-box `seen?: func(hash) -> bool`. The mapping cannot enumerate or read the cache; it can only ask the binary question.

### 3.4 Mid-block partial emission (low-latency consumer paths)

Order-book mirrors, MEV monitors, pre-trade risk screens want to react to a transaction's effects within the same block. Both Subgraphs and Substreams are block-batch: the unit of work is a block, the downstream gets all-or-nothing.

**Why Conduit solves it.** WASIp3's `stream<T>` and `future<T>` are first-class component-model primitives — one component can emit values to another asynchronously, value-by-value, without waiting for end-of-batch. An exported `stream<event>` rather than a return value at end-of-block.

### 3.5 Polyglot pipelines (Rust decoder + Python analytics)

**Subgraph workaround:** rewrite analytics in AssemblyScript. **Substreams workaround:** rewrite in Rust, or run two separate systems.

**Why Conduit solves it.** Component Model is the polyglot Wasm story. WasmGC (Wasm 3.0, W3C standard Sept 17 2025) makes JVM/Kotlin/Dart components viable. Kotlin/Wasm is Beta as of Kotlin 2.2.20 (Sept 10, 2025). Your Rust decoder exports `stream<event>`; your Python analytics imports it. The runtime handles the boundary.

### 3.6 Hot-swappable mapping logic without losing the source cursor

A bug fix in the decoder currently means: redeploy subgraph (resync/graft) or redeploy Substreams (module hash changes → new sync identity, old cache is dead weight).

**Why Conduit solves it.** Components are individually addressable in a composition. If the source-connector component is unchanged, its cursor state is unchanged. Swap the mapping component (new Wasm binary, same WIT signature); the runtime resumes. Same DAG-of-nodes-connected-by-channels pattern as the Meroxa Conduit data-integration project (the name is intentional).

### 3.7 Multiple sinks off one source connection (Postgres + Kafka + Redis)

**Subgraph workaround:** CDC pipeline from Postgres — two cursors, a Debezium dependency. **Substreams workaround:** three sink processes, three gRPC connections, 3× egress, 3× operational footprint.

**Why Conduit solves it.** A pipeline is a DAG. Source connector is one node; three destination components are three sibling nodes downstream of a fan-out. One source connection, one cursor, three sinks.

### 3.8 Effectful precondition checks (sanctions screening, allowlist policy)

**Subgraph/Substreams workaround:** index everything; filter at the GraphQL/sink layer. The sanctioned data is already on the wire.

**Why Conduit solves it.** A policy-check component with `wasi:http` granted only to the policy origin sits in the pipeline before the writer. Drops happen in-pipeline. The mapping never sees the dropped event; the audit trail is the pipeline composition itself, which is signed and addressable.

---

## 4. Counterarguments

### "You can do all of this with Substreams + custom sink + sidecar."

Yes — and that's exactly the operational tax Conduit is trying to retire. Every team building one of these pipelines today runs:
- A Substreams (or subgraph) for chain-side decode
- A custom sink for the effectful step
- A sidecar service for whatever the sink can't do
- A reconciliation layer because cursors now live in three places
- A dashboard to alert when those three pieces drift

This works. It's also expensive in engineering hours, expensive in egress (same blocks decoded twice when the pipeline forks), and brittle in failure modes. Conduit's claim: the same composition with one cursor and one observable failure boundary.

### "Subgraphs are getting Rust mappings via GRC-003/004; isn't that enough?"

GRC-003/004 are great — they're the conservative Rust onramp the ecosystem asked for. But the resulting binary still exports AssemblyScript-compatible host calls. No `stream<T>`, no async, no `wasi:http`, no per-component capability granting, no cross-language composition. Graphite puts it plainly: *"graph-node sees perfectly ordinary AssemblyScript subgraph output"* — that's the feature, and also the ceiling.

### "If WASIp3 isn't even fully stabilized, why build on it now?"

WASIp3 status (not vapor):
- wasi.dev roadmap: *"WASI 0.3.0 previews available in Wasmtime 37+, completion expected around February 2026"*
- Spin v3.5 shipped first WASIp3 RC support (November 2025)
- WebAssembly 3.0 became W3C "live" standard (September 17, 2025) — WasmGC, exception handling, tail calls, 64-bit memory, 128-bit SIMD
- `wasm-tools` and `wit-bindgen` already support the async ABI; idiomatic Rust bindings generated; Wasmtime host work in `wasip3-prototyping`

Risk is real — APIs may shift in tail edges. But the alternative is to wait until the spec is fossilized, by which point first-mover advantage is gone. The Graph has historically led on Wasm-for-indexing (graph-node was an early production user); doing it again on WASIp3 is consistent with that posture. A pipeline written today needs at most a recompile when WASIp3 finalizes, not a rewrite.

### "This is just complexity for its own sake. The DX will be worse than Substreams."

Actually the opposite:
- **No `.spkg` build/pack step.** A Conduit component is a Wasm binary + WIT file. `cargo component build` produces it.
- **No `protoc` and no protobuf-everywhere.** WIT is purpose-built for Wasm component interfaces and more ergonomic than prost-generated `Option<T>` semantics.
- **No reasoning about segments and parallel replay.** No idempotency, segment boundaries, store merge semantics.
- **One cursor, one process, one observability surface.**

The complexity is in the host (wasmtime, Component Model loader, capability granter). The developer surface for writing a pipeline is smaller than Substreams, not larger.

---

## 5. Decision framework

```
                                  ┌─ Need a GraphQL endpoint
                                  │   served by the decentralized
                                  │   network, with PoI?
                                  │   ──► Subgraph.  Done.
                                  │
                                  ├─ Need to backfill historical
                                  │   chain data fast, pure compute,
                                  │   into a warehouse / sink?
                                  │   ──► Substreams.  Done.
                                  │
                                  ├─ Need any of:
       What is your pipeline ────►│     • effectful middleware
       actually doing?            │       (HTTP, oracle, policy)
                                  │     • capability-isolated
                                  │       untrusted plugins
                                  │     • polyglot stages
                                  │       (Rust + Python + …)
                                  │     • mid-block streaming
                                  │       emission
                                  │     • hot-swap of mapping
                                  │       without source resync
                                  │     • multiple sinks off one
                                  │       cursor
                                  │   ──► Conduit.
                                  │
                                  └─ Not sure?
                                      Default to Subgraph if the
                                      output is a queryable API.
                                      Default to Substreams if the
                                      output is a backfill into a
                                      warehouse.  Conduit is the
                                      narrower lane on purpose.
```

Three concrete examples:
- **Uniswap v3 analytics dashboard.** Subgraph. Events → entities → GraphQL.
- **Multi-chain ERC-20 transfer warehouse, full history, into ClickHouse.** Substreams + `substreams-sink-sql`. Canonical Token API shape.
- **NFT marketplace with live floor-price updates + IPFS metadata enrichment + sanctions screening + Postgres mirror + Kafka feed for ML team.** Conduit. Not because the others can't be glued into this, but because gluing four systems is the worst part of every team's roadmap.

---

## 6. Closing

Conduit is not the future of indexing on The Graph. Subgraphs are. Substreams are. Conduit is a **third lane** for a class of pipelines that the first two lanes were not designed for and should not be retrofitted to serve. The 2026 roadmap explicitly frames The Graph as a *modular, multi-service data layer* on Horizon — that framing only works if we stand up purpose-built services for purpose-built workloads.

— Pete
