# Customs — compliance-gated transfer indexer

The first *compliance-grade* Liminal pipeline. It screens ERC-20 transfers against a sanctions
list **before any event reaches the system-of-record**, and proves the one thing no
Subgraph + Substreams + sidecar stack can express: **architectural non-ingestion under capability
isolation**.

Full spec: [RFC-LIM-001](./RFC.md).

```
fixtures ─▶ decoder ─▶ screener
                         ├─ cleared ──────▶ enricher ─▶ {sink-sor, sink-kafka}
                         ├─ flagged ───────────────────▶ sink-quarantine
                         └─ indeterminate ─────────────▶ sink-hold   (fail-closed)
```

## The claim, verifiable from `customs.pipeline.toml` alone

1. **`sink-sor` declares no `http` capability** — the writer is *structurally* incapable of
   calling out (Component-Model grant layer; a component can only invoke interfaces its world
   imports).
2. **The only edge into `sink-sor` is `enricher`, whose only inbound edge is
   `screener … when = "cleared"`.** A `flagged` or `indeterminate` transfer has *no path* to the
   writer. The compliance control is the topology.

Both facts are asserted in CI:
- `manifest::tests::customs_writer_is_unreachable_by_flagged` (the manifest proof)
- `tests/customs_e2e.rs::flagged_transfers_never_reach_the_writer` (the live drop-path proof)

## Run it (offline, no RPC or services)

```bash
just build          # build host + all components, stage wasm
just run-customs
```

You'll see each transfer routed by verdict:

```
SOR        {... "tx_hash":"0xaa01" ...}     # cleared  → system of record + kafka
KAFKA      {... "tx_hash":"0xaa01" ...}
QUARANTINE {... "tx_hash":"0xaa02" ...}     # flagged  → quarantine ONLY
HOLD       {... "tx_hash":"0xcc01" ...}     # indeterminate → hold ONLY (fail-closed)
```

The flagged transfers (`0xaa02` to, `0xbb02` from the OFAC-SDN address) appear **only** in
quarantine — never in `SOR`/`KAFKA`. The unresolvable counterparty (`0xcc01` → `0x…dead`) is held,
not written.

## Going live

Swap the `[source]` block from `fixture` to `evm` (RPC + Transfer topic + token addresses) — no
code change. The screener's compiled-in list becomes an origin-scoped `wasi:http` call to a real
provider (tracked as **W2** in the root README roadmap); durable hold + verdict caching land with
`wasi:keyvalue` (**W4**); signed, content-addressed composition with **W1/W8**.

## Status against the RFC

Shipped: conditional `when` routing (**W3**), fixture + address-filtered EVM source (**W5**), the
seven components and manifest (**W6**), the attestation + drop-path tests.

Pending (see root README roadmap): HTTP origin allow-lists (**W2**), `wasi:keyvalue` namespacing
(**W4** — needs a Wasmtime 45 bump), content addressing + signing (**W1+/W8**), and the full
infra harness — `screening-server`, `docker-compose`, Redis-backed durable hold (**W7**).
