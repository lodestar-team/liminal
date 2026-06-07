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

## Live demo (origin-scoped HTTP screening + fail-closed)

The offline run uses a compiled-in list. The **live** pipeline points the screener at a real local
provider it reaches over origin-scoped `wasi:http`:

```bash
just run-customs-live          # starts screening-server on :8088, runs customs.live.pipeline.toml
```

- `screener-http` is granted `http` + `allow_origins = ["http://localhost:8088"]` (W2) and
  `keyvalue = "verdicts"` (W4). It can reach the screening origin and nowhere else.
- **Fail-closed (W7):** stop the server and re-run — every transfer is held, nothing is written.
  A gate that fails open is no gate. This is asserted in CI
  (`unreachable_provider_holds_everything`).

`docker-compose.yml` brings up Postgres/Kafka/Redis for wiring the sinks to real services and giving
the hold store durable, Redis-backed persistence (the remaining W7 upgrade).

## Going live

Swap the `[source]` block from `fixture` to `evm` (RPC + Transfer topic + token addresses) — no
code change. The screener's compiled-in list becomes an origin-scoped `wasi:http` call to a real
provider (origin allow-lists are enforced today — **W2**); the verdict cache already runs on the
namespaced key-value store (**W4**), with durable, cross-restart hold arriving when it's backed by
Redis (**W7**); the composition is already content-addressed and signed (**W1+/W8**).

## Signed composition (W1+ / W8)

The topology is signed. The canonical composition — component ids + the `sha256` of their wasm
bytes + capability declarations + the edge set — is hashed and signed with ed25519, excluding all
`${VAR}` secrets. `customs.pub` and `customs.pipeline.toml.sig` are committed as the attestation
artifacts (the secret `customs.key` is git-ignored).

```bash
just verify-customs          # checks the committed signature + content addresses
#   …or: liminal compose verify examples/customs/customs.pipeline.toml \
#          --sig examples/customs/customs.pipeline.toml.sig --pub examples/customs/customs.pub
```

Rebuilding the components changes their content addresses, so re-sign after a rebuild:
`liminal compose sign examples/customs/customs.pipeline.toml --key examples/customs/customs.key`.

## Verdict cache (W4)

The screener memoises each address's classification in the host key-value store, namespace
`verdicts`, keyed by `(list_version, address)` — so the screening "call" runs once per address per
list version, and a list update busts the cache automatically (the key changes). The namespace is
isolated by the host: no other component can read or write it. The screener is built with the
`node_kv!` SDK macro and granted `keyvalue = "verdicts"` in the manifest — without that grant it
won't instantiate.

## Status against the RFC

Shipped: conditional `when` routing (**W3**), fixture + address-filtered EVM source (**W5**), the
seven components and manifest (**W6**), content addressing + `compose hash|sign|verify` (**W1+/W8**),
HTTP origin allow-lists (**W2**), namespaced key-value with the verdict cache (**W4**), and the
attestation + drop-path tests.

Pending (see root README roadmap): the full infra harness — `screening-server`, `docker-compose`,
Redis-backed durable hold, and the fail-closed + cache-bust integration tests (**W7**); plus the
deliberate Wasmtime-45 bump to the standard `wasi:keyvalue` interface.
