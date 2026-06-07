# Customs — audit artifact

This document is for an auditor (internal compliance, external assessor, regulator) who needs to
attest that **sanctioned activity cannot be ingested into the system of record** — and that the
control is *architectural*, not a code-review promise.

The whole attestation reduces to **two facts you can read off a signed file**, plus a one-command
integrity check.

---

## What you are attesting

> A flagged (or unresolvable) transfer never reaches the system-of-record writer, and the writer is
> structurally incapable of fetching it.

This holds because of the pipeline's *shape*, captured in
[`customs.pipeline.toml`](./customs.pipeline.toml):

```
fixtures ─▶ decoder ─▶ screener
                         ├─ cleared ──────▶ enricher ─▶ {sink-sor, sink-kafka}
                         ├─ flagged ───────────────────▶ sink-quarantine
                         └─ indeterminate ─────────────▶ sink-hold
```

### Fact 1 — the writer cannot call out

`sink-sor` (the system-of-record writer) declares **no `http` capability**. By the WebAssembly
Component Model, a component can only invoke interfaces its world imports; the writer's world does
not import `wasi:http`, so it cannot make a network request — there is no flag to misconfigure and
no code path to audit. (Liminal proves this enforcement in CI:
`runtime::tests::http_capability_is_enforced_at_load` — a component that imports `wasi:http` fails
to instantiate without the grant.)

### Fact 2 — flagged events have no path to the writer

The only edge into `sink-sor` is from `enricher`, and the only edge into `enricher` is
`screener … when = "cleared"`. A `flagged` or `indeterminate` verdict is routed by the **host** to
`sink-quarantine` / `sink-hold` respectively — there is no edge from those verdicts to the writer.

Both facts are machine-checked on every commit by
`manifest::tests::customs_writer_is_unreachable_by_flagged`, and demonstrated end-to-end by
`tests/customs_e2e.rs::flagged_transfers_never_reach_the_writer`. **A change that breaks either is a
compliance regression and fails CI.**

---

## Verifying the artifact

The composition is content-addressed and signed (ed25519). Verifying proves the topology, the
capability boundaries, and the exact component binaries are the attested ones:

```bash
just verify-customs
#   …or:
cargo run --release -p liminal-host -- compose verify \
    examples/customs/customs.pipeline.toml \
    --sig examples/customs/customs.pipeline.toml.sig \
    --pub examples/customs/customs.pub
# → OK: signature valid for composition sha256:<hash>
```

`compose hash` prints the per-component content addresses (sha256 of each `.wasm`) and the canonical
composition hash, so you can record exactly what ran.

### What the signature covers

The **canonical composition**: each component's id + the `sha256` of its wasm bytes + its capability
declarations (`capabilities`, `allow_origins`, `keyvalue` namespace) + the edge set (with `when`
conditions) + the structural source filter.

### What it deliberately excludes

All `${VAR}` runtime values — RPC URLs, DB credentials, broker lists, provider endpoints. The same
signature therefore validates in staging and production against different endpoints: it attests
*structure and capability boundaries*, not secrets.

---

## Capability boundaries in this pipeline

| Component | http | allow_origins | keyvalue | Notes |
|---|---|---|---|---|
| `decoder` | — | — | — | pure compute |
| `screener` (offline) | — | — | `verdicts` | compiled-in list; private verdict cache |
| `screener-http` (live) | ✅ | screening origin only | `verdicts` | fails closed if provider unreachable |
| `enricher` | — | — | — | price enrichment |
| `sink-sor` | **—** | — | — | **the writer: no egress, by construction** |
| `sink-kafka` | — | — | — | second fan-out leg |
| `sink-quarantine` | — | — | — | destination for flagged |
| `sink-hold` | — | — | — | fail-closed destination |

---

## Threat model (honesty)

- **TCB** = the host + Wasmtime. A compromised host defeats everything; the guarantee is "given an
  honest host, a malicious or buggy *component* is contained."
- It attests **integrity/provenance of the composition**, not **correctness of the screening data**.
  A wrong sanctions list is faithfully enforced — list quality is the provider's problem.
- The pipeline is **non-deterministic** (it makes HTTP calls), so the signature does **not** make
  outputs reproducible from the manifest. It attests "this topology with these capability boundaries
  ran," not "these exact rows can be re-derived." This pipeline is operator-run and off-network —
  **not** eligible for indexing rewards or Proof-of-Indexing, by design.

See [RFC-LIM-001 §7](./RFC.md) for the full treatment.
