//! Pipeline manifests — the whole point of v0.2.
//!
//! A `pipeline.toml` declares a source, a set of nodes (each a `.wasm` plus the
//! capabilities it's granted), and the edges between them. The host reads this
//! and wires the DAG. No host recompile, ever.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// A complete pipeline definition.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Human-readable pipeline name (used in logs).
    pub name: String,
    /// Where raw messages come from.
    pub source: SourceSpec,
    /// The components, in any order — edges define the topology.
    pub nodes: Vec<NodeSpec>,
    /// Directed edges. `from = "source"` marks a pipeline root.
    pub edges: Vec<Edge>,
    /// Optional live dashboard fed by terminal-node output.
    #[serde(default)]
    pub dashboard: Option<DashboardSpec>,
}

/// The data feed at the head of the pipeline.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// `"evm"` — an EVM log subscription over WebSocket; or
    /// `"fixture"` — newline-delimited JSON messages from a file (offline runs).
    #[serde(rename = "type")]
    pub kind: String,
    /// RPC endpoint for `evm`. Supports `${ENV_VAR}` interpolation.
    #[serde(default)]
    pub rpc: Option<String>,
    /// Event signature hashes (topic0) to subscribe to (`evm`).
    #[serde(default)]
    pub topics: Vec<String>,
    /// Optional contract-address allow-list to filter on (`evm`). Empty = any.
    #[serde(default)]
    pub addresses: Vec<String>,
    /// Path to a newline-delimited JSON fixture file (`fixture`).
    #[serde(default)]
    pub path: Option<String>,
}

/// One component in the DAG.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// Unique node id, referenced by edges.
    pub id: String,
    /// Path to the component `.wasm`, relative to the current directory.
    pub wasm: String,
    /// Optional content address (hex sha256) of the wasm. `compose verify`
    /// cross-checks it against the file; `compose hash` can fill it in.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Capabilities granted to this node and this node only. See [`Capability`].
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// HTTP origin allow-list (W2). Only meaningful with the `http` capability.
    /// Empty = unrestricted; non-empty = the host rejects any egress to an
    /// origin not in this list. Entries are `scheme://authority`, e.g.
    /// `https://coins.llama.fi`.
    #[serde(default)]
    pub allow_origins: Vec<String>,
    /// Key-value namespace (W4). Presence grants the `liminal:kv/store` import,
    /// scoped to this namespace; absence means no key-value access at all.
    #[serde(default)]
    pub keyvalue: Option<String>,
    /// Per-node environment variables. Values support `${ENV_VAR}`.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// A directed edge `from -> to`. The special source id is `"source"`.
///
/// `when` makes the edge conditional: the message flows along it only if the
/// emitting node's output carries a discriminant (`"tag"` field) equal to
/// `when`. An edge with no `when` is unconditional (classic fan-out). This is
/// W3 — the routing that lets a `flagged` verdict reach quarantine while never
/// having an edge to the writer.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub when: Option<String>,
}

/// Live SSE dashboard configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardSpec {
    pub port: u16,
    /// Path to the HTML file served at `/`.
    pub html: String,
}

/// The reserved node id that denotes the source feed.
pub const SOURCE_ID: &str = "source";

/// A capability a node can be granted. The host enforces these at link time:
/// a node that imports a capability it wasn't granted simply fails to load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Outbound HTTP via `wasi:http` — network egress.
    Http,
    /// Write access to the process stdout.
    Stdout,
}

impl Capability {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "http" => Ok(Capability::Http),
            "stdout" => Ok(Capability::Stdout),
            other => bail!("unknown capability {other:?} (known: http, stdout)"),
        }
    }
}

impl NodeSpec {
    /// Parse and validate this node's capability grants.
    pub fn parsed_capabilities(&self) -> Result<Vec<Capability>> {
        self.capabilities
            .iter()
            .map(|c| Capability::parse(c).with_context(|| format!("node {:?}", self.id)))
            .collect()
    }
}

impl Manifest {
    /// Load a manifest from a TOML file, interpolating `${ENV_VAR}` references,
    /// then validate the DAG.
    pub fn load(path: &str) -> Result<Self> {
        Self::load_inner(path, false)
    }

    /// Like [`Manifest::load`] but unresolved `${ENV_VAR}` references become
    /// empty strings instead of errors. Used by `compose`, which only cares
    /// about the structural topology — not the secret runtime values it excludes.
    pub fn load_lenient(path: &str) -> Result<Self> {
        Self::load_inner(path, true)
    }

    fn load_inner(path: &str, lenient: bool) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading manifest {path}"))?;
        let interpolated = interpolate_env(&raw, lenient)
            .with_context(|| format!("interpolating env vars in {path}"))?;
        let manifest: Manifest = toml::from_str(&interpolated)
            .with_context(|| format!("parsing manifest {path}"))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Reject malformed graphs early, with a clear message, before we touch
    /// Wasmtime. Cheap insurance against a baffling runtime panic later.
    fn validate(&self) -> Result<()> {
        match self.source.kind.as_str() {
            "evm" => {
                if self.source.rpc.is_none() {
                    bail!("evm source requires `rpc`");
                }
                if self.source.topics.is_empty() {
                    bail!("evm source requires at least one `topics` entry");
                }
            }
            "fixture" => {
                if self.source.path.is_none() {
                    bail!("fixture source requires `path`");
                }
            }
            other => bail!("unsupported source type {other:?} (known: evm, fixture)"),
        }
        if self.nodes.is_empty() {
            bail!("manifest has no nodes");
        }

        // Node ids must be unique and must not collide with the source id.
        let mut ids = std::collections::HashSet::new();
        for node in &self.nodes {
            if node.id == SOURCE_ID {
                bail!("node id {SOURCE_ID:?} is reserved for the source");
            }
            if !ids.insert(node.id.as_str()) {
                bail!("duplicate node id {:?}", node.id);
            }
            node.parsed_capabilities()?; // surface bad capability strings now
        }

        // Every edge endpoint must exist; `source` is only ever a `from`.
        let mut has_root = false;
        for edge in &self.edges {
            if edge.from == SOURCE_ID {
                has_root = true;
            } else if !ids.contains(edge.from.as_str()) {
                bail!("edge from unknown node {:?}", edge.from);
            }
            if edge.to == SOURCE_ID {
                bail!("nothing may flow into the source ({:?} -> source)", edge.from);
            }
            if !ids.contains(edge.to.as_str()) {
                bail!("edge to unknown node {:?}", edge.to);
            }
        }
        if !has_root {
            bail!("no root: at least one edge must start from \"source\"");
        }

        Ok(())
    }
}

/// Replace `${NAME}` with the value of environment variable `NAME`.
///
/// A missing variable is a hard error — better than silently shipping an empty
/// RPC URL — unless a default is supplied with `${NAME:-default}`, in which case
/// the default (which may be empty) is used.
fn interpolate_env(input: &str, lenient: bool) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .with_context(|| "unterminated ${...} in manifest")?;
        let expr = &after[..end];

        let (name, default) = match expr.split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (expr, None),
        };
        let val = match (std::env::var(name), default) {
            (Ok(v), _) => v,
            (Err(_), Some(d)) => d.to_string(),
            (Err(_), None) if lenient => String::new(),
            (Err(_), None) => anyhow::bail!(
                "environment variable {name:?} referenced but not set \
                 (use ${{{name}:-default}} to supply a fallback)"
            ),
        };
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolation_substitutes_and_errors() {
        std::env::set_var("LIMINAL_TEST_VAR", "wss://example");
        assert_eq!(
            interpolate_env("rpc = \"${LIMINAL_TEST_VAR}/path\"", false).unwrap(),
            "rpc = \"wss://example/path\""
        );
        assert!(interpolate_env("${DEFINITELY_NOT_SET_42}", false).is_err());
        // Lenient: unknown vars collapse to empty rather than erroring.
        assert_eq!(interpolate_env("x${DEFINITELY_NOT_SET_42}y", true).unwrap(), "xy");
        assert!(interpolate_env("no vars here", false).unwrap() == "no vars here");
    }

    #[test]
    fn validate_rejects_dangling_edges() {
        let toml = r#"
            name = "t"
            [source]
            type = "evm"
            rpc = "wss://x"
            topics = ["0xabc"]
            [[nodes]]
            id = "a"
            wasm = "a.wasm"
            [[edges]]
            from = "source"
            to = "ghost"
        "#;
        let m: Manifest = toml::from_str(toml).unwrap();
        assert!(m.validate().is_err());
    }

    /// The Customs compliance claim, asserted against the real manifest. If
    /// this fails, the topology no longer guarantees non-ingestion — treat it
    /// as a compliance regression, not a flaky test.
    #[test]
    fn customs_writer_is_unreachable_by_flagged() {
        let path = "../examples/customs/customs.pipeline.toml";
        if !std::path::Path::new(path).exists() {
            eprintln!("skipping: customs manifest not present");
            return;
        }
        let m = Manifest::load(path).expect("customs manifest must load");

        // 1. The writer has no `http` capability — it cannot call out.
        let sor = m.nodes.iter().find(|n| n.id == "sink-sor").expect("sink-sor node");
        assert!(
            !sor.capabilities.iter().any(|c| c == "http"),
            "compliance regression: sink-sor must NOT hold the http capability"
        );

        // 2. Every edge into the writer comes from the enricher.
        let into_sor: Vec<_> = m.edges.iter().filter(|e| e.to == "sink-sor").collect();
        assert!(!into_sor.is_empty(), "sink-sor must be reachable");
        for e in &into_sor {
            assert_eq!(e.from, "enricher", "the only path to sink-sor is via enricher");
        }

        // 3. Every edge into the enricher comes from screener `when = cleared`.
        let into_enricher: Vec<_> = m.edges.iter().filter(|e| e.to == "enricher").collect();
        assert!(!into_enricher.is_empty(), "enricher must be reachable");
        for e in &into_enricher {
            assert_eq!(e.from, "screener", "enricher is fed only by the screener");
            assert_eq!(
                e.when.as_deref(),
                Some("cleared"),
                "compliance regression: only CLEARED verdicts may reach the enricher → writer"
            );
        }
    }

    #[test]
    fn validate_accepts_a_simple_chain() {
        let toml = r#"
            name = "t"
            [source]
            type = "evm"
            rpc = "wss://x"
            topics = ["0xabc"]
            [[nodes]]
            id = "a"
            wasm = "a.wasm"
            capabilities = ["http"]
            [[edges]]
            from = "source"
            to = "a"
        "#;
        let m: Manifest = toml::from_str(toml).unwrap();
        m.validate().unwrap();
        assert_eq!(m.nodes[0].parsed_capabilities().unwrap(), vec![Capability::Http]);
    }
}
