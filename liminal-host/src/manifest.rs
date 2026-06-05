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
    /// Currently only `"evm"` — an EVM log subscription over WebSocket.
    #[serde(rename = "type")]
    pub kind: String,
    /// RPC endpoint. Supports `${ENV_VAR}` interpolation.
    pub rpc: String,
    /// Event signature hashes (topic0) to subscribe to.
    pub topics: Vec<String>,
}

/// One component in the DAG.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// Unique node id, referenced by edges.
    pub id: String,
    /// Path to the component `.wasm`, relative to the current directory.
    pub wasm: String,
    /// Capabilities granted to this node and this node only. See [`Capability`].
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Per-node environment variables. Values support `${ENV_VAR}`.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// A directed edge `from -> to`. The special source id is `"source"`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: String,
    pub to: String,
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
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading manifest {path}"))?;
        let interpolated = interpolate_env(&raw)
            .with_context(|| format!("interpolating env vars in {path}"))?;
        let manifest: Manifest = toml::from_str(&interpolated)
            .with_context(|| format!("parsing manifest {path}"))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Reject malformed graphs early, with a clear message, before we touch
    /// Wasmtime. Cheap insurance against a baffling runtime panic later.
    fn validate(&self) -> Result<()> {
        if self.source.kind != "evm" {
            bail!("unsupported source type {:?} (only \"evm\" for now)", self.source.kind);
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
fn interpolate_env(input: &str) -> Result<String> {
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
            interpolate_env("rpc = \"${LIMINAL_TEST_VAR}/path\"").unwrap(),
            "rpc = \"wss://example/path\""
        );
        assert!(interpolate_env("${DEFINITELY_NOT_SET_42}").is_err());
        assert!(interpolate_env("no vars here").unwrap() == "no vars here");
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
