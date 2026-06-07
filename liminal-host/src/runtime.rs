//! The generic DAG runtime.
//!
//! Loads every node from the manifest into its own capability-scoped store,
//! then streams messages from the source through the graph. The runtime never
//! knows what a "swap" is — it moves bytes from node to node and lets the
//! components give them meaning.

use std::collections::VecDeque;

use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use wasmtime::{
    component::{Component, Linker},
    Engine, Store,
};
use wasmtime_wasi::WasiCtxBuilder;

use crate::manifest::{Capability, Manifest, NodeSpec, SOURCE_ID};
use crate::node_bindings::LiminalNode;
use crate::source::Source;
use crate::{make_state, HostState};

/// A downstream edge: the target node index and, optionally, the discriminant
/// (`when`) a message must carry to travel along it.
struct Successor {
    to: usize,
    when: Option<String>,
}

/// A single instantiated node: its own store, its own component instance, and
/// the edges its output flows along.
struct Node {
    id: String,
    store: Store<HostState>,
    instance: LiminalNode,
    /// Outgoing edges into [`Runtime::nodes`].
    successors: Vec<Successor>,
}

impl Node {
    /// A node with no successors is a sink: its output is pipeline output.
    fn is_terminal(&self) -> bool {
        self.successors.is_empty()
    }
}

/// Read the routing discriminant (`"tag"`) from a JSON message, if present.
/// Components emit routable variants as serde internally-tagged enums
/// (`#[serde(tag = "tag")]`), so the case name lands in this field.
fn message_tag(payload: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()?
        .get("tag")?
        .as_str()
        .map(str::to_owned)
}

/// The wired-up pipeline, ready to run.
pub struct Runtime {
    nodes: Vec<Node>,
    /// Indices of nodes fed directly by the source.
    roots: Vec<usize>,
    /// Broadcast channel for terminal output, consumed by the dashboard (if any).
    output_tx: broadcast::Sender<String>,
}

impl Runtime {
    /// Instantiate every node in the manifest, each in an isolated store with
    /// exactly the capabilities it was granted — nothing more.
    pub async fn load(engine: &Engine, manifest: &Manifest) -> Result<Self> {
        // Resolve node id -> index up front so edges can be turned into indices.
        let index_of = |id: &str| -> Result<usize> {
            manifest
                .nodes
                .iter()
                .position(|n| n.id == id)
                .with_context(|| format!("edge references unknown node {id:?}"))
        };

        let mut nodes = Vec::with_capacity(manifest.nodes.len());
        for spec in &manifest.nodes {
            let (store, instance) = instantiate_node(engine, spec)
                .await
                .with_context(|| format!("loading node {:?}", spec.id))?;
            nodes.push(Node {
                id: spec.id.clone(),
                store,
                instance,
                successors: Vec::new(),
            });
        }

        // Translate edges into adjacency (by index) and collect the roots.
        let mut roots = Vec::new();
        for edge in &manifest.edges {
            let to = index_of(&edge.to)?;
            if edge.from == SOURCE_ID {
                if !roots.contains(&to) {
                    roots.push(to);
                }
            } else {
                let from = index_of(&edge.from)?;
                nodes[from].successors.push(Successor { to, when: edge.when.clone() });
            }
        }

        let (output_tx, _) = broadcast::channel(256);

        for n in &nodes {
            info!(
                node = %n.id,
                terminal = n.is_terminal(),
                successors = n.successors.len(),
                "node ready"
            );
        }

        Ok(Self { nodes, roots, output_tx })
    }

    /// A handle to the terminal-output stream, for wiring up a dashboard.
    pub fn output_stream(&self) -> broadcast::Sender<String> {
        self.output_tx.clone()
    }

    /// Run until the source is exhausted or `limit` source messages have been
    /// processed (whichever comes first).
    pub async fn run(mut self, source: &mut Source, limit: Option<u64>) -> Result<()> {
        info!(roots = self.roots.len(), "pipeline running");
        let mut processed = 0u64;

        while let Some(result) = source.next().await {
            let message = result?;

            // Breadth-first propagation through the DAG, one source message at a
            // time. Deterministic and easy to reason about; concurrent fan-out
            // across sibling branches is a future enhancement.
            let mut queue: VecDeque<(usize, Vec<u8>)> =
                self.roots.iter().map(|&r| (r, message.clone())).collect();

            while let Some((idx, input)) = queue.pop_front() {
                let outputs = self.call_node(idx, input).await?;
                // Snapshot routing decisions (to, when) so we don't hold a
                // borrow on self.nodes across the enqueue.
                let routes: Vec<(usize, Option<String>)> = self.nodes[idx]
                    .successors
                    .iter()
                    .map(|s| (s.to, s.when.clone()))
                    .collect();

                for out in outputs {
                    if routes.is_empty() {
                        self.emit(idx, out);
                        continue;
                    }
                    let tag = message_tag(&out);
                    let mut routed = false;
                    for (to, when) in &routes {
                        // Unconditional edge, or the message's tag matches.
                        if when.is_none() || when.as_deref() == tag.as_deref() {
                            queue.push_back((*to, out.clone()));
                            routed = true;
                        }
                    }
                    if !routed {
                        debug!(
                            node = %self.nodes[idx].id,
                            tag = tag.as_deref().unwrap_or("<none>"),
                            "output matched no outgoing edge; dropping"
                        );
                    }
                }
            }

            processed += 1;
            if limit.is_some_and(|lim| processed >= lim) {
                info!(processed, "message limit reached, stopping");
                break;
            }
        }

        info!(processed, "pipeline stopped");
        Ok(())
    }

    /// Invoke one node's `transform`. A node returning `err` is logged and
    /// treated as "no output" — one poisoned message never sinks the pipeline.
    async fn call_node(&mut self, idx: usize, input: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        let node = &mut self.nodes[idx];
        let result = node
            .instance
            .liminal_node_node()
            .call_transform(&mut node.store, &input)
            .await
            .map_err(anyhow::Error::from)
            .with_context(|| format!("calling node {:?}", node.id))?;

        match result {
            Ok(outputs) => {
                if !outputs.is_empty() {
                    debug!(node = %node.id, emitted = outputs.len(), "node produced output");
                }
                Ok(outputs)
            }
            Err(msg) => {
                warn!(node = %node.id, error = %msg, "node reported a per-message error");
                Ok(Vec::new())
            }
        }
    }

    /// Terminal output: log it and broadcast to any connected dashboard.
    fn emit(&self, idx: usize, payload: Vec<u8>) {
        let node = &self.nodes[idx];
        match String::from_utf8(payload) {
            Ok(line) => {
                info!(node = %node.id, "output: {line}");
                // Ignore send errors — they only mean no dashboard is listening.
                let _ = self.output_tx.send(line);
            }
            Err(_) => warn!(node = %node.id, "terminal output was not valid UTF-8; dropping"),
        }
    }
}

/// Build a node's store with precisely its granted capabilities, then
/// instantiate the component against a linker carrying only those capabilities.
///
/// This is where capability isolation actually happens: if a component imports
/// `wasi:http` but the manifest didn't grant `http`, the linker has no such
/// import and instantiation fails — loudly, at load time, by construction.
async fn instantiate_node(
    engine: &Engine,
    spec: &NodeSpec,
) -> Result<(Store<HostState>, LiminalNode)> {
    let caps = spec.parsed_capabilities()?;
    let grants_http = caps.contains(&Capability::Http);
    let grants_stdout = caps.contains(&Capability::Stdout);

    let mut linker: Linker<HostState> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    if grants_http {
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    }

    // W2: scope HTTP egress to the declared origins. An allow-list without the
    // http grant is meaningless, so warn the operator rather than silently
    // ignoring it.
    if !spec.allow_origins.is_empty() && !grants_http {
        warn!(node = %spec.id, "allow_origins set but the http capability isn't granted; ignoring");
    }
    let http_policy = if grants_http && !spec.allow_origins.is_empty() {
        crate::http_policy::OriginPolicy::restricted(&spec.allow_origins)
    } else {
        crate::http_policy::OriginPolicy::unrestricted()
    };

    // stderr is always available so components can log; stdout is a grant.
    let mut builder = WasiCtxBuilder::new();
    builder.inherit_stderr();
    if grants_stdout {
        builder.inherit_stdout();
    }
    for (key, val) in &spec.env {
        builder.env(key, val);
    }

    let component = Component::from_file(engine, &spec.wasm)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("reading wasm {:?}", spec.wasm))?;
    let mut store = Store::new(engine, make_state(builder.build(), http_policy));

    let instance = LiminalNode::instantiate_async(&mut store, &component, &linker)
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| {
            format!(
                "instantiating {:?} — if it imports a capability (e.g. wasi:http) \
                 make sure the manifest grants it",
                spec.wasm
            )
        })?;

    Ok((store, instance))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::NodeSpec;
    use std::collections::BTreeMap;
    use wasmtime::Config;

    fn test_engine() -> Engine {
        let mut config = Config::new();
        config.wasm_component_model(true);
        Engine::new(&config).unwrap()
    }

    fn enricher_spec(caps: &[&str]) -> NodeSpec {
        NodeSpec {
            id: "enricher".into(),
            wasm: "../examples/cross-dex-arb/enricher.wasm".into(),
            sha256: None,
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            allow_origins: vec![],
            env: BTreeMap::new(),
        }
    }

    /// The headline property, proven against the real component: the enricher
    /// imports `wasi:http`, so it loads with the `http` grant and is *refused*
    /// without it. Capability isolation enforced at link time, by construction.
    #[tokio::test]
    async fn http_capability_is_enforced_at_load() {
        let wasm = "../examples/cross-dex-arb/enricher.wasm";
        if !std::path::Path::new(wasm).exists() {
            eprintln!("skipping: build components first (`just build`)");
            return;
        }
        let engine = test_engine();

        let denied = instantiate_node(&engine, &enricher_spec(&[])).await;
        assert!(denied.is_err(), "enricher must NOT instantiate without the http grant");

        let granted = instantiate_node(&engine, &enricher_spec(&["http"])).await;
        assert!(granted.is_ok(), "enricher must instantiate with the http grant");
    }
}
