//! Liminal — a polyglot, capability-isolated WASIp2 component runtime for
//! streaming indexing pipelines.
//!
//! The host is generic: it reads a manifest, wires the declared components into
//! a DAG, grants each its declared capabilities, and streams source messages
//! through. Pipelines are data, not code.

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use wasmtime::component::ResourceTable;
use wasmtime::{Config, Engine};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    p2::{WasiHttpCtxView, WasiHttpView},
    WasiHttpCtx,
};

mod dashboard;
mod manifest;
mod node_bindings;
mod runtime;
mod source;

use manifest::Manifest;
use runtime::Runtime;

// ---------------------------------------------------------------------------
// Host state shared by every node's store.
// ---------------------------------------------------------------------------

pub struct HostState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

impl WasiHttpView for HostState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView { ctx: &mut self.http, table: &mut self.table, hooks: Default::default() }
    }
}

pub fn make_state(ctx: WasiCtx) -> HostState {
    HostState { wasi: ctx, http: WasiHttpCtx::new(), table: ResourceTable::new() }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "liminal", about = "Generic WASIp2 pipeline runtime")]
struct Cli {
    /// Path to the pipeline manifest (TOML).
    manifest: String,
    /// Stop after this many source messages (handy for demos).
    #[arg(long)]
    limit: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "liminal=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let manifest = Manifest::load(&cli.manifest)?;
    info!(name = %manifest.name, nodes = manifest.nodes.len(), "loaded manifest");

    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config)?;

    let runtime = Runtime::load(&engine, &manifest).await?;

    // Stand up the dashboard, if the manifest asked for one, before we connect.
    if let Some(dash) = &manifest.dashboard {
        let html = std::fs::read_to_string(&dash.html)
            .with_context(|| format!("reading dashboard html {:?}", dash.html))?;
        let tx = runtime.output_stream();
        let port = dash.port;
        tokio::spawn(dashboard::serve(port, html, tx));
    }

    info!(source = %manifest.source.kind, "connecting to source");
    let mut source = source::Source::connect(&manifest.source)
        .await
        .context("connecting to source")?;

    runtime.run(&mut source, cli.limit).await
}
