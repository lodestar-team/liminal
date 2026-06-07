//! Liminal — a polyglot, capability-isolated WASIp2 component runtime for
//! streaming indexing pipelines.
//!
//! The host is generic: it reads a manifest, wires the declared components into
//! a DAG, grants each its declared capabilities, and streams source messages
//! through. Pipelines are data, not code.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::info;
use wasmtime::component::ResourceTable;
use wasmtime::{Config, Engine};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    p2::{WasiHttpCtxView, WasiHttpView},
    WasiHttpCtx,
};

mod compose;
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
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a pipeline from its manifest.
    Run {
        /// Path to the pipeline manifest (TOML).
        manifest: String,
        /// Stop after this many source messages (handy for demos).
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Inspect, hash, sign, and verify a pipeline composition.
    #[command(subcommand)]
    Compose(ComposeCmd),
}

#[derive(Subcommand)]
enum ComposeCmd {
    /// Print component content addresses and the canonical composition hash.
    Hash { manifest: String },
    /// Generate an ed25519 keypair: <out>.key (secret) and <out>.pub.
    Keygen {
        /// Output path prefix (e.g. `customs` → customs.key, customs.pub).
        out: String,
    },
    /// Sign a composition; writes <manifest>.sig.
    Sign {
        manifest: String,
        #[arg(long)]
        key: String,
    },
    /// Verify a composition's signature and component content addresses.
    Verify {
        manifest: String,
        #[arg(long)]
        sig: String,
        #[arg(long = "pub")]
        public: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "liminal=info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Run { manifest, limit } => run(&manifest, limit).await,
        Command::Compose(ComposeCmd::Hash { manifest }) => compose::hash(&manifest),
        Command::Compose(ComposeCmd::Keygen { out }) => compose::keygen(&out),
        Command::Compose(ComposeCmd::Sign { manifest, key }) => compose::sign(&manifest, &key),
        Command::Compose(ComposeCmd::Verify { manifest, sig, public }) => {
            compose::verify(&manifest, &sig, &public)
        }
    }
}

async fn run(manifest_path: &str, limit: Option<u64>) -> Result<()> {
    let manifest = Manifest::load(manifest_path)?;
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

    runtime.run(&mut source, limit).await
}
