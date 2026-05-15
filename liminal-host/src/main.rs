use anyhow::Result;
use clap::Parser;
use tracing::info;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, p2::{WasiHttpCtxView, WasiHttpView}};

mod bindings;
mod pipeline;
mod source;

const SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

#[derive(Parser)]
#[command(name = "liminal", about = "Liminal pipeline runner")]
struct Args {
    #[arg(long, env = "ETH_RPC_URL")]
    rpc: String,
    #[arg(long, default_value = "examples/uni-v3-swaps/decoder.wasm")]
    decoder: String,
    #[arg(long, default_value = "examples/uni-v3-swaps/price-enricher.wasm")]
    enricher: String,
    #[arg(long, default_value = "examples/uni-v3-swaps/sink-postgres.wasm")]
    sink_postgres: String,
    #[arg(long, default_value = "examples/uni-v3-swaps/sink-kafka.wasm")]
    sink_kafka: String,
    #[arg(long, default_value = "https://coins.llama.fi", env = "ORACLE_URL")]
    oracle_url: String,
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "KAFKA_BROKERS")]
    kafka_brokers: Option<String>,
    #[arg(long)]
    limit: Option<u64>,
}

// ---------------------------------------------------------------------------
// Host state — shared by all component stores.
//
// Both WasiCtx and WasiHttpCtx are always present.  Capability isolation is
// enforced at the LINKER level: only the enricher's linker has
// wasmtime_wasi_http::p2::add_to_linker_async called on it.
// ---------------------------------------------------------------------------
pub struct HostState {
    wasi:  WasiCtx,
    http:  WasiHttpCtx,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "liminal=info".into()),
        )
        .init();

    let args = Args::parse();

    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config)?;

    info!("loading components");
    let decoder_component = Component::from_file(&engine, &args.decoder)
        .map_err(|e| anyhow::anyhow!("loading decoder from {}: {e:#}", args.decoder))?;
    let enricher_component = Component::from_file(&engine, &args.enricher)
        .map_err(|e| anyhow::anyhow!("loading enricher from {}: {e:#}", args.enricher))?;
    let pg_component = Component::from_file(&engine, &args.sink_postgres)
        .map_err(|e| anyhow::anyhow!("loading postgres sink from {}: {e:#}", args.sink_postgres))?;
    let kafka_component = Component::from_file(&engine, &args.sink_kafka)
        .map_err(|e| anyhow::anyhow!("loading kafka sink from {}: {e:#}", args.sink_kafka))?;

    // Decoder — basic WASI only, no HTTP capability.
    let (decoder_store, decoder_bindings) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        let ctx = WasiCtxBuilder::new().inherit_stderr().build();
        let mut store = Store::new(&engine, make_state(ctx));
        let bindings = bindings::decoder::DecoderWorld::instantiate_async(
            &mut store, &decoder_component, &linker,
        ).await?;
        (store, bindings)
    };

    // Enricher — WASI + HTTP (oracle URL scoped to this instance only).
    let (enricher_store, enricher_bindings) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
        let ctx = WasiCtxBuilder::new()
            .inherit_stderr()
            .env("ORACLE_URL", &args.oracle_url)
            .build();
        let mut store = Store::new(&engine, make_state(ctx));
        let bindings = bindings::enricher::EnricherWorld::instantiate_async(
            &mut store, &enricher_component, &linker,
        ).await?;
        (store, bindings)
    };

    let pipeline = pipeline::Pipeline {
        decoder:      (decoder_store,  decoder_bindings),
        enricher:     (enricher_store, enricher_bindings),
        pg_sink:      pg_component,
        kafka_sink:   kafka_component,
        engine:       engine.clone(),
        database_url: args.database_url,
        kafka_brokers: args.kafka_brokers,
    };

    info!(rpc = %args.rpc, "connecting to EVM source");
    let mut source = source::EvmSource::connect(&args.rpc, SWAP_TOPIC).await?;
    pipeline.run(&mut source, args.limit).await
}
