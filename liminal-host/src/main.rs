use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

mod bindings;
mod pipeline;
mod source;

/// Uniswap v3 Swap(address,address,int256,int256,uint160,uint128,int24)
const SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

#[derive(Parser)]
#[command(name = "liminal", about = "Liminal pipeline runner")]
struct Args {
    /// Ethereum WebSocket RPC URL
    #[arg(long, env = "ETH_RPC_URL")]
    rpc: String,

    /// Path to the decoder .wasm component
    #[arg(long, default_value = "examples/uni-v3-swaps/decoder.wasm")]
    decoder: String,

    /// Path to the price-enricher .wasm component
    #[arg(long, default_value = "examples/uni-v3-swaps/price-enricher.wasm")]
    enricher: String,

    /// Path to the postgres-sink .wasm component
    #[arg(long, default_value = "examples/uni-v3-swaps/sink-postgres.wasm")]
    sink_postgres: String,

    /// Path to the kafka-sink .wasm component
    #[arg(long, default_value = "examples/uni-v3-swaps/sink-kafka.wasm")]
    sink_kafka: String,

    /// DeFiLlama price oracle base URL (granted only to the enricher component)
    #[arg(long, default_value = "https://coins.llama.fi", env = "ORACLE_URL")]
    oracle_url: String,

    /// Postgres connection string (granted only to the postgres-sink component)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Kafka bootstrap servers (granted only to the kafka-sink component)
    #[arg(long, env = "KAFKA_BROKERS")]
    kafka_brokers: Option<String>,

    /// Process this many blocks then exit (omit to run indefinitely)
    #[arg(long)]
    limit: Option<u64>,
}

// ---------------------------------------------------------------------------
// Host state
//
// Every component instance owns its own Store<HostState>.  The struct
// includes both WasiCtx (basic WASI) and WasiHttpCtx (HTTP capability).
// Having the field doesn't grant the capability — the capability is granted
// or withheld at the LINKER level: only the enricher's linker has
// wasmtime_wasi_http::add_to_linker_async called on it.
// ---------------------------------------------------------------------------
pub struct HostState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: wasmtime_wasi::ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx { &mut self.wasi }
    fn table(&mut self) -> &mut wasmtime_wasi::ResourceTable { &mut self.table }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx { &mut self.http }
    fn table(&mut self) -> &mut wasmtime_wasi::ResourceTable { &mut self.table }
}

pub fn make_state(wasi: WasiCtx) -> HostState {
    HostState { wasi, http: WasiHttpCtx::new(), table: Default::default() }
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

    // -----------------------------------------------------------------------
    // Engine: component model + async.
    // -----------------------------------------------------------------------
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.async_support(true);
    let engine = Engine::new(&config)?;

    // -----------------------------------------------------------------------
    // Load component binaries.
    // -----------------------------------------------------------------------
    info!("loading components");
    let decoder_component = Component::from_file(&engine, &args.decoder)
        .with_context(|| format!("loading decoder from {}", args.decoder))?;
    let enricher_component = Component::from_file(&engine, &args.enricher)
        .with_context(|| format!("loading enricher from {}", args.enricher))?;
    let pg_component = Component::from_file(&engine, &args.sink_postgres)
        .with_context(|| format!("loading postgres sink from {}", args.sink_postgres))?;
    let kafka_component = Component::from_file(&engine, &args.sink_kafka)
        .with_context(|| format!("loading kafka sink from {}", args.sink_kafka))?;

    // -----------------------------------------------------------------------
    // Decoder — basic WASI only, no HTTP capability.
    // -----------------------------------------------------------------------
    let (decoder_store, decoder_bindings) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)?;
        let mut store = Store::new(&engine, make_state(WasiCtxBuilder::new().inherit_stderr().build()));
        let (bindings, _) = bindings::decoder::DecoderWorld::instantiate_async(
            &mut store, &decoder_component, &linker,
        ).await?;
        (store, bindings)
    };

    // -----------------------------------------------------------------------
    // Enricher — WASI + HTTP.  Oracle URL injected as env var so the
    // component can read it; the decoder and sinks never see it.
    // -----------------------------------------------------------------------
    let (enricher_store, enricher_bindings) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)?;
        wasmtime_wasi_http::add_to_linker_async(&mut linker)?; // HTTP capability: enricher only
        let wasi = WasiCtxBuilder::new()
            .inherit_stderr()
            .env("ORACLE_URL", &args.oracle_url)
            .build();
        let mut store = Store::new(&engine, make_state(wasi));
        let (bindings, _) = bindings::enricher::EnricherWorld::instantiate_async(
            &mut store, &enricher_component, &linker,
        ).await?;
        (store, bindings)
    };

    // -----------------------------------------------------------------------
    // Assemble and run the pipeline.
    // -----------------------------------------------------------------------
    let pipeline = pipeline::Pipeline {
        decoder: (decoder_store, decoder_bindings),
        enricher: (enricher_store, enricher_bindings),
        pg_sink: pg_component,
        kafka_sink: kafka_component,
        engine: engine.clone(),
        database_url: args.database_url,
        kafka_brokers: args.kafka_brokers,
    };

    info!(rpc = %args.rpc, "connecting to EVM source");
    let mut source = source::EvmSource::connect(&args.rpc, SWAP_TOPIC).await?;

    pipeline.run(&mut source, args.limit).await
}
