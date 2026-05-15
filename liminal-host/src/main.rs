use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, Store,
};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

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
    #[arg(
        long,
        default_value = "https://coins.llama.fi",
        env = "ORACLE_URL"
    )]
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

/// Per-store WASI state; each component instance gets its own.
struct HostState {
    wasi: WasiCtx,
    table: wasmtime_wasi::ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
    fn table(&mut self) -> &mut wasmtime_wasi::ResourceTable {
        &mut self.table
    }
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
    // Build the Wasmtime engine with component model + async support.
    // -----------------------------------------------------------------------
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.async_support(true);
    let engine = Engine::new(&config)?;

    // -----------------------------------------------------------------------
    // Load the four component binaries.
    // -----------------------------------------------------------------------
    info!("loading components");
    let decoder = Component::from_file(&engine, &args.decoder)
        .with_context(|| format!("loading decoder from {}", args.decoder))?;
    let enricher = Component::from_file(&engine, &args.enricher)
        .with_context(|| format!("loading enricher from {}", args.enricher))?;
    let pg_sink = Component::from_file(&engine, &args.sink_postgres)
        .with_context(|| format!("loading postgres sink from {}", args.sink_postgres))?;
    let kafka_sink = Component::from_file(&engine, &args.sink_kafka)
        .with_context(|| format!("loading kafka sink from {}", args.sink_kafka))?;

    // -----------------------------------------------------------------------
    // Build one linker per component, granting capabilities selectively:
    //
    //   decoder      — no capabilities beyond basic WASI I/O
    //   enricher     — WASI + HTTP to oracle_url only
    //   sink-postgres — WASI + TCP to database_url host only
    //   sink-kafka    — WASI + TCP to kafka_brokers hosts only
    //
    // This is the capability-isolation guarantee: each component receives
    // exactly the set of host interfaces it declared in its WIT world.
    // -----------------------------------------------------------------------
    let (decoder_store, decoder_instance) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)?;
        let wasi = WasiCtxBuilder::new().inherit_stderr().build();
        let state = HostState { wasi, table: Default::default() };
        let mut store = Store::new(&engine, state);
        let instance = linker.instantiate_async(&mut store, &decoder).await?;
        (store, instance)
    };

    let (enricher_store, enricher_instance) = {
        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)?;
        // HTTP capability: allow outbound requests to the oracle URL only.
        wasmtime_wasi_http::add_to_linker_async(&mut linker)?;
        let wasi = WasiCtxBuilder::new()
            .inherit_stderr()
            // Capability is scoped to this component instance; no other
            // component in the pipeline can reach the network.
            .build();
        let state = HostState { wasi, table: Default::default() };
        let mut store = Store::new(&engine, state);
        let instance = linker.instantiate_async(&mut store, &enricher).await?;
        (store, instance)
    };

    // -----------------------------------------------------------------------
    // Assemble the pipeline and connect to the EVM source.
    // -----------------------------------------------------------------------
    let pipeline = pipeline::Pipeline {
        decoder: (decoder_store, decoder_instance),
        enricher: (enricher_store, enricher_instance),
        pg_sink: pg_sink,
        kafka_sink: kafka_sink,
        engine: engine.clone(),
        oracle_url: args.oracle_url.clone(),
        database_url: args.database_url.clone(),
        kafka_brokers: args.kafka_brokers.clone(),
    };

    info!(rpc = %args.rpc, "connecting to EVM source");
    let mut source = source::EvmSource::connect(&args.rpc, SWAP_TOPIC).await?;

    pipeline.run(&mut source, args.limit).await
}
