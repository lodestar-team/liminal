use anyhow::Result;
use tracing::{debug, info, warn};
use wasmtime::{component::Linker, Engine, Store};
use wasmtime::component::Component;
use alloy::rpc::types::eth::Log;

use crate::{
    bindings::{self, EvmLog, EnrichedSwap},
    make_state,
    source::EvmSource,
    HostState,
};

/// The assembled pipeline: four component instances wired in a DAG.
///
/// ```text
/// EVM source
///     └─► decoder          (no capabilities)
///              └─► enricher  (wasi:http → DeFiLlama only)
///                       ├─► sink-postgres  (env: DATABASE_URL)
///                       └─► sink-kafka     (env: KAFKA_BROKERS)
/// ```
///
/// One source connection.  One cursor.  The sinks see the same enriched
/// batch and run concurrently — no second gRPC connection, no second cursor.
pub struct Pipeline {
    pub decoder:    (Store<HostState>, bindings::decoder::DecoderWorld),
    pub enricher:   (Store<HostState>, bindings::enricher::EnricherWorld),
    pub pg_sink:    Component,
    pub kafka_sink: Component,
    pub engine:     Engine,
    pub database_url:  Option<String>,
    pub kafka_brokers: Option<String>,
}

impl Pipeline {
    pub async fn run(mut self, source: &mut EvmSource, limit: Option<u64>) -> Result<()> {
        let mut blocks_seen = 0u64;
        let mut last_block = 0u64;
        let mut batch: Vec<Log> = Vec::new();

        info!("pipeline running");

        while let Some(result) = source.next().await {
            let log = result?;
            let block_number = log.block_number.unwrap_or(0);

            if block_number != last_block && !batch.is_empty() {
                self.process_batch(&batch).await?;
                batch.clear();
                blocks_seen += 1;

                if limit.is_some_and(|lim| blocks_seen >= lim) {
                    info!(blocks_seen, "block limit reached, stopping");
                    break;
                }
            }

            last_block = block_number;
            batch.push(log);
        }

        if !batch.is_empty() {
            self.process_batch(&batch).await?;
        }

        info!("pipeline stopped");
        Ok(())
    }

    async fn process_batch(&mut self, logs: &[Log]) -> Result<()> {
        // -------------------------------------------------------------------
        // Stage 1: decoder — typed call, returns Option<Swap>.
        // -------------------------------------------------------------------
        let (store, world) = &mut self.decoder;
        let decode = world.liminal_pipeline_decode();

        let mut swaps = Vec::new();
        for log in logs {
            let evm_log = alloy_log_to_evm_log(log);
            if let Some(swap) = decode.call_decode_swap(store, evm_log).await? {
                swaps.push(swap);
            }
        }

        if swaps.is_empty() {
            return Ok(());
        }
        debug!(count = swaps.len(), "decoded swaps");

        // -------------------------------------------------------------------
        // Stage 2: enricher — typed call, returns Result<EnrichedSwap, String>.
        // -------------------------------------------------------------------
        let (store, world) = &mut self.enricher;
        let enrich = world.liminal_pipeline_enrich();

        let mut enriched: Vec<EnrichedSwap> = Vec::new();
        for swap in swaps {
            match enrich.call_enrich_swap(store, swap).await? {
                Ok(e)  => enriched.push(e),
                Err(e) => warn!("enrichment failed: {e}; skipping swap"),
            }
        }

        if enriched.is_empty() {
            return Ok(());
        }
        debug!(count = enriched.len(), "enriched swaps");

        // -------------------------------------------------------------------
        // Stage 3: fan-out — both sinks see the same batch.
        // Sinks are re-instantiated per batch (stateless writes); the source
        // cursor lives only in the pipeline runner, not in any sink.
        // -------------------------------------------------------------------
        let (pg_result, kafka_result) = tokio::join!(
            call_sink(
                &self.engine,
                &self.pg_sink,
                &enriched,
                "POSTGRES_CONFIG",
                self.database_url.as_deref(),
                "postgres",
            ),
            call_sink(
                &self.engine,
                &self.kafka_sink,
                &enriched,
                "KAFKA_CONFIG",
                self.kafka_brokers.as_deref(),
                "kafka",
            ),
        );

        if let Err(e) = pg_result    { warn!("postgres sink error: {e}"); }
        if let Err(e) = kafka_result { warn!("kafka sink error: {e}"); }

        Ok(())
    }
}

/// Instantiate a sink component, call `write-batch`, return the count.
///
/// Sinks are intentionally stateless: instantiated fresh per batch so that
/// a sink crash cannot corrupt pipeline cursor state.  Connection config is
/// injected as a WASI env var scoped to this instance only.
async fn call_sink(
    engine:     &Engine,
    component:  &Component,
    batch:      &[EnrichedSwap],
    env_key:    &str,
    env_val:    Option<&str>,
    name:       &str,
) -> Result<()> {
    let mut linker: Linker<HostState> = Linker::new(engine);
    wasmtime_wasi::add_to_linker_async(&mut linker)?;
    // Note: no wasmtime_wasi_http here — sinks have no HTTP capability.

    let mut wasi = wasmtime_wasi::WasiCtxBuilder::new().inherit_stderr();
    if let Some(val) = env_val {
        wasi = wasi.env(env_key, val);
    }
    let mut store = Store::new(engine, make_state(wasi.build()));

    let (sink_world, _) = bindings::sink::SinkWorld::instantiate_async(
        &mut store, component, &linker,
    ).await?;

    let count = sink_world
        .liminal_pipeline_sink()
        .call_write_batch(&mut store, batch.to_vec())
        .await?
        .map_err(|e| anyhow::anyhow!("{name} sink returned error: {e}"))?;

    info!(sink = name, count, "wrote batch");
    Ok(())
}

// ---------------------------------------------------------------------------
// Conversion: alloy Log → WIT EvmLog
// ---------------------------------------------------------------------------

fn alloy_log_to_evm_log(log: &Log) -> EvmLog {
    EvmLog {
        address:      format!("{}", log.address()),
        topics:       log.topics().iter().map(|t| format!("{t}")).collect(),
        data:         log.data().data.to_vec(),
        block_number: log.block_number.unwrap_or(0),
        tx_hash:      log.transaction_hash.map(|h| format!("{h}")).unwrap_or_default(),
        log_index:    log.log_index.unwrap_or(0) as u32,
    }
}
