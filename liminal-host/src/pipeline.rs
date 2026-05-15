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

pub struct Pipeline {
    pub decoder:       (Store<HostState>, bindings::decoder::DecoderWorld),
    pub enricher:      (Store<HostState>, bindings::enricher::EnricherWorld),
    pub pg_sink:       Component,
    pub kafka_sink:    Component,
    pub engine:        Engine,
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
        // Stage 1: decoder — sync component call.
        let (store, world) = &mut self.decoder;
        let decode = world.liminal_pipeline_decode();

        let mut swaps = Vec::new();
        for log in logs {
            let evm_log = alloy_log_to_evm_log(log);
            if let Some(swap) = decode.call_decode_swap(&mut *store, &evm_log)? {
                swaps.push(swap);
            }
        }
        if swaps.is_empty() { return Ok(()); }
        debug!(count = swaps.len(), "decoded swaps");

        // Stage 2: enricher — sync component call.
        let (store, world) = &mut self.enricher;
        let enrich = world.liminal_pipeline_enrich();

        let mut enriched: Vec<EnrichedSwap> = Vec::new();
        for swap in swaps {
            // Convert from decoder's Swap type to enricher's Swap type.
            let enricher_swap = decoder_swap_to_enricher(swap);
            match enrich.call_enrich_swap(&mut *store, &enricher_swap)? {
                Ok(e)  => enriched.push(enricher_to_canonical(e)),
                Err(e) => warn!("enrichment failed: {e}; skipping swap"),
            }
        }
        if enriched.is_empty() { return Ok(()); }
        debug!(count = enriched.len(), "enriched swaps");

        // Stage 3: fan-out — both sinks receive the same batch concurrently.
        let (pg_result, kafka_result) = tokio::join!(
            call_sink(&self.engine, &self.pg_sink, &enriched,
                      "POSTGRES_CONFIG", self.database_url.as_deref(), "postgres"),
            call_sink(&self.engine, &self.kafka_sink, &enriched,
                      "KAFKA_CONFIG", self.kafka_brokers.as_deref(), "kafka"),
        );
        if let Err(e) = pg_result    { warn!("postgres sink error: {e}"); }
        if let Err(e) = kafka_result { warn!("kafka sink error: {e}"); }

        Ok(())
    }
}

async fn call_sink(
    engine:    &Engine,
    component: &Component,
    batch:     &[EnrichedSwap],
    env_key:   &str,
    env_val:   Option<&str>,
    name:      &str,
) -> Result<()> {
    let mut linker: Linker<HostState> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut builder = wasmtime_wasi::WasiCtxBuilder::new();
    builder.inherit_stderr();
    if let Some(val) = env_val { builder.env(env_key, val); }
    let mut store = Store::new(engine, make_state(builder.build()));

    let sink_world = bindings::sink::SinkWorld::instantiate_async(
        &mut store, component, &linker,
    ).await?;

    // Convert canonical EnrichedSwap to sink world's type.
    let sink_batch: Vec<bindings::sink::liminal::pipeline::types::EnrichedSwap> =
        batch.iter().cloned().map(canonical_to_sink).collect();

    let count = sink_world
        .liminal_pipeline_sink()
        .call_write_batch(&mut store, &sink_batch)?
        .map_err(|e| anyhow::anyhow!("{name} sink returned error: {e}"))?;

    info!(sink = name, count, "wrote batch");
    Ok(())
}

// ---------------------------------------------------------------------------
// Type conversions between the three bindgen! generated worlds.
// Each world re-generates the WIT types; these trivial conversions bridge them
// until we configure `with` sharing properly.
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

fn decoder_swap_to_enricher(
    s: bindings::decoder::liminal::pipeline::types::Swap,
) -> bindings::enricher::liminal::pipeline::types::Swap {
    bindings::enricher::liminal::pipeline::types::Swap {
        pool: s.pool, sender: s.sender, recipient: s.recipient,
        amount0: s.amount0, amount1: s.amount1, tick: s.tick,
        block_number: s.block_number, tx_hash: s.tx_hash, log_index: s.log_index,
    }
}

fn enricher_to_canonical(
    e: bindings::enricher::liminal::pipeline::types::EnrichedSwap,
) -> EnrichedSwap {
    EnrichedSwap {
        swap: bindings::decoder::liminal::pipeline::types::Swap {
            pool: e.swap.pool, sender: e.swap.sender, recipient: e.swap.recipient,
            amount0: e.swap.amount0, amount1: e.swap.amount1, tick: e.swap.tick,
            block_number: e.swap.block_number, tx_hash: e.swap.tx_hash, log_index: e.swap.log_index,
        },
        token0_symbol: e.token0_symbol, token1_symbol: e.token1_symbol,
        token0_usd_price: e.token0_usd_price, token1_usd_price: e.token1_usd_price,
        amount_usd: e.amount_usd,
    }
}

fn canonical_to_sink(
    e: EnrichedSwap,
) -> bindings::sink::liminal::pipeline::types::EnrichedSwap {
    bindings::sink::liminal::pipeline::types::EnrichedSwap {
        swap: bindings::sink::liminal::pipeline::types::Swap {
            pool: e.swap.pool, sender: e.swap.sender, recipient: e.swap.recipient,
            amount0: e.swap.amount0, amount1: e.swap.amount1, tick: e.swap.tick,
            block_number: e.swap.block_number, tx_hash: e.swap.tx_hash, log_index: e.swap.log_index,
        },
        token0_symbol: e.token0_symbol, token1_symbol: e.token1_symbol,
        token0_usd_price: e.token0_usd_price, token1_usd_price: e.token1_usd_price,
        amount_usd: e.amount_usd,
    }
}
