use anyhow::Result;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use wasmtime::{component::Linker, Engine, Store};
use wasmtime::component::Component;
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use alloy::rpc::types::eth::Log;

use crate::{
    arb_bindings::{self, EnrichedArbSwap, NormalizedSwap},
    make_state,
    source::EvmSource,
    HostState,
};

pub struct ArbPipeline {
    pub decoder:      (Store<HostState>, arb_bindings::decoder::ArbDecoderWorld),
    pub enricher:     (Store<HostState>, arb_bindings::enricher::ArbEnricherWorld),
    pub json_sink:    Component,
    pub engine:       Engine,
    pub oracle_url:   String,
    pub sse_tx:       broadcast::Sender<String>,
}

impl ArbPipeline {
    pub async fn run(mut self, source: &mut EvmSource, limit: Option<u64>) -> Result<()> {
        let mut blocks_seen = 0u64;
        let mut last_block  = 0u64;
        let mut batch: Vec<Log> = Vec::new();

        info!("arb pipeline running");

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

        info!("arb pipeline stopped");
        Ok(())
    }

    async fn process_batch(&mut self, logs: &[Log]) -> Result<()> {
        // Stage 1 — decode.
        let (store, world) = &mut self.decoder;
        let decode = world.liminal_pipeline_arb_decode();

        let mut swaps: Vec<NormalizedSwap> = Vec::new();
        for log in logs {
            let evm_log = alloy_log_to_evm_log(log);
            if let Some(s) = decode.call_decode_swap(&mut *store, &evm_log).await? {
                swaps.push(s);
            }
        }
        if swaps.is_empty() { return Ok(()); }
        debug!(count = swaps.len(), "decoded arb swaps");

        // Stage 2 — enrich.
        let (store, world) = &mut self.enricher;
        let enrich = world.liminal_pipeline_arb_enrich();

        let mut enriched: Vec<EnrichedArbSwap> = Vec::new();
        for swap in swaps {
            let enricher_swap = decoder_to_enricher_swap(swap);
            match enrich.call_enrich_swap(&mut *store, &enricher_swap).await? {
                Ok(e)  => enriched.push(enricher_to_canonical(e)),
                Err(e) => warn!("arb enrichment failed: {e}; skipping"),
            }
        }
        if enriched.is_empty() { return Ok(()); }
        debug!(count = enriched.len(), "enriched arb swaps");

        // Stage 3 — JSON sink (stdout captured → SSE broadcast).
        if let Err(e) = self.call_json_sink(&enriched).await {
            warn!("json sink error: {e}");
        }

        Ok(())
    }

    async fn call_json_sink(&self, batch: &[EnrichedArbSwap]) -> Result<()> {
        let pipe = MemoryOutputPipe::new(4 * 1024 * 1024); // 4 MB

        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

        let mut builder = wasmtime_wasi::WasiCtxBuilder::new();
        builder.stdout(pipe.clone()).inherit_stderr();
        builder.env("ORACLE_URL", &self.oracle_url);
        let mut store = Store::new(&self.engine, make_state(builder.build()));

        let sink_world = arb_bindings::sink::ArbSinkWorld::instantiate_async(
            &mut store, &self.json_sink, &linker,
        ).await?;

        let sink_batch: Vec<_> = batch.iter().cloned().map(canonical_to_sink).collect();
        let count = sink_world
            .liminal_pipeline_arb_sink()
            .call_write_batch(&mut store, &sink_batch)
            .await?
            .map_err(|e| anyhow::anyhow!("json sink returned error: {e}"))?;

        info!(sink = "json", count, "wrote arb batch");

        // Broadcast each JSON line via SSE.
        let output = pipe.contents();
        if let Ok(text) = std::str::from_utf8(&output) {
            for line in text.lines() {
                if !line.is_empty() {
                    let _ = self.sse_tx.send(line.to_string());
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Alloy → WIT conversion
// ---------------------------------------------------------------------------

fn alloy_log_to_evm_log(log: &Log) -> arb_bindings::decoder::liminal::pipeline::types::EvmLog {
    arb_bindings::decoder::liminal::pipeline::types::EvmLog {
        address:      format!("{}", log.address()),
        topics:       log.topics().iter().map(|t| format!("{t}")).collect(),
        data:         log.data().data.to_vec(),
        block_number: log.block_number.unwrap_or(0),
        tx_hash:      log.transaction_hash.map(|h| format!("{h}")).unwrap_or_default(),
        log_index:    log.log_index.unwrap_or(0) as u32,
    }
}

// ---------------------------------------------------------------------------
// Type conversions between the three independently generated worlds.
// ---------------------------------------------------------------------------

fn decoder_to_enricher_swap(
    s: arb_bindings::decoder::liminal::pipeline::arb_types::NormalizedSwap,
) -> arb_bindings::enricher::liminal::pipeline::arb_types::NormalizedSwap {
    use arb_bindings::enricher::liminal::pipeline::arb_types::Protocol as EP;
    use arb_bindings::decoder::liminal::pipeline::arb_types::Protocol as DP;
    arb_bindings::enricher::liminal::pipeline::arb_types::NormalizedSwap {
        protocol: match s.protocol { DP::UniswapV3 => EP::UniswapV3, DP::BalancerV2 => EP::BalancerV2 },
        pool: s.pool, token_in: s.token_in, token_out: s.token_out,
        amount_in: s.amount_in, amount_out: s.amount_out,
        block_number: s.block_number, tx_hash: s.tx_hash, log_index: s.log_index,
    }
}

fn enricher_to_canonical(
    e: arb_bindings::enricher::liminal::pipeline::arb_types::EnrichedArbSwap,
) -> EnrichedArbSwap {
    use arb_bindings::decoder::liminal::pipeline::arb_types::Protocol as DP;
    use arb_bindings::enricher::liminal::pipeline::arb_types::Protocol as EP;
    EnrichedArbSwap {
        swap: arb_bindings::decoder::liminal::pipeline::arb_types::NormalizedSwap {
            protocol: match e.swap.protocol { EP::UniswapV3 => DP::UniswapV3, EP::BalancerV2 => DP::BalancerV2 },
            pool: e.swap.pool, token_in: e.swap.token_in, token_out: e.swap.token_out,
            amount_in: e.swap.amount_in, amount_out: e.swap.amount_out,
            block_number: e.swap.block_number, tx_hash: e.swap.tx_hash, log_index: e.swap.log_index,
        },
        token_in_symbol: e.token_in_symbol, token_out_symbol: e.token_out_symbol,
        token_in_usd_price: e.token_in_usd_price, token_out_usd_price: e.token_out_usd_price,
        token_in_decimals: e.token_in_decimals, token_out_decimals: e.token_out_decimals,
    }
}

fn canonical_to_sink(
    e: EnrichedArbSwap,
) -> arb_bindings::sink::liminal::pipeline::arb_types::EnrichedArbSwap {
    use arb_bindings::sink::liminal::pipeline::arb_types::Protocol as SP;
    use arb_bindings::decoder::liminal::pipeline::arb_types::Protocol as DP;
    arb_bindings::sink::liminal::pipeline::arb_types::EnrichedArbSwap {
        swap: arb_bindings::sink::liminal::pipeline::arb_types::NormalizedSwap {
            protocol: match e.swap.protocol { DP::UniswapV3 => SP::UniswapV3, DP::BalancerV2 => SP::BalancerV2 },
            pool: e.swap.pool, token_in: e.swap.token_in, token_out: e.swap.token_out,
            amount_in: e.swap.amount_in, amount_out: e.swap.amount_out,
            block_number: e.swap.block_number, tx_hash: e.swap.tx_hash, log_index: e.swap.log_index,
        },
        token_in_symbol: e.token_in_symbol, token_out_symbol: e.token_out_symbol,
        token_in_usd_price: e.token_in_usd_price, token_out_usd_price: e.token_out_usd_price,
        token_in_decimals: e.token_in_decimals, token_out_decimals: e.token_out_decimals,
    }
}
