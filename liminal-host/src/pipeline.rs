use anyhow::Result;
use tracing::{debug, info, warn};
use wasmtime::{
    component::{Component, Instance, Val},
    Engine, Store,
};
use alloy::rpc::types::eth::Log;

use crate::{HostState, source::EvmSource};

/// The assembled pipeline: four component instances wired in a DAG.
///
/// ```text
/// EVM source
///     └─► decoder
///              └─► price-enricher   (HTTP → DeFiLlama; only this component)
///                       ├─► sink-postgres
///                       └─► sink-kafka
/// ```
pub struct Pipeline {
    pub decoder: (Store<HostState>, Instance),
    pub enricher: (Store<HostState>, Instance),
    pub pg_sink: Component,
    pub kafka_sink: Component,
    pub engine: Engine,
    pub oracle_url: String,
    pub database_url: Option<String>,
    pub kafka_brokers: Option<String>,
}

impl Pipeline {
    pub async fn run(mut self, source: &mut EvmSource, limit: Option<u64>) -> Result<()> {
        let mut blocks_seen = 0u64;
        let mut last_block = 0u64;
        let mut batch: Vec<alloy::rpc::types::eth::Log> = Vec::new();

        info!("pipeline running");

        while let Some(log_result) = source.next().await {
            let log = log_result?;
            let block_number = log.block_number.unwrap_or(0);

            // Flush the batch at block boundaries.
            if block_number != last_block && !batch.is_empty() {
                self.process_batch(&batch).await?;
                batch.clear();
                blocks_seen += 1;

                if let Some(lim) = limit {
                    if blocks_seen >= lim {
                        info!(blocks_seen, "block limit reached, stopping");
                        break;
                    }
                }
            }

            last_block = block_number;
            batch.push(log);
        }

        // Flush any remaining logs.
        if !batch.is_empty() {
            self.process_batch(&batch).await?;
        }

        info!("pipeline stopped");
        Ok(())
    }

    async fn process_batch(&mut self, logs: &[Log]) -> Result<()> {
        // -----------------------------------------------------------------------
        // Stage 1: decoder — one call per log, returns Option<swap>.
        // -----------------------------------------------------------------------
        let (decoder_store, decoder_instance) = &mut self.decoder;

        let decode_fn = decoder_instance
            .get_func(&mut *decoder_store, "liminal:pipeline/decode#decode-swap")
            .expect("decoder component must export liminal:pipeline/decode#decode-swap");

        let mut swaps = Vec::new();
        for log in logs {
            let evm_log = log_to_wit(log);
            let mut results = vec![Val::Bool(false)]; // placeholder; real type from bindgen
            decode_fn.call_async(&mut *decoder_store, &[evm_log], &mut results).await?;
            decode_fn.post_return_async(&mut *decoder_store).await?;

            if let Some(swap) = extract_swap_option(&results[0]) {
                swaps.push(swap);
            }
        }

        if swaps.is_empty() {
            return Ok(());
        }

        debug!(count = swaps.len(), "decoded swaps");

        // -----------------------------------------------------------------------
        // Stage 2: enricher — one call per swap.
        // -----------------------------------------------------------------------
        let (enricher_store, enricher_instance) = &mut self.enricher;

        let enrich_fn = enricher_instance
            .get_func(&mut *enricher_store, "liminal:pipeline/enrich#enrich-swap")
            .expect("enricher component must export liminal:pipeline/enrich#enrich-swap");

        let mut enriched = Vec::new();
        for swap in swaps {
            let mut results = vec![Val::Bool(false)];
            enrich_fn.call_async(&mut *enricher_store, &[swap], &mut results).await?;
            enrich_fn.post_return_async(&mut *enricher_store).await?;

            match extract_result(&results[0]) {
                Ok(v) => enriched.push(v),
                Err(e) => warn!("enrichment failed: {e}; skipping swap"),
            }
        }

        if enriched.is_empty() {
            return Ok(());
        }

        debug!(count = enriched.len(), "enriched swaps");

        // -----------------------------------------------------------------------
        // Stage 3: fan-out — postgres and kafka sinks run concurrently from the
        // same enriched batch.  One source connection, one cursor, two sinks.
        // -----------------------------------------------------------------------
        let pg_result = self.call_sink(&self.pg_sink.clone(), &enriched, &self.database_url.clone(), "postgres").await;
        let kafka_result = self.call_sink(&self.kafka_sink.clone(), &enriched, &self.kafka_brokers.clone(), "kafka").await;

        if let Err(e) = pg_result { warn!("postgres sink error: {e}"); }
        if let Err(e) = kafka_result { warn!("kafka sink error: {e}"); }

        Ok(())
    }

    async fn call_sink(
        &self,
        component: &Component,
        batch: &[Val],
        config: &Option<String>,
        name: &str,
    ) -> Result<()> {
        use wasmtime::component::Linker;
        use wasmtime_wasi::WasiCtxBuilder;

        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)?;

        // Sink components receive their connection config via WASI env vars,
        // scoped to this instance only — not visible to any other component.
        let mut wasi_builder = WasiCtxBuilder::new().inherit_stderr();
        if let Some(cfg) = config {
            wasi_builder = wasi_builder.env(
                &format!("{}_CONFIG", name.to_uppercase()),
                cfg,
            );
        }

        let state = HostState {
            wasi: wasi_builder.build(),
            table: Default::default(),
        };
        let mut store = Store::new(&self.engine, state);
        let instance = linker.instantiate_async(&mut store, component).await?;

        let write_fn = instance
            .get_func(&mut store, "liminal:pipeline/sink#write-batch")
            .expect("sink component must export liminal:pipeline/sink#write-batch");

        let batch_val = Val::List(batch.to_vec());
        let mut results = vec![Val::Bool(false)];
        write_fn.call_async(&mut store, &[batch_val], &mut results).await?;
        write_fn.post_return_async(&mut store).await?;

        match extract_result(&results[0]) {
            Ok(count) => {
                info!(sink = name, count = ?count, "wrote batch");
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("{name} sink returned error: {e}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers: convert between alloy types and WIT Val representations.
// These will be replaced by typed bindgen! bindings once we wire up the
// full component model macro expansion.
// ---------------------------------------------------------------------------

fn log_to_wit(log: &Log) -> Val {
    Val::Record(vec![
        ("address".into(), Val::String(format!("{:?}", log.address()))),
        (
            "topics".into(),
            Val::List(
                log.topics()
                    .iter()
                    .map(|t| Val::String(format!("{t:?}")))
                    .collect(),
            ),
        ),
        ("data".into(), Val::List(log.data().data.iter().map(|b| Val::U8(*b)).collect())),
        ("block-number".into(), Val::U64(log.block_number.unwrap_or(0))),
        ("tx-hash".into(), Val::String(log.transaction_hash.map(|h| format!("{h:?}")).unwrap_or_default())),
        ("log-index".into(), Val::U32(log.log_index.unwrap_or(0) as u32)),
    ])
}

fn extract_swap_option(val: &Val) -> Option<Val> {
    match val {
        Val::Option(Some(v)) => Some(*v.clone()),
        _ => None,
    }
}

fn extract_result(val: &Val) -> Result<Val, String> {
    match val {
        Val::Result(Ok(Some(v))) => Ok(*v.clone()),
        Val::Result(Err(Some(e))) => {
            if let Val::String(s) = e.as_ref() { Err(s.clone()) } else { Err("unknown error".into()) }
        }
        _ => Err("unexpected result shape".into()),
    }
}
