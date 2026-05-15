wit_bindgen::generate!({
    world: "sink-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::sink::Guest;
use liminal::pipeline::types::EnrichedSwap;
use serde::Serialize;

/// Kafka message shape published to the `uniswap.v3.swaps` topic.
#[derive(Serialize)]
struct SwapMessage<'a> {
    block_number: u64,
    tx_hash: &'a str,
    log_index: u32,
    pool: &'a str,
    sender: &'a str,
    recipient: &'a str,
    amount0: &'a str,
    amount1: &'a str,
    tick: i32,
    token0_symbol: &'a str,
    token1_symbol: &'a str,
    token0_usd_price: f64,
    token1_usd_price: f64,
    amount_usd: f64,
}

struct KafkaSink;

impl Guest for KafkaSink {
    fn write_batch(swaps: Vec<EnrichedSwap>) -> Result<u32, String> {
        // Broker list is injected via KAFKA_CONFIG env var by the host,
        // scoped to this component instance only.
        let _brokers = std::env::var("KAFKA_CONFIG")
            .map_err(|_| "KAFKA_CONFIG not set".to_string())?;

        // TODO: use a Wasm-compatible Kafka client once one stabilises
        // (wasi:messaging is the candidate interface).  For the PoC, emit
        // newline-delimited JSON to stdout so the host can pipe to kcat.
        for swap in &swaps {
            let msg = SwapMessage {
                block_number: swap.swap.block_number,
                tx_hash: &swap.swap.tx_hash,
                log_index: swap.swap.log_index,
                pool: &swap.swap.pool,
                sender: &swap.swap.sender,
                recipient: &swap.swap.recipient,
                amount0: &swap.swap.amount0,
                amount1: &swap.swap.amount1,
                tick: swap.swap.tick,
                token0_symbol: &swap.token0_symbol,
                token1_symbol: &swap.token1_symbol,
                token0_usd_price: swap.token0_usd_price,
                token1_usd_price: swap.token1_usd_price,
                amount_usd: swap.amount_usd,
            };

            let json = serde_json::to_string(&msg)
                .map_err(|e| e.to_string())?;
            println!("{json}");
        }

        Ok(swaps.len() as u32)
    }
}

export!(KafkaSink);
