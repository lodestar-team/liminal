//! Terminal Kafka sink: emits newline-delimited JSON for each enriched swap to
//! stdout (so the host can pipe it to kcat). A Wasm-native Kafka client — or
//! `wasi:messaging` — would replace the stdout hop once one stabilises.
//!
//! Needs the `stdout` capability and a `KAFKA_CONFIG` env var, both granted by
//! the manifest. As a terminal node it returns no downstream output.

use liminal_sdk::node;
use uni_types::EnrichedSwap;

node!(|e: EnrichedSwap| -> Result<Vec<EnrichedSwap>, String> {
    // Broker list is injected for this node only.
    std::env::var("KAFKA_CONFIG").map_err(|_| "KAFKA_CONFIG not set".to_string())?;

    // The flattened EnrichedSwap is exactly the message shape for the
    // `uniswap.v3.swaps` topic.
    let json = serde_json::to_string(&e).map_err(|err| err.to_string())?;
    println!("{json}");

    Ok(vec![]) // terminal: nothing flows onward
});
