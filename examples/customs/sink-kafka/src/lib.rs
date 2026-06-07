//! Kafka sink — the second leg of the fan-out off a single cursor.
//!
//! At-least-once after the SoR commit (see RFC §3.6). Writes a `KAFKA ` line to
//! stdout; a real build would publish to the `transfers` topic.

use customs_types::PricedTransfer;
use liminal_sdk::node;

node!(|p: PricedTransfer| -> Result<Vec<PricedTransfer>, String> {
    let line = serde_json::to_string(&p).map_err(|e| e.to_string())?;
    println!("KAFKA {line}");
    Ok(vec![]) // terminal
});
