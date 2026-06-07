//! Decoder node: raw EVM log → ERC-20 `Transfer`, or nothing.
//!
//! Zero capabilities. Pure compute — it cannot call out, by construction.

use customs_types::Transfer;
use liminal_sdk::{node, EvmLog};

// keccak256("Transfer(address,address,uint256)")
const TRANSFER_SIG: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

node!(|log: EvmLog| -> Result<Vec<Transfer>, String> {
    // topic0 must be Transfer, and we need from/to in topics[1]/[2].
    if log.topics.first().map(String::as_str) != Some(TRANSFER_SIG) || log.topics.len() < 3 {
        return Ok(vec![]);
    }

    let value = alloy_primitives::U256::from_be_slice(&log.data).to_string();

    Ok(vec![Transfer {
        block_number: log.block_number,
        log_index: log.log_index,
        tx_hash: log.tx_hash,
        token: log.address.to_lowercase(),
        from: topic_to_address(&log.topics[1]),
        to: topic_to_address(&log.topics[2]),
        value,
    }])
});

/// An indexed address topic is a 32-byte word; the address is the low 20 bytes.
fn topic_to_address(topic: &str) -> String {
    let hex = topic.trim_start_matches("0x");
    let addr = hex.get(hex.len().saturating_sub(40)..).unwrap_or(hex);
    format!("0x{}", addr.to_lowercase())
}
