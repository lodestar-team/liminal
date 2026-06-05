//! Uniswap v3 decoder node: raw EVM log → `Swap`, or nothing if it isn't one.

use alloy_sol_types::{sol, SolEvent};
use liminal_sdk::{node, EvmLog};
use uni_types::Swap as OutSwap;

const SWAP_SIG: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

// The ABI event type. `OutSwap` (above) is our wire type.
sol! {
    event Swap(
        address indexed sender,
        address indexed recipient,
        int256  amount0,
        int256  amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24   tick
    );
}

node!(|log: EvmLog| -> Result<Vec<OutSwap>, String> {
    if log.topics.first().map(String::as_str) != Some(SWAP_SIG) {
        return Ok(vec![]);
    }

    let topics: Vec<alloy_primitives::B256> = log
        .topics
        .iter()
        .filter_map(|t| t.trim_start_matches("0x").parse().ok())
        .collect();

    let decoded = match Swap::decode_raw_log(&topics, &log.data) {
        Ok(d) => d,
        Err(e) => return Err(format!("decode_raw_log: {e}")),
    };

    Ok(vec![OutSwap {
        pool: log.address.clone(),
        sender: decoded.sender.to_string(),
        recipient: decoded.recipient.to_string(),
        amount0: decoded.amount0.to_string(),
        amount1: decoded.amount1.to_string(),
        tick: i32::try_from(decoded.tick).unwrap_or(0),
        block_number: log.block_number,
        tx_hash: log.tx_hash,
        log_index: log.log_index,
    }])
});
