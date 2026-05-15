wit_bindgen::generate!({
    world: "decoder-world",
    path: "../../../wit",
});

use alloy_primitives::{Address, I256, I32, U128, U160};
use alloy_sol_types::{sol, SolEvent};
use exports::liminal::pipeline::decode::Guest;
use liminal::pipeline::types::{EvmLog, Swap};

/// Uniswap v3 Pool Swap event ABI.
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

struct Decoder;

impl Guest for Decoder {
    fn decode_swap(log: EvmLog) -> Option<Swap> {
        // topic[0] must be the Swap event signature.
        let sig = log.topics.first()?;
        if sig != "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" {
            return None;
        }

        // Rebuild the alloy Log for ABI decoding.
        let topics: Vec<alloy_primitives::B256> = log
            .topics
            .iter()
            .filter_map(|t| t.trim_start_matches("0x").parse().ok())
            .collect();

        let decoded = SwapEvent::decode_raw_log(&topics, &log.data, false).ok()?;

        Some(Swap {
            pool: log.address.clone(),
            sender: format!("{:?}", decoded.sender),
            recipient: format!("{:?}", decoded.recipient),
            amount0: decoded.amount0.to_string(),
            amount1: decoded.amount1.to_string(),
            tick: decoded.tick,
            block_number: log.block_number,
            tx_hash: log.tx_hash,
            log_index: log.log_index,
        })
    }
}

export!(Decoder);
