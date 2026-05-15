wit_bindgen::generate!({
    world: "decoder-world",
    path: "../../../wit",
});

use alloy_sol_types::{sol, SolEvent};
use exports::liminal::pipeline::decode::Guest;
use liminal::pipeline::types::{EvmLog, Swap as WitSwap};

// Rename the ABI event to avoid colliding with the WIT `Swap` type.
sol! {
    event UniswapV3Swap(
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
    fn decode_swap(log: EvmLog) -> Option<WitSwap> {
        // topic[0] must be the Uniswap v3 Swap event signature.
        let sig = log.topics.first()?;
        if sig != "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" {
            return None;
        }

        let topics: Vec<alloy_primitives::B256> = log
            .topics
            .iter()
            .filter_map(|t| t.trim_start_matches("0x").parse().ok())
            .collect();

        // validate: false — we already checked topic[0] manually above.
        let decoded = UniswapV3Swap::decode_raw_log(&topics, &log.data).ok()?;

        Some(WitSwap {
            pool:         log.address.clone(),
            sender:       decoded.sender.to_string(),
            recipient:    decoded.recipient.to_string(),
            amount0:      decoded.amount0.to_string(),
            amount1:      decoded.amount1.to_string(),
            tick:         i32::try_from(decoded.tick).unwrap_or(0),
            block_number: log.block_number,
            tx_hash:      log.tx_hash,
            log_index:    log.log_index,
        })
    }
}

export!(Decoder);
