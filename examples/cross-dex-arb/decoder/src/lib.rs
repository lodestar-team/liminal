//! Cross-DEX decoder node: raw EVM log → `NormalizedSwap`, or nothing.
//!
//! Handles two Swap event ABIs (Uniswap v3 and Balancer v2) that happen to
//! share a name but differ entirely on the wire, and normalises both into one
//! in/out shape the rest of the pipeline understands.

use alloy_sol_types::SolEvent;
use arb_types::{NormalizedSwap, Protocol};
use liminal_sdk::{node, EvmLog};

// ---------------------------------------------------------------------------
// ABI definitions — separate modules so each Swap gets its own keccak sig hash.
// ---------------------------------------------------------------------------

mod uni {
    alloy_sol_types::sol! {
        // topic0 = 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
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
}

mod bal {
    alloy_sol_types::sol! {
        // topic0 = 0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b
        event Swap(
            bytes32 indexed poolId,
            address indexed tokenIn,
            address indexed tokenOut,
            uint256 amountIn,
            uint256 amountOut
        );
    }
}

// Known Uniswap v3 pools — (pool, token0, token1). The event carries only
// amounts, so we resolve token addresses from this table.
const KNOWN_UNI_POOLS: &[(&str, &str, &str)] = &[
    // USDC/WETH 0.05%
    (
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    // USDC/WETH 0.3%
    (
        "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    // WBTC/WETH 0.3%
    (
        "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    // DAI/WETH 0.3%
    (
        "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
        "0x6b175474e89094c44da98b954eedeac495271d0f",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
];

const UNI_SWAP_SIG: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const BAL_SWAP_SIG: &str = "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b";

node!(|log: EvmLog| -> Result<Vec<NormalizedSwap>, String> {
    let swap = match log.topics.first().map(String::as_str) {
        Some(UNI_SWAP_SIG) => decode_uniswap_v3(&log),
        Some(BAL_SWAP_SIG) => decode_balancer_v2(&log),
        _ => None,
    };
    // Ok([]) = filtered (not a swap we track); Ok([swap]) = emit downstream.
    Ok(swap.into_iter().collect())
});

fn decode_uniswap_v3(log: &EvmLog) -> Option<NormalizedSwap> {
    let pool = log.address.to_lowercase();
    let (token0, token1) = KNOWN_UNI_POOLS
        .iter()
        .find(|(addr, ..)| *addr == pool.as_str())
        .map(|(_, t0, t1)| (*t0, *t1))?;

    let topics = parse_topics(log);
    let d = uni::Swap::decode_raw_log(&topics, &log.data).ok()?;

    // Positive amount0 → user sold token0 → token0 is "in", token1 is "out".
    let (token_in, token_out, amount_in, amount_out) =
        if d.amount0 > alloy_primitives::I256::ZERO {
            (token0, token1, d.amount0.to_string(), (-d.amount1).to_string())
        } else {
            (token1, token0, d.amount1.to_string(), (-d.amount0).to_string())
        };

    Some(NormalizedSwap {
        protocol: Protocol::UniswapV3,
        pool: log.address.clone(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in,
        amount_out,
        block_number: log.block_number,
        tx_hash: log.tx_hash.clone(),
        log_index: log.log_index,
    })
}

fn decode_balancer_v2(log: &EvmLog) -> Option<NormalizedSwap> {
    let topics = parse_topics(log);
    let d = bal::Swap::decode_raw_log(&topics, &log.data).ok()?;

    Some(NormalizedSwap {
        protocol: Protocol::BalancerV2,
        pool: format!("{}", d.poolId),
        token_in: d.tokenIn.to_string(),
        token_out: d.tokenOut.to_string(),
        amount_in: d.amountIn.to_string(),
        amount_out: d.amountOut.to_string(),
        block_number: log.block_number,
        tx_hash: log.tx_hash.clone(),
        log_index: log.log_index,
    })
}

fn parse_topics(log: &EvmLog) -> Vec<alloy_primitives::B256> {
    log.topics
        .iter()
        .filter_map(|t| t.trim_start_matches("0x").parse().ok())
        .collect()
}
