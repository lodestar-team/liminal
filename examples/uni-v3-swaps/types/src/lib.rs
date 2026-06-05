//! Wire types flowing along the uni-v3-swaps pipeline's edges.
//!
//! decoder → [Swap] → enricher → [EnrichedSwap] → {postgres, kafka}.

use serde::{Deserialize, Serialize};

/// A decoded Uniswap v3 Swap. Amounts are decimal strings (they're int256).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub pool: String,
    pub sender: String,
    pub recipient: String,
    pub amount0: String,
    pub amount1: String,
    pub tick: i32,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u32,
}

/// A Swap enriched with token symbols and USD pricing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedSwap {
    #[serde(flatten)]
    pub swap: Swap,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub token0_usd_price: f64,
    pub token1_usd_price: f64,
    /// Best-effort USD value of the swap; 0 if pricing unavailable.
    pub amount_usd: f64,
}
