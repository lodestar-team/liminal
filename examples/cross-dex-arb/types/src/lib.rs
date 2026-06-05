//! Wire types flowing along the cross-dex-arb pipeline's edges.
//!
//! These are plain serde structs — the host never sees them, only the bytes.
//! decoder → [NormalizedSwap] → enricher → [EnrichedArbSwap] → sink → dashboard.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    UniswapV3,
    BalancerV2,
}

/// A swap normalised across protocols into a common in/out shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedSwap {
    pub protocol: Protocol,
    pub pool: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: String,
    pub amount_out: String,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u32,
}

/// A normalised swap enriched with token symbols, USD prices, and decimals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedArbSwap {
    #[serde(flatten)]
    pub swap: NormalizedSwap,
    pub token_in_symbol: String,
    pub token_out_symbol: String,
    pub token_in_usd_price: f64,
    pub token_out_usd_price: f64,
    pub token_in_decimals: u8,
    pub token_out_decimals: u8,
}
