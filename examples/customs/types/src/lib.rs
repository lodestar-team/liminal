//! Wire types flowing along the Customs pipeline's edges.
//!
//! decoder → [Transfer] → screener → [Verdict] ──┬─ cleared ─→ enricher → [PricedTransfer] → sinks
//!                                                ├─ flagged ─→ quarantine
//!                                                └─ indeterminate ─→ hold
//!
//! `Verdict` is internally tagged on `"tag"`, which is exactly the field the
//! host reads to route conditional (`when`) edges.

use serde::{Deserialize, Serialize};

/// A decoded ERC-20 Transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transfer {
    pub block_number: u64,
    pub log_index: u32,
    pub tx_hash: String,
    pub token: String,
    pub from: String,
    pub to: String,
    /// u256 as a decimal string.
    pub value: String,
}

/// A transfer that has been through the sanctions screen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScreenedTransfer {
    pub transfer: Transfer,
    /// The address that triggered the verdict (the screened counterparty).
    pub counterparty: String,
    /// The screening-list version that produced the verdict (cache key + audit).
    pub list_version: String,
    /// Unix seconds the screen ran (0 if no clock capability).
    pub screened_at: u64,
}

/// A cleared transfer enriched with a USD price.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricedTransfer {
    pub base: ScreenedTransfer,
    /// price * 1e8 at block time.
    pub usd_price_e8: u64,
    pub price_source: String,
}

/// The screener's output. Internally tagged on `"tag"` so the host routes each
/// case to the matching `when` edge.
///
/// - `cleared`        → enricher → system-of-record
/// - `flagged`        → quarantine (NEVER reaches the writer)
/// - `indeterminate`  → hold (fail-closed; re-screened later)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "tag", rename_all = "snake_case")]
pub enum Verdict {
    Cleared(ScreenedTransfer),
    Flagged(ScreenedTransfer),
    Indeterminate(Transfer),
}
