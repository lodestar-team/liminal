//! Enricher node: cleared `ScreenedTransfer` → `PricedTransfer`.
//!
//! Receives the `cleared` arm of the verdict (the host strips nothing — the
//! `"tag"` field is simply ignored on deserialize). For this milestone the
//! price is a deterministic stub so the demo runs fully offline; wiring it to
//! an origin-scoped oracle over `wasi:http` is the W2 increment (the uni-v3
//! enricher already shows the HTTP pattern).

use customs_types::{PricedTransfer, ScreenedTransfer};
use liminal_sdk::node;

// Stablecoins priced at $1.00 (1e8); everything else left at 0 offline.
const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";

node!(|s: ScreenedTransfer| -> Result<Vec<PricedTransfer>, String> {
    let usd_price_e8 = if s.transfer.token == USDC { 100_000_000 } else { 0 };
    Ok(vec![PricedTransfer {
        base: s,
        usd_price_e8,
        price_source: "stub".to_string(),
    }])
});
