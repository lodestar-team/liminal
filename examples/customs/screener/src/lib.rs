//! Screener node: `Transfer` → `Verdict`. The compliance gate.
//!
//! It screens both counterparties and emits a tagged `Verdict`; the **host**
//! routes on that tag. The `flagged` and `indeterminate` cases have no edge to
//! the writer, so a sanctioned transfer is *structurally* barred from the SoR.
//!
//! For this milestone the list is compiled in (pure compute, zero capabilities).
//! Production screening calls an origin-scoped provider over `wasi:http` — that
//! is tracked as W2; the runtime already supports origin-granted HTTP.

use customs_types::{ScreenedTransfer, Transfer, Verdict};
use liminal_sdk::node;

const LIST_VERSION: &str = "ofac-sdn-2024-06-07";

// Publicly documented OFAC-SDN address (Tornado Cash router) — demo only.
const SANCTIONED: &[&str] = &["0x722122df12d4e14e13ac3b6895a86e84145b6967"];

// Addresses the provider cannot resolve → fail-closed to `indeterminate`.
// (Stands in for a provider timeout / non-2xx in the offline demo.)
const UNRESOLVABLE: &[&str] = &["0x000000000000000000000000000000000000dead"];

node!(|t: Transfer| -> Result<Vec<Verdict>, String> {
    let from = t.from.to_lowercase();
    let to = t.to.to_lowercase();

    // Flagged wins over everything: either side sanctioned → quarantine.
    if let Some(hit) = [&from, &to].into_iter().find(|a| SANCTIONED.contains(&a.as_str())) {
        return Ok(vec![Verdict::Flagged(screened(t.clone(), hit.clone()))]);
    }

    // Otherwise, if we cannot resolve a counterparty, fail closed → hold.
    if [&from, &to].into_iter().any(|a| UNRESOLVABLE.contains(&a.as_str())) {
        return Ok(vec![Verdict::Indeterminate(t)]);
    }

    // Cleared → on to enrichment and the system of record. Counterparty of
    // record is the recipient.
    Ok(vec![Verdict::Cleared(screened(t.clone(), to))])
});

fn screened(transfer: Transfer, counterparty: String) -> ScreenedTransfer {
    ScreenedTransfer {
        transfer,
        counterparty,
        list_version: LIST_VERSION.to_string(),
        screened_at: 0,
    }
}
