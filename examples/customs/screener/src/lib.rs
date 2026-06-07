//! Screener node: `Transfer` → `Verdict`. The compliance gate.
//!
//! It screens both counterparties and emits a tagged `Verdict`; the **host**
//! routes on that tag. The `flagged` and `indeterminate` cases have no edge to
//! the writer, so a sanctioned transfer is *structurally* barred from the SoR.
//!
//! Verdicts are memoised in the host key-value store (W4), namespace `verdicts`,
//! keyed by `(list_version, address)`. The cache is invisible to every other
//! component and busts automatically when `LIST_VERSION` changes (the key
//! changes). For this milestone the list is compiled in; production screening
//! calls an origin-scoped provider over `wasi:http` (W2 — already supported).

use customs_types::{ScreenedTransfer, Transfer, Verdict};
use liminal_sdk::node_kv;

const LIST_VERSION: &str = "ofac-sdn-2024-06-07";

// Publicly documented OFAC-SDN address (Tornado Cash router) — demo only.
const SANCTIONED: &[&str] = &["0x722122df12d4e14e13ac3b6895a86e84145b6967"];

// Addresses the provider cannot resolve → fail-closed to `indeterminate`.
const UNRESOLVABLE: &[&str] = &["0x000000000000000000000000000000000000dead"];

node_kv!(|t: Transfer| -> Result<Vec<Verdict>, String> {
    let from = classify(&t.from.to_lowercase());
    let to = classify(&t.to.to_lowercase());

    // Flagged wins: either side sanctioned → quarantine.
    if from == Class::Sanctioned {
        return Ok(vec![Verdict::Flagged(screened(t.clone(), t.from.to_lowercase()))]);
    }
    if to == Class::Sanctioned {
        return Ok(vec![Verdict::Flagged(screened(t.clone(), t.to.to_lowercase()))]);
    }
    // Otherwise, an unresolvable counterparty fails closed → hold.
    if from == Class::Unresolvable || to == Class::Unresolvable {
        return Ok(vec![Verdict::Indeterminate(t)]);
    }
    // Cleared → on to enrichment and the system of record.
    let counterparty = t.to.to_lowercase();
    Ok(vec![Verdict::Cleared(screened(t, counterparty))])
});

#[derive(PartialEq, Eq, Clone, Copy)]
enum Class {
    Sanctioned,
    Unresolvable,
    Clean,
}

impl Class {
    fn as_byte(self) -> u8 {
        match self {
            Class::Sanctioned => b'S',
            Class::Unresolvable => b'U',
            Class::Clean => b'C',
        }
    }
    fn from_byte(b: u8) -> Option<Class> {
        match b {
            b'S' => Some(Class::Sanctioned),
            b'U' => Some(Class::Unresolvable),
            b'C' => Some(Class::Clean),
            _ => None,
        }
    }
}

/// Classify an address, memoising the decision in the `verdicts` namespace.
/// On a repeat counterparty the result is served from cache — the screening
/// "call" runs once per (list version, address).
fn classify(addr: &str) -> Class {
    let key = format!("{LIST_VERSION}:{addr}");

    if let Some(cached) = kv::get(&key).and_then(|v| v.first().copied()).and_then(Class::from_byte) {
        return cached;
    }

    let class = if SANCTIONED.contains(&addr) {
        Class::Sanctioned
    } else if UNRESOLVABLE.contains(&addr) {
        Class::Unresolvable
    } else {
        Class::Clean
    };

    kv::set(&key, &[class.as_byte()]);
    class
}

fn screened(transfer: Transfer, counterparty: String) -> ScreenedTransfer {
    ScreenedTransfer {
        transfer,
        counterparty,
        list_version: LIST_VERSION.to_string(),
        screened_at: 0,
    }
}
