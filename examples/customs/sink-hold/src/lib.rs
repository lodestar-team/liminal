//! Hold sink — the fail-closed destination for `indeterminate` verdicts.
//!
//! When the screener can't resolve a counterparty, the transfer is held here
//! (durably, in a real build, via `wasi:keyvalue` "hold") rather than written.
//! A background re-screen loop would later resolve it. Writes a `HOLD ` line.

use customs_types::Transfer;
use liminal_sdk::node;

node!(|t: Transfer| -> Result<Vec<Transfer>, String> {
    let line = serde_json::to_string(&t).map_err(|e| e.to_string())?;
    println!("HOLD {line}");
    Ok(vec![]) // terminal
});
