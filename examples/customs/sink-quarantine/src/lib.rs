//! Quarantine sink — the destination for `flagged` verdicts.
//!
//! This is where sanctioned transfers land, and the only place they land.
//! Writes a `QUARANTINE ` line to stdout.

use customs_types::ScreenedTransfer;
use liminal_sdk::node;

node!(|s: ScreenedTransfer| -> Result<Vec<ScreenedTransfer>, String> {
    let line = serde_json::to_string(&s).map_err(|e| e.to_string())?;
    println!("QUARANTINE {line}");
    Ok(vec![]) // terminal
});
