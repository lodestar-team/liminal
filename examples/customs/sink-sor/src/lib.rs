//! System-of-record sink. The compliance-critical writer.
//!
//! It imports no `wasi:http` and has no edge from the `flagged`/`indeterminate`
//! branches — so a sanctioned transfer can neither reach it nor be fetched by
//! it. Writes a `SOR ` line to stdout (the granted capability); a real build
//! would `INSERT` into Postgres.

use customs_types::PricedTransfer;
use liminal_sdk::node;

node!(|p: PricedTransfer| -> Result<Vec<PricedTransfer>, String> {
    let line = serde_json::to_string(&p).map_err(|e| e.to_string())?;
    println!("SOR {line}");
    Ok(vec![]) // terminal
});
