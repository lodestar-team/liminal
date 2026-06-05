//! Terminal JSON sink: prints each enriched swap to stdout as a JSON line and
//! re-emits it so the host can broadcast it to the live dashboard.
//!
//! Writing to stdout needs the `stdout` capability — granted to this node, and
//! this node only, in the manifest.

use arb_types::EnrichedArbSwap;
use liminal_sdk::node;

node!(|swap: EnrichedArbSwap| -> Result<Vec<EnrichedArbSwap>, String> {
    if let Ok(line) = serde_json::to_string(&swap) {
        // stdout — capability granted by the manifest.
        println!("{line}");
    }
    // Re-emit: as a terminal node, our output is the pipeline's output, which
    // the host fans out to the dashboard SSE stream.
    Ok(vec![swap])
});
