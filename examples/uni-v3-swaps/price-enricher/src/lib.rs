wit_bindgen::generate!({
    world: "enricher-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::enrich::Guest;
use liminal::pipeline::types::{EnrichedSwap, Swap};

// Token metadata for the pools we care about.
// Keyed by pool address (lowercase), value is (token0_sym, token1_sym, token0_addr, token1_addr).
// Expand as needed; a production implementation would fetch this from the pool contract.
const KNOWN_POOLS: &[(&str, &str, &str, &str, &str)] = &[
    // USDC/WETH 0.05%
    (
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "USDC", "WETH",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    // USDC/WETH 0.3%
    (
        "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        "USDC", "WETH",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    // WBTC/WETH 0.3%
    (
        "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        "WBTC", "WETH",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
];

struct PriceEnricher;

impl Guest for PriceEnricher {
    fn enrich_swap(swap: Swap) -> Result<EnrichedSwap, String> {
        let pool = swap.pool.to_lowercase();

        let (token0_sym, token1_sym, token0_addr, token1_addr) = KNOWN_POOLS
            .iter()
            .find(|(addr, ..)| *addr == pool.as_str())
            .map(|(_, s0, s1, a0, a1)| (*s0, *s1, *a0, *a1))
            .unwrap_or(("UNKNOWN", "UNKNOWN", "", ""));

        // -----------------------------------------------------------------------
        // HTTP call to DeFiLlama price oracle.
        // This is the capability that makes Liminal different: this component
        // imports wasi:http (granted only here by the host), making the
        // enrichment a first-class part of the pipeline rather than a sidecar.
        //
        // For the PoC we call the DeFiLlama /prices/current endpoint.
        // -----------------------------------------------------------------------
        let (token0_usd, token1_usd) =
            fetch_prices(token0_addr, token1_addr).unwrap_or((0.0, 0.0));

        let amount_usd = compute_amount_usd(&swap, token0_usd, token1_usd, token0_addr);

        Ok(EnrichedSwap {
            swap,
            token0_symbol: token0_sym.to_string(),
            token1_symbol: token1_sym.to_string(),
            token0_usd_price: token0_usd,
            token1_usd_price: token1_usd,
            amount_usd,
        })
    }
}

/// Fetches USD prices for two EVM token addresses from DeFiLlama.
/// Uses the WASI HTTP interface made available by the host.
fn fetch_prices(addr0: &str, addr1: &str) -> Option<(f64, f64)> {
    if addr0.is_empty() || addr1.is_empty() {
        return None;
    }

    // DeFiLlama coins API: /prices/current/ethereum:<addr>,ethereum:<addr>
    let coins = format!("ethereum:{addr0},ethereum:{addr1}");
    let url = format!("https://coins.llama.fi/prices/current/{coins}");

    // wasi:http call — the host has granted this component outbound HTTP
    // to coins.llama.fi only.  The decoder and sink components have no
    // HTTP capability whatsoever.
    let body = wasi_http_get(&url)?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let coins_obj = json.get("coins")?;

    let p0 = coins_obj
        .get(format!("ethereum:{addr0}").as_str())
        .and_then(|v| v.get("price"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let p1 = coins_obj
        .get(format!("ethereum:{addr1}").as_str())
        .and_then(|v| v.get("price"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    Some((p0, p1))
}

/// Compute a best-effort USD value for the swap using the larger of the two
/// token sides (the other side of the pool might be near zero temporarily).
fn compute_amount_usd(swap: &Swap, price0: f64, price1: f64, token0_addr: &str) -> f64 {
    // Use the positive-side amount as the swap value.
    let amt0: f64 = swap.amount0.parse().unwrap_or(0.0);
    let amt1: f64 = swap.amount1.parse().unwrap_or(0.0);

    let usd0 = amt0.abs() * price0;
    let usd1 = amt1.abs() * price1;

    // Use whichever is larger (handles cases where one side is near-zero).
    usd0.max(usd1)
}

/// Thin wrapper around the WASI outgoing-handler for a simple GET request.
/// The real implementation uses the generated wasi:http bindings; this is
/// a stand-in until the wasi:http WIT package is bundled.
fn wasi_http_get(_url: &str) -> Option<String> {
    // TODO: replace with generated wasi:http/outgoing-handler bindings once
    // the wasi:http WIT package is included in the workspace.
    // See: https://github.com/WebAssembly/wasi-http
    todo!("wire up wasi:http/outgoing-handler")
}

export!(PriceEnricher);
