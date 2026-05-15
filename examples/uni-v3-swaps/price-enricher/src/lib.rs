wit_bindgen::generate!({
    world: "enricher-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::enrich::Guest;
use liminal::pipeline::types::{EnrichedSwap, Swap};
use wasi::http::outgoing_handler;
use wasi::http::types::{Fields, Method, OutgoingRequest, Scheme};
use wasi::io::poll;

// Token metadata for known Uniswap v3 pools (pool address → token symbols + addresses).
// A production implementation would fetch this from the pool contract via eth_call,
// but for the PoC we pre-seed the highest-volume pools.
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
    // DAI/WETH 0.3%
    (
        "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
        "DAI", "WETH",
        "0x6b175474e89094c44da98b954eedeac495271d0f",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
];

struct PriceEnricher;

impl Guest for PriceEnricher {
    fn enrich_swap(swap: Swap) -> Result<EnrichedSwap, String> {
        let pool = swap.pool.to_lowercase();

        let (sym0, sym1, addr0, addr1) = KNOWN_POOLS
            .iter()
            .find(|(addr, ..)| *addr == pool.as_str())
            .map(|(_, s0, s1, a0, a1)| (*s0, *s1, *a0, *a1))
            .unwrap_or(("UNKNOWN", "UNKNOWN", "", ""));

        // -------------------------------------------------------------------
        // HTTP call to DeFiLlama: this is the capability that distinguishes
        // Liminal from a subgraph mapping.  The `wasi::http` interfaces are
        // available here because the host granted wasi:http to this component's
        // linker — and only to this component's linker.  The decoder and sink
        // components cannot reach the network at all.
        // -------------------------------------------------------------------
        let oracle_url = std::env::var("ORACLE_URL")
            .unwrap_or_else(|_| "https://coins.llama.fi".to_string());

        let (price0, price1) = fetch_usd_prices(&oracle_url, addr0, addr1)
            .unwrap_or((0.0, 0.0));

        let amount_usd = best_effort_usd(&swap, price0, price1);

        Ok(EnrichedSwap {
            swap,
            token0_symbol: sym0.to_string(),
            token1_symbol: sym1.to_string(),
            token0_usd_price: price0,
            token1_usd_price: price1,
            amount_usd,
        })
    }
}

/// Fetch USD prices for two EVM tokens from DeFiLlama /prices/current.
fn fetch_usd_prices(base_url: &str, addr0: &str, addr1: &str) -> Option<(f64, f64)> {
    if addr0.is_empty() || addr1.is_empty() {
        return None;
    }

    let path = format!(
        "/prices/current/ethereum:{addr0},ethereum:{addr1}"
    );
    let body = wasi_https_get(base_url, &path)?;

    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let coins = json.get("coins")?;

    let p0 = coins
        .get(format!("ethereum:{addr0}").as_str())
        .and_then(|v| v.get("price"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let p1 = coins
        .get(format!("ethereum:{addr1}").as_str())
        .and_then(|v| v.get("price"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    Some((p0, p1))
}

/// Compute a best-effort USD value for the swap: use the larger of the two
/// token sides so partial liquidity / rounding on one side doesn't zero out.
fn best_effort_usd(swap: &Swap, price0: f64, price1: f64) -> f64 {
    let amt0: f64 = swap.amount0.parse().unwrap_or(0.0);
    let amt1: f64 = swap.amount1.parse().unwrap_or(0.0);
    (amt0.abs() * price0).max(amt1.abs() * price1)
}

/// Synchronous HTTPS GET using the WASI outgoing-handler.
///
/// Uses `wasi::io::poll` to block until the response is ready — the correct
/// pattern for a synchronous WASIp2 component.  The host must have granted
/// `wasi:http/outgoing-handler` to this component's linker; if it hasn't,
/// the `handle` call will trap, which is the intended capability enforcement.
fn wasi_https_get(authority: &str, path_and_query: &str) -> Option<String> {
    // Strip any scheme prefix so we have just the host[:port].
    let authority = authority
        .strip_prefix("https://")
        .unwrap_or(authority);

    let headers = Fields::new();
    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Get).ok()?;
    req.set_scheme(Some(&Scheme::Https)).ok()?;
    req.set_authority(Some(authority)).ok()?;
    req.set_path_with_query(Some(path_and_query)).ok()?;

    let fut = outgoing_handler::handle(req, None).ok()?;

    // Poll until the future resolves.
    loop {
        if let Some(result) = fut.get() {
            let resp = result.ok()?.ok()?;
            if resp.status() != 200 {
                return None;
            }
            let body = resp.consume().ok()?;
            let stream = body.stream().ok()?;
            let mut bytes = Vec::new();
            loop {
                match stream.blocking_read(4096) {
                    Ok(chunk) if chunk.is_empty() => break,
                    Ok(chunk) => bytes.extend(chunk),
                    Err(_)   => break,
                }
            }
            return String::from_utf8(bytes).ok();
        }
        // Not ready yet — yield to the WASI scheduler.
        let pollable = fut.subscribe();
        poll::poll(&[&pollable]);
    }
}

export!(PriceEnricher);
