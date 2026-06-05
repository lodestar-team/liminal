//! Price enricher node: `Swap` → `EnrichedSwap` with live USD prices.
//!
//! The only node granted `http`. The `wasi:http` interfaces resolve because
//! the manifest granted this node network egress — and no other node in the
//! pipeline can reach the network at all.

use liminal_sdk::node;
use uni_types::{EnrichedSwap, Swap};
use wasi::http::outgoing_handler;
use wasi::http::types::{Fields, Method, OutgoingRequest, Scheme};
use wasi::io::poll;

// Token metadata for known Uniswap v3 pools:
// (pool, symbol0, symbol1, addr0, addr1). A production build would eth_call the
// pool contract; for the PoC we pre-seed the highest-volume pools.
const KNOWN_POOLS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "USDC", "WETH",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    (
        "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        "USDC", "WETH",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    (
        "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        "WBTC", "WETH",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
    (
        "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
        "DAI", "WETH",
        "0x6b175474e89094c44da98b954eedeac495271d0f",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    ),
];

node!(|swap: Swap| -> Result<Vec<EnrichedSwap>, String> {
    let pool = swap.pool.to_lowercase();
    let (sym0, sym1, addr0, addr1) = KNOWN_POOLS
        .iter()
        .find(|(addr, ..)| *addr == pool.as_str())
        .map(|(_, s0, s1, a0, a1)| (*s0, *s1, *a0, *a1))
        .unwrap_or(("UNKNOWN", "UNKNOWN", "", ""));

    let oracle_url =
        std::env::var("ORACLE_URL").unwrap_or_else(|_| "https://coins.llama.fi".to_string());

    let (price0, price1) = fetch_usd_prices(&oracle_url, addr0, addr1).unwrap_or((0.0, 0.0));
    let amount_usd = best_effort_usd(&swap, price0, price1);

    Ok(vec![EnrichedSwap {
        swap,
        token0_symbol: sym0.to_string(),
        token1_symbol: sym1.to_string(),
        token0_usd_price: price0,
        token1_usd_price: price1,
        amount_usd,
    }])
});

fn fetch_usd_prices(base_url: &str, addr0: &str, addr1: &str) -> Option<(f64, f64)> {
    if addr0.is_empty() || addr1.is_empty() {
        return None;
    }
    let path = format!("/prices/current/ethereum:{addr0},ethereum:{addr1}");
    let body = wasi_https_get(base_url, &path)?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let coins = json.get("coins")?;

    let price = |addr: &str| -> f64 {
        coins
            .get(format!("ethereum:{addr}").as_str())
            .and_then(|v| v.get("price"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    };
    Some((price(addr0), price(addr1)))
}

/// Best-effort USD value: take the larger token side so rounding on one side
/// doesn't zero out the estimate.
fn best_effort_usd(swap: &Swap, price0: f64, price1: f64) -> f64 {
    let amt0: f64 = swap.amount0.parse().unwrap_or(0.0);
    let amt1: f64 = swap.amount1.parse().unwrap_or(0.0);
    (amt0.abs() * price0).max(amt1.abs() * price1)
}

fn wasi_https_get(authority: &str, path_and_query: &str) -> Option<String> {
    let authority = authority.strip_prefix("https://").unwrap_or(authority);
    let headers = Fields::new();
    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Get).ok()?;
    req.set_scheme(Some(&Scheme::Https)).ok()?;
    req.set_authority(Some(authority)).ok()?;
    req.set_path_with_query(Some(path_and_query)).ok()?;

    let fut = outgoing_handler::handle(req, None).ok()?;
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
                    Err(_) => break,
                }
            }
            return String::from_utf8(bytes).ok();
        }
        poll::poll(&[&fut.subscribe()]);
    }
}
