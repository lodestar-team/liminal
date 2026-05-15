wit_bindgen::generate!({
    world: "arb-enricher-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::arb_enrich::Guest;
use liminal::pipeline::arb_types::{EnrichedArbSwap, NormalizedSwap};
use wasi::http::outgoing_handler;
use wasi::http::types::{Fields, Method, OutgoingRequest, Scheme};
use wasi::io::poll;

struct ArbEnricher;

impl Guest for ArbEnricher {
    fn enrich_swap(swap: NormalizedSwap) -> Result<EnrichedArbSwap, String> {
        if swap.token_in.is_empty() || swap.token_out.is_empty() {
            return Err("token addresses unknown".to_string());
        }

        let oracle_url = std::env::var("ORACLE_URL")
            .unwrap_or_else(|_| "https://coins.llama.fi".to_string());

        let (in_sym, in_price, in_dec, out_sym, out_price, out_dec) =
            fetch_token_info(&oracle_url, &swap.token_in, &swap.token_out)
                .unwrap_or_else(|| ("?".into(), 0.0, 18, "?".into(), 0.0, 18));

        Ok(EnrichedArbSwap {
            swap,
            token_in_symbol:    in_sym,
            token_out_symbol:   out_sym,
            token_in_usd_price: in_price,
            token_out_usd_price: out_price,
            token_in_decimals:  in_dec,
            token_out_decimals: out_dec,
        })
    }
}

struct TokenInfo {
    symbol:   String,
    price:    f64,
    decimals: u8,
}

fn fetch_token_info(
    base_url: &str,
    addr_in: &str,
    addr_out: &str,
) -> Option<(String, f64, u8, String, f64, u8)> {
    let path = format!("/prices/current/ethereum:{addr_in},ethereum:{addr_out}");
    let body = wasi_https_get(base_url, &path)?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let coins = json.get("coins")?;

    let get = |addr: &str| -> TokenInfo {
        let key = format!("ethereum:{addr}");
        let obj = coins.get(&key);
        TokenInfo {
            symbol:   obj.and_then(|v| v.get("symbol")).and_then(|v| v.as_str())
                         .unwrap_or("?").to_string(),
            price:    obj.and_then(|v| v.get("price")).and_then(|v| v.as_f64())
                         .unwrap_or(0.0),
            decimals: obj.and_then(|v| v.get("decimals")).and_then(|v| v.as_u64())
                         .unwrap_or(18) as u8,
        }
    };

    let ti = get(addr_in);
    let to = get(addr_out);
    Some((ti.symbol, ti.price, ti.decimals, to.symbol, to.price, to.decimals))
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
            if resp.status() != 200 { return None; }
            let body = resp.consume().ok()?;
            let stream = body.stream().ok()?;
            let mut bytes = Vec::new();
            loop {
                match stream.blocking_read(4096) {
                    Ok(chunk) if chunk.is_empty() => break,
                    Ok(chunk) => bytes.extend(chunk),
                    Err(_)    => break,
                }
            }
            return String::from_utf8(bytes).ok();
        }
        poll::poll(&[&fut.subscribe()]);
    }
}

export!(ArbEnricher);
