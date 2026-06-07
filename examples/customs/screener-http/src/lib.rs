//! Live screener: `Transfer` → `Verdict`, screening each counterparty against a
//! sanctions provider over `wasi:http`.
//!
//! Three capabilities, each scoped by the host:
//!   - `http` + `allow_origins` (W2): may reach ONLY the screening origin.
//!   - `keyvalue = "verdicts"` (W4): a private verdict cache no one else can read.
//!
//! **Fail-closed (W7):** if the provider is unreachable, times out, or returns
//! anything but a clean 200, the verdict is `indeterminate` → the transfer is
//! held, never written. A compliance gate that fails *open* is no gate at all.

use customs_types::{ScreenedTransfer, Transfer, Verdict};
use liminal_sdk::node_kv;
use wasi::http::outgoing_handler;
use wasi::http::types::{Fields, Method, OutgoingRequest, Scheme};
use wasi::io::poll;

node_kv!(|t: Transfer| -> Result<Vec<Verdict>, String> {
    let base = std::env::var("SCREENING_URL").unwrap_or_else(|_| "http://localhost:8088".into());
    let version =
        std::env::var("SCREENING_LIST_VERSION").unwrap_or_else(|_| "live".into());

    let from = classify(&base, &t.from.to_lowercase());
    let to = classify(&base, &t.to.to_lowercase());

    // Flagged wins; then fail-closed; then cleared.
    if from == Class::Sanctioned {
        return Ok(vec![Verdict::Flagged(screened(t.clone(), t.from.to_lowercase(), &version))]);
    }
    if to == Class::Sanctioned {
        return Ok(vec![Verdict::Flagged(screened(t.clone(), t.to.to_lowercase(), &version))]);
    }
    if from == Class::Indeterminate || to == Class::Indeterminate {
        return Ok(vec![Verdict::Indeterminate(t)]);
    }
    let counterparty = t.to.to_lowercase();
    Ok(vec![Verdict::Cleared(screened(t, counterparty, &version))])
});

#[derive(PartialEq, Eq, Clone, Copy)]
enum Class {
    Sanctioned,
    Clean,
    Indeterminate,
}

/// Screen an address: cache → provider → fail-closed. Only definitive answers
/// (sanctioned / clean) are cached; transient failures are not.
fn classify(base: &str, addr: &str) -> Class {
    if let Some(byte) = kv::get(addr).and_then(|v| v.first().copied()) {
        match byte {
            b'S' => return Class::Sanctioned,
            b'C' => return Class::Clean,
            _ => {}
        }
    }

    match screen(base, addr) {
        Some(true) => {
            kv::set(addr, b"S");
            Class::Sanctioned
        }
        Some(false) => {
            kv::set(addr, b"C");
            Class::Clean
        }
        None => Class::Indeterminate, // fail closed; do not cache
    }
}

/// GET {base}/screen?address={addr} → Some(sanctioned) or None on any failure.
fn screen(base: &str, addr: &str) -> Option<bool> {
    let path = format!("/screen?address={addr}");
    let body = http_get(base, &path)?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    json.get("sanctioned")?.as_bool()
}

fn screened(transfer: Transfer, counterparty: String, version: &str) -> ScreenedTransfer {
    ScreenedTransfer {
        transfer,
        counterparty,
        list_version: version.to_string(),
        screened_at: 0,
    }
}

fn http_get(base: &str, path_and_query: &str) -> Option<String> {
    let (scheme, authority) = match base.split_once("://") {
        Some(("https", a)) => (Scheme::Https, a),
        Some(("http", a)) => (Scheme::Http, a),
        _ => (Scheme::Http, base),
    };

    let req = OutgoingRequest::new(Fields::new());
    req.set_method(&Method::Get).ok()?;
    req.set_scheme(Some(&scheme)).ok()?;
    req.set_authority(Some(authority)).ok()?;
    req.set_path_with_query(Some(path_and_query)).ok()?;

    let fut = outgoing_handler::handle(req, None).ok()?;
    loop {
        if let Some(result) = fut.get() {
            let resp = result.ok()?.ok()?;
            if resp.status() != 200 {
                return None;
            }
            // Bind the body so it outlives its child stream — dropping a body
            // with a live stream traps with "resource has children".
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
