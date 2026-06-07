//! HTTP egress policy — origin allow-lists (W2).
//!
//! Layer 1 (the strong layer) is the Component-Model grant: a node that doesn't
//! import `wasi:http` cannot make a request at all. This is Layer 2: for nodes
//! that *do* import it, the host intercepts every outgoing request and rejects
//! any whose `scheme://authority` isn't on the node's allow-list. Least
//! privilege per stage — the screener reaches the screening origin and nothing
//! else; the enricher reaches the oracle and nothing else.

use wasmtime_wasi_http::p2::{
    bindings::http::types::ErrorCode, body::HyperOutgoingBody, default_send_request,
    types::HostFutureIncomingResponse, types::OutgoingRequestConfig, HttpResult, WasiHttpHooks,
};

/// Per-store egress policy installed as a `wasi:http` hook.
#[derive(Default)]
pub struct OriginPolicy {
    /// `None` = unrestricted (no allow-list configured). `Some` = only these
    /// normalized `scheme://authority` origins are permitted.
    allow: Option<Vec<String>>,
}

impl OriginPolicy {
    /// No restriction — any origin the component reaches is permitted.
    pub fn unrestricted() -> Self {
        Self { allow: None }
    }

    /// Restrict egress to exactly these origins (each `scheme://authority`).
    pub fn restricted(origins: &[String]) -> Self {
        Self { allow: Some(origins.iter().map(|o| normalize(o)).collect()) }
    }

    fn permits(&self, scheme: Option<&str>, authority: Option<&str>) -> bool {
        let Some(list) = &self.allow else { return true };
        match authority {
            // Default scheme to https, matching `wasi:http` convention.
            Some(auth) => {
                let candidate = format!("{}://{}", scheme.unwrap_or("https"), auth).to_lowercase();
                list.iter().any(|o| *o == candidate)
            }
            None => false,
        }
    }
}

/// Normalize an origin to lowercase `scheme://authority` with no trailing slash.
fn normalize(origin: &str) -> String {
    origin.trim().trim_end_matches('/').to_lowercase()
}

impl WasiHttpHooks for OriginPolicy {
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let scheme = request.uri().scheme_str();
        let authority = request.uri().authority().map(|a| a.as_str());
        if self.permits(scheme, authority) {
            Ok(default_send_request(request, config))
        } else {
            tracing::warn!(
                origin = format!("{}://{}", scheme.unwrap_or("?"), authority.unwrap_or("?")),
                "HTTP egress denied: origin not on the component's allow-list"
            );
            Err(ErrorCode::HttpRequestDenied.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrestricted_permits_anything() {
        let p = OriginPolicy::unrestricted();
        assert!(p.permits(Some("https"), Some("anywhere.example")));
        assert!(p.permits(Some("http"), Some("localhost:9000")));
    }

    #[test]
    fn restricted_permits_only_the_allow_list() {
        let p = OriginPolicy::restricted(&["https://coins.llama.fi".to_string()]);
        assert!(p.permits(Some("https"), Some("coins.llama.fi")), "allowed origin must pass");
        assert!(!p.permits(Some("https"), Some("evil.example")), "other origin must be denied");
        assert!(!p.permits(Some("http"), Some("coins.llama.fi")), "wrong scheme must be denied");
        assert!(!p.permits(Some("https"), None), "missing authority must be denied");
    }

    #[test]
    fn normalization_is_case_and_slash_insensitive() {
        let p = OriginPolicy::restricted(&["HTTPS://Screening.Internal/".to_string()]);
        assert!(p.permits(Some("https"), Some("screening.internal")));
    }
}
