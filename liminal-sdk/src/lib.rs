//! # liminal-sdk
//!
//! Everything a Liminal component author needs, and nothing they don't.
//!
//! A component is just a typed function. Wrap it with [`node!`] and you get a
//! WASIp2 component exporting `liminal:node/node` — the host handles the rest:
//! wiring, capabilities, and piping JSON bytes along the DAG's edges.
//!
//! ```rust,ignore
//! use liminal_sdk::{node, EvmLog};
//! use my_types::Swap;
//!
//! node!(|log: EvmLog| -> Result<Vec<Swap>, String> {
//!     // return Ok(vec![]) to filter, Ok(vec![swap]) to emit, Err(_) to skip.
//!     Ok(decode(log).into_iter().collect())
//! });
//! ```

// Re-exported so component crates depend only on liminal-sdk.
pub use serde;
pub use serde_json;
pub use wit_bindgen;

use serde::{de::DeserializeOwned, Serialize};

/// The one message the host itself produces: a raw EVM log straight off the
/// wire. Decoders deserialize this; everything downstream is component-defined.
///
/// Kept in the SDK because it is the single type the host and components must
/// agree on byte-for-byte. Field names are the JSON contract.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvmLog {
    /// Contract address that emitted the log (hex, `0x`-prefixed).
    pub address: String,
    /// Indexed topics; `topics[0]` is the event signature hash.
    pub topics: Vec<String>,
    /// Non-indexed ABI-encoded data.
    pub data: Vec<u8>,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u32,
}

/// The glue between the typed closure you write and the raw byte boundary the
/// WIT interface demands. You never call this directly — [`node!`] does.
///
/// Decodes the input JSON into `In`, runs your function, then encodes each
/// `Out` back to JSON. Any error — decode, your logic, or encode — becomes the
/// `result::err` string the host logs.
pub fn run_node<In, Out, E, F>(input: Vec<u8>, f: F) -> Result<Vec<Vec<u8>>, String>
where
    In: DeserializeOwned,
    Out: Serialize,
    E: core::fmt::Display,
    F: FnOnce(In) -> Result<Vec<Out>, E>,
{
    let parsed: In =
        serde_json::from_slice(&input).map_err(|e| format!("decoding node input: {e}"))?;
    let outputs = f(parsed).map_err(|e| e.to_string())?;
    outputs
        .iter()
        .map(|o| serde_json::to_vec(o).map_err(|e| format!("encoding node output: {e}")))
        .collect()
}

/// Turn a typed closure into a complete WASIp2 component.
///
/// Expands to the WIT bindings, the `Guest` implementation, and the `export!`
/// invocation. Drop one of these at your crate root and you have a component.
///
/// The closure signature is `Fn(In) -> Result<Vec<Out>, E>` where `In` and
/// `Out` are any `serde` types and `E: Display` (so `String`, `anyhow::Error`,
/// or your own error all work).
#[macro_export]
macro_rules! node {
    ($func:expr) => {
        // The WIT is inlined (not loaded from a path) so a component needs no
        // knowledge of the repo layout. This MUST stay in sync with
        // `wit/world.wit`, the canonical definition the host binds against.
        //
        // `generate!` emits code that refers to the `wit_bindgen` crate by bare
        // name, so component crates must depend on `wit-bindgen` directly.
        wit_bindgen::generate!({
            inline: "
                package liminal:node@0.2.0;
                interface node {
                    transform: func(input: list<u8>) -> result<list<list<u8>>, string>;
                }
                world liminal-node {
                    export node;
                }
            ",
        });

        struct __LiminalNode;

        impl exports::liminal::node::node::Guest for __LiminalNode {
            fn transform(input: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
                $crate::run_node(input, $func)
            }
        }

        export!(__LiminalNode);
    };
}

/// Like [`node!`], but the component also imports the host key-value store (W4).
///
/// Inside the closure, call `kv::get/set/delete/exists` to use the store; the
/// host scopes every key to the namespace granted to this node in the manifest.
/// A component built with this macro MUST be granted a `keyvalue` namespace, or
/// it won't instantiate.
///
/// ```rust,ignore
/// use liminal_sdk::node_kv;
/// node_kv!(|t: Transfer| -> Result<Vec<Verdict>, String> {
///     if let Some(cached) = kv::get("some-key") { /* ... */ }
///     kv::set("some-key", b"value");
///     Ok(vec![/* ... */])
/// });
/// ```
#[macro_export]
macro_rules! node_kv {
    ($func:expr) => {
        wit_bindgen::generate!({
            inline: "
                package liminal:node@0.2.0;
                interface node {
                    transform: func(input: list<u8>) -> result<list<list<u8>>, string>;
                }
                interface store {
                    get: func(key: string) -> option<list<u8>>;
                    set: func(key: string, value: list<u8>);
                    delete: func(key: string);
                    exists: func(key: string) -> bool;
                }
                world liminal-node-kv {
                    export node;
                    import store;
                }
            ",
        });

        /// Namespaced key-value access, scoped by the host to this node.
        mod kv {
            pub use super::liminal::node::store::{delete, exists, get, set};
        }

        struct __LiminalNode;

        impl exports::liminal::node::node::Guest for __LiminalNode {
            fn transform(input: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
                $crate::run_node(input, $func)
            }
        }

        export!(__LiminalNode);
    };
}
