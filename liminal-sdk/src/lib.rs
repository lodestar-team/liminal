/// Re-export wit-bindgen so component authors only need to depend on
/// liminal-sdk, not wit-bindgen directly.
pub use wit_bindgen;

/// Convenience macro: generate WIT bindings pointed at the workspace-root
/// wit/ directory.  Use this inside component crates.
///
/// ```rust,ignore
/// liminal_sdk::generate!(decoder_world);
/// ```
#[macro_export]
macro_rules! generate {
    ($world:ident) => {
        $crate::wit_bindgen::generate!({
            world: stringify!($world),
            path: concat!(env!("CARGO_MANIFEST_DIR"), "/../../../wit"),
        });
    };
}
