//! Host-side bindings for the universal node interface.
//!
//! One `bindgen!` for the whole runtime — there are no per-pipeline types any
//! more. Every component is a `LiminalNode` exporting a single `transform`
//! that takes and returns opaque bytes.

wasmtime::component::bindgen!({
    path: "../wit",
    world: "liminal-node",
    exports: { default: async },
});
