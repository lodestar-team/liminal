/// Host-side WIT bindings for the cross-DEX arb pipeline.

pub mod decoder {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "arb-decoder-world",
        exports: { default: async },
    });
}

pub mod enricher {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "arb-enricher-world",
        exports: { default: async },
    });
}

pub mod sink {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "arb-sink-world",
        exports: { default: async },
    });
}

// Canonical types — use decoder's generated structs.
pub use decoder::liminal::pipeline::arb_types::{EnrichedArbSwap, NormalizedSwap};
