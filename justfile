# Liminal task runner. Install just: https://github.com/casey/just
#
#   just build       # build host + all wasm components, copy artifacts
#   just run-arb     # run the cross-dex-arb pipeline (needs ETH_RPC_URL)
#   just run-uni     # run the uni-v3-swaps pipeline (needs ETH_RPC_URL)
#   just test        # run host tests

# Component crate name -> output wasm path (snake_cased by cargo).
arb_components := "arb-decoder arb-enricher arb-sink-json"
uni_components := "uni-v3-decoder uni-v3-price-enricher uni-v3-sink-postgres uni-v3-sink-kafka"

default: build

# Build the host (native) and every component (wasm), then stage the .wasm files.
build: build-host build-components

build-host:
    cargo build --release -p liminal-host

build-components:
    cargo build --target wasm32-wasip2 --release \
        -p arb-decoder -p arb-enricher -p arb-sink-json \
        -p uni-v3-decoder -p uni-v3-price-enricher \
        -p uni-v3-sink-postgres -p uni-v3-sink-kafka
    # Stage cross-dex-arb artifacts
    cp target/wasm32-wasip2/release/arb_decoder.wasm   examples/cross-dex-arb/decoder.wasm
    cp target/wasm32-wasip2/release/arb_enricher.wasm  examples/cross-dex-arb/enricher.wasm
    cp target/wasm32-wasip2/release/arb_sink_json.wasm examples/cross-dex-arb/sink-json.wasm
    # Stage uni-v3-swaps artifacts
    cp target/wasm32-wasip2/release/uni_v3_decoder.wasm        examples/uni-v3-swaps/decoder.wasm
    cp target/wasm32-wasip2/release/uni_v3_price_enricher.wasm examples/uni-v3-swaps/price-enricher.wasm
    cp target/wasm32-wasip2/release/uni_v3_sink_postgres.wasm  examples/uni-v3-swaps/sink-postgres.wasm
    cp target/wasm32-wasip2/release/uni_v3_sink_kafka.wasm     examples/uni-v3-swaps/sink-kafka.wasm

test:
    cargo test -p liminal-host

# Run a pipeline from its manifest. ETH_RPC_URL must be set.
run-arb *ARGS:
    cargo run --release -p liminal-host -- examples/cross-dex-arb/pipeline.toml {{ARGS}}

run-uni *ARGS:
    cargo run --release -p liminal-host -- examples/uni-v3-swaps/pipeline.toml {{ARGS}}
