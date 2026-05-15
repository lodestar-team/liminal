wit_bindgen::generate!({
    world: "sink-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::sink::Guest;
use liminal::pipeline::types::EnrichedSwap;

/// DDL for reference:
///
/// ```sql
/// CREATE TABLE uniswap_v3_swaps (
///     id              BIGSERIAL PRIMARY KEY,
///     block_number    BIGINT      NOT NULL,
///     tx_hash         TEXT        NOT NULL,
///     log_index       INT         NOT NULL,
///     pool            TEXT        NOT NULL,
///     sender          TEXT        NOT NULL,
///     recipient       TEXT        NOT NULL,
///     amount0         NUMERIC     NOT NULL,
///     amount1         NUMERIC     NOT NULL,
///     tick            INT         NOT NULL,
///     token0_symbol   TEXT,
///     token1_symbol   TEXT,
///     token0_usd      DOUBLE PRECISION,
///     token1_usd      DOUBLE PRECISION,
///     amount_usd      DOUBLE PRECISION,
///     indexed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
/// );
/// CREATE UNIQUE INDEX ON uniswap_v3_swaps (tx_hash, log_index);
/// ```
struct PostgresSink;

impl Guest for PostgresSink {
    fn write_batch(swaps: Vec<EnrichedSwap>) -> Result<u32, String> {
        // Connection string is injected via POSTGRES_CONFIG env var, set by
        // the host exclusively for this component instance.
        std::env::var("POSTGRES_CONFIG")
            .map_err(|_| "POSTGRES_CONFIG not set".to_string())?;

        // TODO: use wasi:sql or a Wasm-compatible postgres driver once one
        // stabilises.  For the PoC, emit the INSERT SQL to stdout so the host
        // can pipe it to psql or a relay.
        for swap in &swaps {
            let sql = format!(
                "INSERT INTO uniswap_v3_swaps \
                 (block_number,tx_hash,log_index,pool,sender,recipient,\
                  amount0,amount1,tick,token0_symbol,token1_symbol,\
                  token0_usd,token1_usd,amount_usd) \
                 VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{}) \
                 ON CONFLICT (tx_hash,log_index) DO NOTHING;",
                swap.swap.block_number,
                sql_str(&swap.swap.tx_hash),
                swap.swap.log_index,
                sql_str(&swap.swap.pool),
                sql_str(&swap.swap.sender),
                sql_str(&swap.swap.recipient),
                sql_str(&swap.swap.amount0),
                sql_str(&swap.swap.amount1),
                swap.swap.tick,
                sql_str(&swap.token0_symbol),
                sql_str(&swap.token1_symbol),
                swap.token0_usd_price,
                swap.token1_usd_price,
                swap.amount_usd,
            );
            println!("{sql}");
        }

        Ok(swaps.len() as u32)
    }
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

export!(PostgresSink);
