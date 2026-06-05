//! Terminal Postgres sink: emits upsert SQL for each enriched swap to stdout
//! (so the host can pipe it to psql or a relay). A Wasm-native postgres driver
//! would replace the stdout hop once one stabilises.
//!
//! Needs the `stdout` capability and a `POSTGRES_CONFIG` env var, both granted
//! by the manifest. As a terminal node it returns no downstream output.
//!
//! DDL for reference:
//! ```sql
//! CREATE TABLE uniswap_v3_swaps (
//!     id BIGSERIAL PRIMARY KEY,
//!     block_number BIGINT NOT NULL, tx_hash TEXT NOT NULL, log_index INT NOT NULL,
//!     pool TEXT, sender TEXT, recipient TEXT,
//!     amount0 NUMERIC, amount1 NUMERIC, tick INT,
//!     token0_symbol TEXT, token1_symbol TEXT,
//!     token0_usd DOUBLE PRECISION, token1_usd DOUBLE PRECISION, amount_usd DOUBLE PRECISION,
//!     indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
//! );
//! CREATE UNIQUE INDEX ON uniswap_v3_swaps (tx_hash, log_index);
//! ```

use liminal_sdk::node;
use uni_types::EnrichedSwap;

node!(|e: EnrichedSwap| -> Result<Vec<EnrichedSwap>, String> {
    // Connection string is injected for this node only.
    std::env::var("POSTGRES_CONFIG").map_err(|_| "POSTGRES_CONFIG not set".to_string())?;

    let sql = format!(
        "INSERT INTO uniswap_v3_swaps \
         (block_number,tx_hash,log_index,pool,sender,recipient,\
          amount0,amount1,tick,token0_symbol,token1_symbol,\
          token0_usd,token1_usd,amount_usd) \
         VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{}) \
         ON CONFLICT (tx_hash,log_index) DO NOTHING;",
        e.swap.block_number,
        sql_str(&e.swap.tx_hash),
        e.swap.log_index,
        sql_str(&e.swap.pool),
        sql_str(&e.swap.sender),
        sql_str(&e.swap.recipient),
        sql_str(&e.swap.amount0),
        sql_str(&e.swap.amount1),
        e.swap.tick,
        sql_str(&e.token0_symbol),
        sql_str(&e.token1_symbol),
        e.token0_usd_price,
        e.token1_usd_price,
        e.amount_usd,
    );
    println!("{sql}");

    Ok(vec![]) // terminal: nothing flows onward
});

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
