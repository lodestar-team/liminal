wit_bindgen::generate!({
    world: "arb-sink-world",
    path: "../../../wit",
});

use exports::liminal::pipeline::arb_sink::Guest;
use liminal::pipeline::arb_types::{EnrichedArbSwap, Protocol};

struct JsonSink;

impl Guest for JsonSink {
    fn write_batch(swaps: Vec<EnrichedArbSwap>) -> Result<u32, String> {
        for s in &swaps {
            let protocol = match s.swap.protocol {
                Protocol::UniswapV3  => "uniswap_v3",
                Protocol::BalancerV2 => "balancer_v2",
            };
            // Hand-roll JSON to avoid a serde dependency in this tiny component.
            println!(
                "{{\
                    \"protocol\":\"{}\",\
                    \"pool\":\"{}\",\
                    \"token_in\":\"{}\",\
                    \"token_out\":\"{}\",\
                    \"amount_in\":\"{}\",\
                    \"amount_out\":\"{}\",\
                    \"token_in_symbol\":\"{}\",\
                    \"token_out_symbol\":\"{}\",\
                    \"token_in_usd_price\":{},\
                    \"token_out_usd_price\":{},\
                    \"token_in_decimals\":{},\
                    \"token_out_decimals\":{},\
                    \"block_number\":{},\
                    \"tx_hash\":\"{}\",\
                    \"log_index\":{}\
                }}",
                protocol,
                s.swap.pool,
                s.swap.token_in,
                s.swap.token_out,
                s.swap.amount_in,
                s.swap.amount_out,
                s.token_in_symbol,
                s.token_out_symbol,
                s.token_in_usd_price,
                s.token_out_usd_price,
                s.token_in_decimals,
                s.token_out_decimals,
                s.swap.block_number,
                s.swap.tx_hash,
                s.swap.log_index,
            );
        }
        Ok(swaps.len() as u32)
    }
}

export!(JsonSink);
