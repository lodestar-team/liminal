use anyhow::Result;
use alloy::{
    primitives::FixedBytes,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::eth::{Filter, Log},
};
use futures_util::StreamExt;
use liminal_sdk::EvmLog;
use std::any::Any;
use tracing::debug;

/// Wraps an EVM WebSocket log subscription and yields canonical [`EvmLog`]s —
/// the one message type the host produces and decoder nodes consume.
pub struct EvmSource {
    // Keep the provider alive — dropping it closes the WS task and ends the stream.
    _provider: Box<dyn Any + Send + Sync>,
    inner: Box<dyn futures_util::Stream<Item = Log> + Unpin + Send>,
}

impl EvmSource {
    pub async fn connect(rpc_url: &str, topic_hexes: &[String]) -> Result<Self> {
        let ws = WsConnect::new(rpc_url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let topics: Vec<FixedBytes<32>> = topic_hexes
            .iter()
            .map(|h| h.trim_start_matches("0x").parse())
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("invalid topic hex in manifest: {e}"))?;

        let filter = Filter::new().event_signature(topics);
        let sub = provider.subscribe_logs(&filter).await?;

        debug!(topics = topic_hexes.len(), "subscribed to logs");
        Ok(Self { _provider: Box::new(provider), inner: Box::new(sub.into_stream()) })
    }

    /// Yield the next log as a canonical [`EvmLog`].
    pub async fn next(&mut self) -> Option<Result<EvmLog>> {
        self.inner.next().await.map(|log| Ok(to_evm_log(&log)))
    }
}

/// Flatten an alloy RPC log into the SDK's wire type.
fn to_evm_log(log: &Log) -> EvmLog {
    EvmLog {
        address: format!("{}", log.address()),
        topics: log.topics().iter().map(|t| format!("{t}")).collect(),
        data: log.data().data.to_vec(),
        block_number: log.block_number.unwrap_or(0),
        tx_hash: log.transaction_hash.map(|h| format!("{h}")).unwrap_or_default(),
        log_index: log.log_index.unwrap_or(0) as u32,
    }
}
