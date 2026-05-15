use anyhow::Result;
use alloy::{
    primitives::{FixedBytes, keccak256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::eth::{Filter, Log},
};
use futures_util::StreamExt;
use tracing::debug;

/// Wraps an EVM WebSocket subscription and yields raw logs.
pub struct EvmSource {
    inner: Box<dyn futures_util::Stream<Item = Result<Log, alloy::transports::TransportError>> + Unpin + Send>,
}

impl EvmSource {
    pub async fn connect(rpc_url: &str, topic0_hex: &str) -> Result<Self> {
        let ws = WsConnect::new(rpc_url);
        let provider = ProviderBuilder::new().on_ws(ws).await?;

        let topic0: FixedBytes<32> = topic0_hex
            .trim_start_matches("0x")
            .parse()
            .expect("invalid topic0 hex");

        let filter = Filter::new().event_signature(topic0);
        let sub = provider.subscribe_logs(&filter).await?;

        Ok(Self { inner: Box::new(sub.into_stream()) })
    }

    /// Returns the next raw log, or None if the stream ended.
    pub async fn next(&mut self) -> Option<Result<Log>> {
        self.inner.next().await.map(|r| r.map_err(anyhow::Error::from))
    }
}
