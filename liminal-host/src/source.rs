use anyhow::Result;
use alloy::{
    primitives::FixedBytes,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::eth::{Filter, Log},
};
use futures_util::StreamExt;
use std::any::Any;
use tracing::debug;

/// Wraps an EVM WebSocket log subscription and yields raw logs.
pub struct EvmSource {
    // Keep the provider alive — dropping it closes the WS task and ends the stream.
    _provider: Box<dyn Any + Send + Sync>,
    inner: Box<dyn futures_util::Stream<Item = Log> + Unpin + Send>,
}

impl EvmSource {
    pub async fn connect(rpc_url: &str, topic_hexes: &[&str]) -> Result<Self> {
        let ws = WsConnect::new(rpc_url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let topics: Vec<FixedBytes<32>> = topic_hexes
            .iter()
            .map(|h| h.trim_start_matches("0x").parse().expect("invalid topic hex"))
            .collect();

        let filter = Filter::new().event_signature(topics);
        let sub = provider.subscribe_logs(&filter).await?;

        debug!("subscribed to logs");
        Ok(Self { _provider: Box::new(provider), inner: Box::new(sub.into_stream()) })
    }

    pub async fn next(&mut self) -> Option<Result<Log>> {
        self.inner.next().await.map(Ok)
    }
}
