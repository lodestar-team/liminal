use anyhow::{Context, Result};
use alloy::{
    primitives::{Address, FixedBytes},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::eth::{Filter, Log},
};
use futures_util::StreamExt;
use liminal_sdk::EvmLog;
use std::any::Any;
use std::collections::VecDeque;
use tracing::debug;

use crate::manifest::SourceSpec;

/// The head of a pipeline. Yields already-serialized JSON messages (bytes) so
/// the runtime stays oblivious to message shape — exactly like every edge.
pub enum Source {
    Evm(EvmSource),
    Fixture(FixtureSource),
}

impl Source {
    /// Build the source described by the manifest.
    pub async fn connect(spec: &SourceSpec) -> Result<Self> {
        match spec.kind.as_str() {
            "evm" => {
                let rpc = spec.rpc.as_deref().context("evm source missing rpc")?;
                Ok(Source::Evm(
                    EvmSource::connect(rpc, &spec.topics, &spec.addresses).await?,
                ))
            }
            "fixture" => {
                let path = spec.path.as_deref().context("fixture source missing path")?;
                Ok(Source::Fixture(FixtureSource::load(path)?))
            }
            other => anyhow::bail!("unsupported source type {other:?}"),
        }
    }

    /// Next message as JSON bytes, or `None` when the source is exhausted.
    pub async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        match self {
            Source::Evm(s) => s.next().await,
            Source::Fixture(s) => s.next(),
        }
    }
}

/// An EVM WebSocket log subscription. Emits canonical [`EvmLog`] JSON.
pub struct EvmSource {
    // Keep the provider alive — dropping it closes the WS task and ends the stream.
    _provider: Box<dyn Any + Send + Sync>,
    inner: Box<dyn futures_util::Stream<Item = Log> + Unpin + Send>,
}

impl EvmSource {
    pub async fn connect(rpc_url: &str, topic_hexes: &[String], addresses: &[String]) -> Result<Self> {
        let ws = WsConnect::new(rpc_url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let topics: Vec<FixedBytes<32>> = topic_hexes
            .iter()
            .map(|h| h.trim_start_matches("0x").parse())
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("invalid topic hex in manifest: {e}"))?;

        let mut filter = Filter::new().event_signature(topics);

        // W5: optional contract-address allow-list.
        if !addresses.is_empty() {
            let addrs: Vec<Address> = addresses
                .iter()
                .map(|a| a.parse())
                .collect::<Result<_, _>>()
                .map_err(|e| anyhow::anyhow!("invalid address in manifest: {e}"))?;
            filter = filter.address(addrs);
        }

        let sub = provider.subscribe_logs(&filter).await?;
        debug!(topics = topic_hexes.len(), addresses = addresses.len(), "subscribed to logs");
        Ok(Self { _provider: Box::new(provider), inner: Box::new(sub.into_stream()) })
    }

    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        let log = self.inner.next().await?;
        Some(serde_json::to_vec(&to_evm_log(&log)).context("serializing EvmLog"))
    }
}

/// A fixture source: newline-delimited JSON, one message per non-empty line.
/// Lets the whole pipeline run offline with no RPC, no services — the spine of
/// the Customs acceptance demo.
pub struct FixtureSource {
    lines: VecDeque<Vec<u8>>,
}

impl FixtureSource {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading fixture {path}"))?;
        let lines = raw
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| l.as_bytes().to_vec())
            .collect();
        Ok(Self { lines })
    }

    fn next(&mut self) -> Option<Result<Vec<u8>>> {
        self.lines.pop_front().map(Ok)
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
