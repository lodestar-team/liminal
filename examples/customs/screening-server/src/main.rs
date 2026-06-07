//! Local sanctions-screening server — a stand-in for a commercial provider.
//!
//!   GET /screen?address=0x... → {"sanctioned": bool, "list_version": "..."}
//!   GET /healthz              → "ok"
//!
//! The list is loaded from a JSON file (default `fixtures/sanctioned.json`):
//!   { "list_version": "...", "sanctioned": ["0x...", ...] }
//!
//! Swapping in a real provider is a one-line `allow_origins` change in the
//! manifest plus the provider's auth — no component code changes.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct ListFile {
    list_version: String,
    sanctioned: Vec<String>,
}

struct AppState {
    list_version: String,
    sanctioned: HashSet<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::var("SANCTIONED_LIST")
        .unwrap_or_else(|_| "examples/customs/fixtures/sanctioned.json".to_string());
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8088);

    let raw = std::fs::read_to_string(&path).with_context(|| format!("reading list {path}"))?;
    let list: ListFile = serde_json::from_str(&raw).context("parsing sanctioned list")?;
    let state = Arc::new(AppState {
        list_version: list.list_version,
        sanctioned: list.sanctioned.into_iter().map(|a| a.to_lowercase()).collect(),
    });

    eprintln!(
        "screening-server: {} sanctioned addresses (list {}), listening on :{port}",
        state.sanctioned.len(),
        state.list_version
    );

    let app = Router::new()
        .route("/screen", get(screen))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize)]
struct ScreenQuery {
    address: String,
}

async fn screen(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ScreenQuery>,
) -> Json<serde_json::Value> {
    let sanctioned = state.sanctioned.contains(&q.address.to_lowercase());
    Json(serde_json::json!({
        "sanctioned": sanctioned,
        "list_version": state.list_version,
    }))
}
