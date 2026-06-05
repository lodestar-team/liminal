//! A tiny SSE dashboard. The HTML is loaded at runtime from the path in the
//! manifest — the host bundles nothing pipeline-specific.

use axum::{
    extract::State,
    response::sse::{Event, Sse},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tracing::info;

#[derive(Clone)]
struct AppState {
    html: Arc<String>,
    tx: broadcast::Sender<String>,
}

/// Serve the dashboard on `port`, streaming every terminal-node output line to
/// connected browsers over Server-Sent Events.
pub async fn serve(port: u16, html: String, tx: broadcast::Sender<String>) {
    let state = AppState { html: Arc::new(html), tx };
    let app = Router::new()
        .route("/", get(index))
        .route("/events", get(sse_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("dashboard bind to {addr} failed: {e}");
            return;
        }
    };
    info!(url = format!("http://localhost:{port}"), "dashboard listening");
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("dashboard serve error: {e}");
    }
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    Html((*state.html).clone())
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|r| r.ok().map(|data| Ok(Event::default().data(data))));
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}
