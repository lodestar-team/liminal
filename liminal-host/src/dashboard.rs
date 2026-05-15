use axum::{
    Router,
    response::{Html, IntoResponse},
    routing::get,
    extract::State,
};
use axum::response::sse::{Event, Sse};
use std::convert::Infallible;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tracing::info;

const DASHBOARD_HTML: &str = include_str!("../../examples/cross-dex-arb/dashboard/index.html");

pub async fn serve(port: u16, sse_tx: broadcast::Sender<String>) {
    let app = Router::new()
        .route("/", get(index))
        .route("/events", get(sse_handler))
        .with_state(sse_tx);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await
        .expect("dashboard bind failed");
    info!(url = format!("http://localhost:{port}"), "dashboard listening");
    axum::serve(listener, app).await.expect("dashboard serve failed");
}

async fn index() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn sse_handler(
    State(tx): State<broadcast::Sender<String>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| {
        r.ok().map(|data| Ok(Event::default().data(data)))
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}
