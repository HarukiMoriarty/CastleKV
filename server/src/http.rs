use axum::{http::header, response::IntoResponse, routing::get, Router};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

async fn metrics() -> impl IntoResponse {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metrics = prometheus::gather();

    // Encode metrics to buffer, handle errors gracefully
    if let Err(e) = encoder.encode(&metrics, &mut buffer) {
        error!("Error encoding metrics: {}", e);
        return (
            [(header::CONTENT_TYPE, "text/plain".to_string())],
            "Error encoding metrics".to_string().into_bytes(),
        );
    }

    (
        [(header::CONTENT_TYPE, encoder.format_type().to_string())],
        buffer,
    )
}

// Start the HTTP server
pub async fn start_metrics_server(addr: SocketAddr) -> Result<(), anyhow::Error> {
    let app = Router::new().route("/metrics", get(metrics));

    // Bind to the address
    let listener = TcpListener::bind(addr).await?;
    info!(
        "Metrics HTTP server listening on {}",
        listener.local_addr()?
    );

    // Run the server
    if let Err(err) = axum::serve(listener, app).await {
        error!("Metrics server error: {}", err);
        return Err(err.into());
    }

    info!("Metrics HTTP server stopped");
    Ok(())
}
