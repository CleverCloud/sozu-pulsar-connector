use std::net::SocketAddr;

use axum::{
    http::{HeaderValue, Request, Response},
    routing::{any, get},
    Router,
};
use hyper::{Body, Server, StatusCode};
use prometheus::{Encoder, TextEncoder};
use tracing::info;

use crate::cfg::Configuration;

#[derive(thiserror::Error, Debug)]
pub enum MetricsServerError {
    #[error("failed to bind server to address {address}: {hyper_error}")]
    Bind {
        address: String,
        hyper_error: hyper::Error,
    },
    #[error("failed to serve content, {0}")]
    Serve(hyper::Error),
    #[error("Could not parse address")]
    WrongAddress(String),
}

// unlikely used
pub async fn not_found(_req: Request<Body>) -> Response<Body> {
    let mut response = Response::default();
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}

pub async fn prometheus_metrics(_req: Request<Body>) -> Response<Body> {
    let mut response = Response::default();

    let encoder = TextEncoder::new();
    let metrics = prometheus::gather();

    let mut metrics_buffer = vec![];
    match encoder.encode(&metrics, &mut metrics_buffer) {
        Ok(_) => {
            let headers = response.headers_mut();

            headers.insert(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_str(mime::TEXT_PLAIN_UTF_8.as_ref())
                    .expect("constant to be iso8859-1 compliant"),
            );

            headers.insert(
                hyper::header::CONTENT_LENGTH,
                HeaderValue::from_str(&metrics_buffer.len().to_string())
                    .expect("buffer size to be iso8859-1 compliant"),
            );

            *response.status_mut() = StatusCode::OK;
            *response.body_mut() = Body::from(metrics_buffer);
        }
        Err(err) => {
            let headers = response.headers_mut();
            let message = serde_json::json!({"error": err.to_string() }).to_string();

            headers.insert(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_str(mime::APPLICATION_JSON.as_ref())
                    .expect("constant to be iso8859-1 compliant"),
            );

            headers.insert(
                hyper::header::CONTENT_LENGTH,
                HeaderValue::from_str(&message.len().to_string())
                    .expect("buffer size to be iso8859-1 compliant"),
            );

            *response.status_mut() = StatusCode::OK;
            *response.body_mut() = Body::from(message);
        }
    }

    response
}

pub fn router() -> Router {
    Router::new()
        .route("/metrics", get(prometheus_metrics))
        .fallback(any(not_found))
    // .layer(middleware::from_fn(layer::access))
}

pub async fn serve_metrics(config: Configuration) -> Result<(), MetricsServerError> {
    let address: SocketAddr = config.metrics_address.parse().map_err(|_| {
        MetricsServerError::WrongAddress(format!(
            "Could not parse address {}",
            config.metrics_address
        ))
    })?;

    info!(addr = address.to_string(), "Begin to listen on address");

    Server::try_bind(&address)
        .map_err(|hyper_error| MetricsServerError::Bind {
            hyper_error,
            address: address.to_string(),
        })?
        .serve(router().into_make_service())
        .await
        .map_err(MetricsServerError::Serve)?;

    Ok(())
}
