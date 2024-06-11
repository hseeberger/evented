mod v0;

use crate::domain::AccountRepository;
use anyhow::{Context, Result};
use api_version::api_version;
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode, Uri},
    routing::get,
    Router, ServiceExt,
};
use evented::pool::Pool;
use opentelemetry::{global, propagation::Extractor, trace::TraceContextExt};
use serde::Deserialize;
use std::{convert::Infallible, net::IpAddr};
use tokio::{
    net::TcpListener,
    signal::unix::{signal, SignalKind},
};
use tower::{Layer, ServiceBuilder};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{field, info_span, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    addr: IpAddr,
    port: u16,
}

#[derive(Debug, OpenApi)]
#[openapi()]
pub struct ApiDoc;

pub async fn serve<R>(config: Config, account_repository: R, pool: Pool) -> Result<()>
where
    R: AccountRepository,
{
    let Config { addr, port } = config;

    let app_state = AppState {
        account_repository,
        pool,
    };

    let mut api_doc = ApiDoc::openapi();
    api_doc.merge(v0::ApiDoc::openapi());

    let app = Router::new()
        .route("/", get(ready))
        .nest("/v0", v0::app())
        .merge(SwaggerUi::new("/api-doc").url("/openapi.json", api_doc))
        .with_state(app_state)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http().make_span_with(make_span))
                .layer(CorsLayer::permissive())
                .map_request(accept_trace)
                .map_request(record_trace_id),
        );
    let app = api_version!(0..=0, ApiVersionFilter).layer(app);

    let listener = TcpListener::bind((addr, port))
        .await
        .context("bind TcpListener")?;
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("run server")
}

#[derive(Clone)]
struct AppState<R> {
    account_repository: R,
    pool: Pool,
}

#[derive(Clone)]
struct ApiVersionFilter;

impl api_version::ApiVersionFilter for ApiVersionFilter {
    type Error = Infallible;

    async fn filter(&self, uri: &Uri) -> Result<bool, Self::Error> {
        let path = uri.path();
        let no_rewrite = (path == "/") || path.starts_with("/api-doc") || path == "/openapi.json";
        Ok(!no_rewrite)
    }
}

async fn ready() -> StatusCode {
    StatusCode::OK
}

async fn shutdown_signal() {
    signal(SignalKind::terminate())
        .expect("install SIGTERM handler")
        .recv()
        .await;
}

fn make_span(request: &Request<Body>) -> Span {
    let headers = request.headers();
    let path = request.uri().path();
    info_span!("incoming request", path, ?headers, trace_id = field::Empty)
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| {
            let s = v.to_str();
            if let Err(ref error) = s {
                warn!(%error, ?v, "cannot convert header value to ASCII")
            };
            s.ok()
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

fn accept_trace(request: Request<Body>) -> Request<Body> {
    // Current context, if no or invalid data is received.
    let parent_context = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    Span::current().set_parent(parent_context);

    request
}

fn record_trace_id(request: Request<Body>) -> Request<Body> {
    let span = Span::current();

    let trace_id = span.context().span().span_context().trace_id();
    span.record("trace_id", trace_id.to_string());

    request
}
