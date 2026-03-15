use crate::handlers;
use crate::router::Router;
use anyhow::Context;
use axum::routing::get;
use tokio::net::TcpListener;
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer,
};
use tracing::Level;

#[derive(Clone)]
pub struct AppState {
    pub router: Router,
    pub client: reqwest::Client,
}

pub struct Server {
    router: Router,
}

impl Server {
    pub fn new(nodes: Vec<String>) -> Result<Self, anyhow::Error> {
        let router = Router::new(nodes)?;
        Ok(Self { router })
    }

    pub async fn serve(&self, listener: TcpListener) -> Result<(), anyhow::Error> {
        let app_state = AppState {
            router: self.router.clone(),
            client: reqwest::Client::new(),
        };

        let app = axum::Router::new()
            .route("/kv", get(handlers::get_kv_list))
            .route(
                "/kv/{key}",
                get(handlers::get_kv)
                    .put(handlers::put_kv)
                    .patch(handlers::patch_kv),
            )
            .with_state(app_state)
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_request(DefaultOnRequest::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(Level::ERROR)),
            );

        axum::serve(listener, app)
            .await
            .with_context(|| "Failed to serve")?;
        Ok(())
    }
}
