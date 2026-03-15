use crate::handlers;
use crate::store::KvStore;
use anyhow::Context;
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer,
};
use tracing::Level;

#[derive(Clone)]
pub struct AppState {
    pub store: KvStore<String>,
}

pub struct Server {
    store: KvStore<String>,
}

impl Server {
    pub fn new(shard_amount: usize) -> Result<Self, anyhow::Error> {
        let store: KvStore<String> = KvStore::new(shard_amount)?;
        Ok(Self { store })
    }

    pub async fn serve(&self, listener: TcpListener) -> Result<(), anyhow::Error> {
        // Setup app state
        let app_state = AppState {
            store: self.store.clone(),
        };

        // Setup router
        let app = Router::new()
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

        // Serve
        axum::serve(listener, app)
            .await
            .with_context(|| "Failed to serve")?;
        Ok(())
    }
}
