use kvstore_router::{Config, Server};
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = envy::from_env::<Config>().expect("Failed to parse configuration");

    let nodes: Vec<String> = config.nodes.split(',').map(|n| n.to_owned()).collect();

    if nodes.is_empty() {
        panic!("NODES env var must contain at least one node address (comma-separated)");
    }

    info!("Configured nodes: {:?}", nodes);

    let listener = tokio::net::TcpListener::bind(&config.address)
        .await
        .expect("Failed to bind listener");

    info!("Router listening on {}", listener.local_addr().unwrap());

    let server = Server::new(nodes).expect("Failed to create server");
    server.serve(listener).await.expect("Failed to serve");
}
