use kvstore::Config;
use kvstore::Server;
use tracing::info;

#[tokio::main]
async fn main() {
    // Setup logging
    tracing_subscriber::fmt::init();

    // Setup configuration
    let config = envy::from_env::<Config>().expect("Failed to parse configuration");

    // setup listener
    let listener = tokio::net::TcpListener::bind(config.address)
        .await
        .expect("Failed to bind listener");

    info!("Listening on {}", listener.local_addr().unwrap());

    // Start the server
    let server = Server::new(config.shard_amount).expect("Failed to create server");
    server
        .serve(listener)
        .await
        .expect("Failed to serve server");
}
