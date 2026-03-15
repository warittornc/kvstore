use kvstore::Server;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tokio::task::JoinSet;

#[derive(Deserialize)]
struct Item {
    value: serde_json::Value,
    version: u64,
}

// Start the server on a random OS-assigned port and return the base URL.
async fn start_test_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0".to_string())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    let server = Server::new(64).unwrap();
    tokio::spawn(async move {
        server.serve(listener).await.unwrap();
    });

    format!("http://{addr}")
}

// A single client that performs `count` read-modify-write increments using
// optimistic locking (`ifVersion`). On 409 conflict it retries from the read.
async fn increment_client(base_url: String, key: &str, count: usize) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/kv/{key}");

    for _ in 0..count {
        loop {
            // Read the current value and version
            let resp = client.get(&url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let item: Item = resp.json().await.unwrap();
            let current_value = item.value.as_u64().expect("value should be a number");

            // Compute the new value on the client side
            let new_value = current_value + 1;

            // Write back with the version guard
            let resp = client
                .put(&url)
                .query(&[("ifVersion", item.version)])
                .json(&json!(new_value))
                .send()
                .await
                .unwrap();

            match resp.status() {
                // Success — move on to the next increment
                StatusCode::OK => break,
                // Conflict — another writer got there first, retry
                StatusCode::CONFLICT => continue,
                other => panic!("unexpected status: {other}"),
            }
        }
    }
}

// 3 concurrent clients each increment the same counter 100 times using
// a read-modify-write loop with optimistic locking. The final value must
// be exactly 300, proving no updates were lost.
#[tokio::test]
async fn three_clients_increment_to_300() {
    let base_url = start_test_server().await;
    let key = "counter";
    let increments_per_client = 100;
    let num_clients = 3;

    // Seed the counter to 0 so every client always sees an existing key
    // and must use ifVersion — avoids the unguarded-create race.
    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{base_url}/kv/{key}"))
        .json(&json!(0))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut set = JoinSet::new();
    for _ in 0..num_clients {
        let base_url = base_url.clone();
        set.spawn(async move {
            increment_client(base_url, key, increments_per_client).await;
        });
    }

    // Wait for all clients to finish
    while set.join_next().await.is_some() {}

    // Verify the final value
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base_url}/kv/{key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let item: Item = resp.json().await.unwrap();
    assert_eq!(
        item.value.as_u64().unwrap(),
        (num_clients * increments_per_client) as u64,
        "expected final value to be {}, got {}",
        num_clients * increments_per_client,
        item.value
    );
}
