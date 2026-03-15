use kvstore::Server as KvServer;
use kvstore_router::Server as RouterServer;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Debug)]
struct Item {
    value: serde_json::Value,
    version: u64,
}

#[derive(Deserialize, Debug)]
struct KeyEntry {
    key: String,
    node: String,
}

// Start a kvstore node on a random port and return its base URL.
async fn start_kv_node() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = KvServer::new(64).unwrap();
    tokio::spawn(async move { server.serve(listener).await.unwrap() });
    format!("http://{addr}")
}

// Start a router pointing at the given kvstore nodes and return its base URL.
async fn start_router(nodes: Vec<String>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = RouterServer::new(nodes).unwrap();
    tokio::spawn(async move { server.serve(listener).await.unwrap() });
    format!("http://{addr}")
}

// Spin up 3 kvstore nodes + 1 router and return (router_url, [node_urls]).
async fn setup() -> (String, Vec<String>) {
    let nodes = vec![
        start_kv_node().await,
        start_kv_node().await,
        start_kv_node().await,
    ];
    let router_url = start_router(nodes.clone()).await;
    (router_url, nodes)
}

#[tokio::test]
async fn put_and_get_single_key() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    // PUT a value through the router.
    let resp = client
        .put(format!("{router}/kv/greeting"))
        .json(&json!("hello"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!("hello"));
    assert_eq!(item.version, 1);

    // GET it back through the router.
    let resp = client
        .get(format!("{router}/kv/greeting"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!("hello"));
    assert_eq!(item.version, 1);
}

#[tokio::test]
async fn put_overwrites_previous_value() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    client
        .put(format!("{router}/kv/color"))
        .json(&json!("red"))
        .send()
        .await
        .unwrap();

    let resp = client
        .put(format!("{router}/kv/color"))
        .json(&json!("blue"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!("blue"));
    assert_eq!(item.version, 2);
}

#[tokio::test]
async fn get_missing_key_returns_404() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    let resp = client
        .get(format!("{router}/kv/does_not_exist"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_with_correct_version_succeeds() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    // Create the key (version 1).
    client
        .put(format!("{router}/kv/locked"))
        .json(&json!(1))
        .send()
        .await
        .unwrap();

    // Update with matching version.
    let resp = client
        .put(format!("{router}/kv/locked"))
        .query(&[("ifVersion", "1")])
        .json(&json!(2))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!(2));
    assert_eq!(item.version, 2);
}

#[tokio::test]
async fn put_with_wrong_version_returns_409() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    client
        .put(format!("{router}/kv/locked"))
        .json(&json!(1))
        .send()
        .await
        .unwrap();

    // Try to update with a stale version.
    let resp = client
        .put(format!("{router}/kv/locked"))
        .query(&[("ifVersion", "999")])
        .json(&json!(2))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn patch_merges_json_objects() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    // Create an object.
    client
        .put(format!("{router}/kv/profile"))
        .json(&json!({"name": "Alice"}))
        .send()
        .await
        .unwrap();

    // Merge in a new field.
    let resp = client
        .patch(format!("{router}/kv/profile"))
        .json(&json!({"age": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!({"name": "Alice", "age": 30}));
    assert_eq!(item.version, 2);
}

#[tokio::test]
async fn patch_with_wrong_version_returns_409() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    client
        .put(format!("{router}/kv/profile"))
        .json(&json!({"name": "Alice"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .patch(format!("{router}/kv/profile"))
        .query(&[("ifVersion", "999")])
        .json(&json!({"age": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn list_keys_returns_ndjson_with_all_keys() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    // Insert several keys — they will hash to different nodes.
    let keys = ["alpha", "beta", "gamma", "delta", "epsilon"];
    for key in &keys {
        client
            .put(format!("{router}/kv/{key}"))
            .json(&json!(key))
            .send()
            .await
            .unwrap();
    }

    // List all keys via the router.
    let resp = client.get(format!("{router}/kv")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/x-ndjson"
    );

    let body = resp.text().await.unwrap();
    let entries: Vec<KeyEntry> = body
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    // All inserted keys must appear.
    let mut returned_keys: Vec<String> = entries.iter().map(|e| e.key.clone()).collect();
    returned_keys.sort();
    let mut expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    expected.sort();
    assert_eq!(returned_keys, expected);

    // Each entry should reference a valid node.
    for entry in &entries {
        assert!(
            _nodes.contains(&entry.node),
            "entry node {} not in nodes {:?}",
            entry.node,
            _nodes
        );
    }
}

#[tokio::test]
async fn list_keys_empty_store_returns_empty_body() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    let resp = client.get(format!("{router}/kv")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.trim().is_empty());
}

#[tokio::test]
async fn same_key_always_hits_same_node() {
    let (router, _nodes) = setup().await;
    let client = Client::new();

    // Write and read the same key multiple times — value must always be the latest.
    for i in 0..5u64 {
        client
            .put(format!("{router}/kv/stable"))
            .json(&json!(i))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .get(format!("{router}/kv/stable"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let item: Item = resp.json().await.unwrap();
    assert_eq!(item.value, json!(4));
    assert_eq!(item.version, 5);
}

#[tokio::test]
async fn concurrent_increments_through_router() {
    let (router, _nodes) = setup().await;
    let client = Client::new();
    let key = "counter";

    // Seed the counter.
    let resp = client
        .put(format!("{router}/kv/{key}"))
        .json(&json!(0))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let num_clients = 3;
    let increments_per_client = 50;

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..num_clients {
        let base = router.clone();
        set.spawn(async move {
            let c = Client::new();
            let url = format!("{base}/kv/{key}");
            for _ in 0..increments_per_client {
                loop {
                    let resp = c.get(&url).send().await.unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let item: Item = resp.json().await.unwrap();
                    let new_val = item.value.as_u64().unwrap() + 1;

                    let resp = c
                        .put(&url)
                        .query(&[("ifVersion", item.version.to_string())])
                        .json(&json!(new_val))
                        .send()
                        .await
                        .unwrap();
                    match resp.status() {
                        StatusCode::OK => break,
                        StatusCode::CONFLICT => continue,
                        other => panic!("unexpected status: {other}"),
                    }
                }
            }
        });
    }

    while set.join_next().await.is_some() {}

    let resp = client
        .get(format!("{router}/kv/{key}"))
        .send()
        .await
        .unwrap();
    let item: Item = resp.json().await.unwrap();
    assert_eq!(
        item.value.as_u64().unwrap(),
        (num_clients * increments_per_client) as u64
    );
}
