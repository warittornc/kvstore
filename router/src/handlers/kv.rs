use crate::models::{KeyEntry, VersionQueryParams};
use crate::server::AppState;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use tokio::task::JoinSet;
use tracing::error;

pub async fn get_kv(State(state): State<AppState>, Path(key): Path<String>) -> impl IntoResponse {
    let node = state.router.route(&key);
    let url = format!("{node}/kv/{key}");

    // Send the request to the respective node and forward the response.
    match state.client.get(&url).send().await {
        Ok(resp) => proxy_response(resp).await,
        Err(e) => {
            error!("Failed to proxy GET /kv/{key}: {e}");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

pub async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<VersionQueryParams>,
    body: Body,
) -> impl IntoResponse {
    let node = state.router.route(&key);
    let url = format!("{node}/kv/{key}");

    // Read the request body.
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read request body: {e}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    // Add the version query parameter if it is present.
    let url = match params.version {
        Some(v) => format!("{url}?ifVersion={v}"),
        None => url,
    };

    // Create the request.
    let req = state
        .client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(body_bytes);

    // Send the request to the respective node and forward the response.
    match req.send().await {
        Ok(resp) => proxy_response(resp).await,
        Err(e) => {
            error!("Failed to proxy PUT /kv/{key}: {e}");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

pub async fn patch_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<VersionQueryParams>,
    body: Body,
) -> impl IntoResponse {
    let node = state.router.route(&key);
    let url = format!("{node}/kv/{key}");

    // Read the request body.
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read request body: {e}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    // Add the version query parameter if it is present.
    let url = match params.version {
        Some(v) => format!("{url}?ifVersion={v}"),
        None => url,
    };

    // Create the request.
    let req = state
        .client
        .patch(&url)
        .header("Content-Type", "application/json")
        .body(body_bytes);

    // Send the request to the respective node and forward the response.
    match req.send().await {
        Ok(resp) => proxy_response(resp).await,
        Err(e) => {
            error!("Failed to proxy PATCH /kv/{key}: {e}");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

pub async fn get_kv_list(State(state): State<AppState>) -> impl IntoResponse {
    let nodes = state.router.nodes().to_vec();

    // Create a join set to fan out the requests to all nodes.
    let mut set = JoinSet::from_iter(nodes.iter().map(|node| {
        let client = state.client.clone();
        let url = format!("{node}/kv");
        let node = node.clone();
        async move {
            let resp = client.get(&url).send().await?;
            let keys: Vec<String> = resp.json().await?;
            Ok::<(String, Vec<String>), reqwest::Error>((node, keys))
        }
    }));

    // Collect the results as its being completed from the join set and append them to the results vector.
    let mut results = Vec::with_capacity(nodes.len());
    while let Some(result) = set.join_next().await {
        match result {
            Ok(inner) => results.push(inner),
            Err(e) => {
                error!("Task failed to fetch keys: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    // Convert the results to NDJSON and return the response.
    let mut ndjson = String::new();
    for result in results {
        match result {
            Ok((node, keys)) => {
                for key in keys {
                    let entry = KeyEntry {
                        key,
                        node: node.clone(),
                    };
                    if let Ok(line) = serde_json::to_string(&entry) {
                        ndjson.push_str(&line);
                        ndjson.push('\n');
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch keys from a node: {e}");
                return (StatusCode::BAD_GATEWAY).into_response();
            }
        }
    }

    let headers = [("Content-Type", "application/x-ndjson")];
    (StatusCode::OK, headers, ndjson).into_response()
}

async fn proxy_response(resp: reqwest::Response) -> axum::response::Response {
    // Note: The status code here should always be valid as we are forwarding the response from the upstream node.
    // However, if for some reason the status code is not a valid HTTP status code, we can handle by returning a 502.
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);

    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read upstream response body: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    (status, [("Content-Type", "application/json")], body).into_response()
}
