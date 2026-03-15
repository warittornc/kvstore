use crate::models::{
    GetKvResponse, PatchKvQueryParams, PatchKvRequestBody, PatchKvResponse, PutKvQueryParams,
    PutKvRequestBody, PutKvResponse,
};
use crate::server::AppState;
use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use tracing::error;

pub async fn get_kv(
    // Access the state via the State extractor
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let item = state.store.get(&key).await;
    match item {
        // If the key is found, return the item with 200
        Some(item) => {
            let body = GetKvResponse {
                key,
                value: item.value,
                version: item.version,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        // If the key is not found, return 404
        None => (StatusCode::NOT_FOUND).into_response(),
    }
}

pub async fn get_kv_list(State(state): State<AppState>) -> impl IntoResponse {
    let keys = state.store.keys().await;
    (StatusCode::OK, Json(keys)).into_response()
}

pub async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<PutKvQueryParams>,
    Json(value): Json<PutKvRequestBody>,
) -> impl IntoResponse {
    match state.store.set(key.clone(), value, params.version).await {
        // If the key is set, return the item with 200
        Ok(Some(item)) => {
            let body = PutKvResponse {
                key,
                value: item.value,
                version: item.version,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        // If the key is not set, return 409
        Ok(None) => (StatusCode::CONFLICT).into_response(),
        // If there is an error, return 500
        Err(e) => {
            error!("Error settings kv: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR).into_response()
        }
    }
}

pub async fn patch_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<PatchKvQueryParams>,
    Json(value): Json<PatchKvRequestBody>,
) -> impl IntoResponse {
    match state.store.merge(key.clone(), value, params.version).await {
        // If the key is updated, return the item with 200
        Ok(Some(item)) => {
            let body = PatchKvResponse {
                key,
                value: item.value,
                version: item.version,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        // If the key is not updated, return 409
        Ok(None) => (StatusCode::CONFLICT).into_response(),
        // If there is an error, return 500
        Err(e) => {
            error!("Error merging kv: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR).into_response()
        }
    }
}
