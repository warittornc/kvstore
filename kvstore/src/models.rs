use serde::{Deserialize, Serialize};
use serde_json::Value;

// Common
#[derive(Deserialize, Serialize)]
pub struct KeyValueResponse {
    pub key: String,
    pub value: Value,
    pub version: u64,
}

// GET /kv/{key}
pub type GetKvResponse = KeyValueResponse;

// PUT /kv/{key}
pub type PutKvRequestBody = Value;

#[derive(Deserialize)]
pub struct PutKvQueryParams {
    #[serde(rename = "ifVersion")]
    pub version: Option<u64>,
}

pub type PutKvResponse = KeyValueResponse;

// Patch /kv/{key}
pub type PatchKvRequestBody = Value;

#[derive(Deserialize)]
pub struct PatchKvQueryParams {
    #[serde(rename = "ifVersion")]
    pub version: Option<u64>,
}

pub type PatchKvResponse = KeyValueResponse;
