use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct VersionQueryParams {
    #[serde(rename = "ifVersion")]
    pub version: Option<u64>,
}

// NDJSON entry for the list-all-keys response.
#[derive(Serialize)]
pub struct KeyEntry {
    pub key: String,
    pub node: String,
}
