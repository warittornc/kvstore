use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub address: String,

    // Comma-separated list of backend node addresses, e.g. "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
    pub nodes: String,
}
