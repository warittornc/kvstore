use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub address: String,
    pub shard_amount: usize,
}
