use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Shard amount must be a power of 2: {0}")]
    InvalidShardAmount(usize),
}
