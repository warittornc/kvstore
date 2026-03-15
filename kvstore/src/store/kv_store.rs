use dashmap::{DashMap, Entry};
use serde_json::Value;
use std::hash::Hash;
use std::sync::Arc;

use crate::store::Item;
use crate::store::StoreError;

#[derive(Clone, Debug)]
pub struct KvStore<K: Hash + Eq + Clone> {
    // Use Dashmap as it's thread-safe and supports sharding.
    map: Arc<DashMap<K, Item>>,
}

impl<K: Hash + Eq + Clone> KvStore<K> {
    pub fn new(shard_amount: usize) -> Result<Self, StoreError> {
        if shard_amount < 2 || (shard_amount & (shard_amount - 1)) != 0 {
            return Err(StoreError::InvalidShardAmount(shard_amount));
        }

        Ok(Self {
            map: Arc::new(DashMap::with_shard_amount(shard_amount)),
        })
    }

    pub async fn keys(&self) -> Vec<K> {
        self.map.iter().map(|entry| entry.key().clone()).collect()
    }

    pub async fn get(&self, key: &K) -> Option<Item> {
        self.map.get(key).map(|entry| entry.clone())
    }

    pub async fn set(
        &self,
        key: K,
        value: Value,
        version: Option<u64>,
    ) -> Result<Option<Item>, anyhow::Error> {
        self.mutate(
            key,
            value,
            |item, value| {
                // Overwrite the entire value
                item.value = value;
                // Increment the version
                item.version += 1;

                Ok(())
            },
            version,
        )
        .await
    }

    pub async fn merge(
        &self,
        key: K,
        value: Value,
        version: Option<u64>,
    ) -> Result<Option<Item>, anyhow::Error> {
        self.mutate(
            key,
            value,
            |item, value| {
                match (&mut item.value, value) {
                    // If the values are both objects, extend the current object with the new object
                    (Value::Object(curr), Value::Object(new)) => {
                        curr.extend(new);
                    }
                    // // If the values are both numbers, add them together
                    // (curr @ Value::Number(_), new @ Value::Number(_)) => {
                    //     // if the numbers are both integers, add them together
                    //     let curr_decimal = from_value::<Decimal>(curr.clone())?;
                    //     let new_decimal = from_value::<Decimal>(new)?;
                    //     let result = curr_decimal + new_decimal;
                    //     item.value = to_value(result)?;
                    // }
                    // Otherwise, overwrite the entire value
                    (_, v) => {
                        item.value = v;
                    }
                }
                item.version += 1;
                Ok(())
            },
            version,
        )
        .await
    }

    async fn mutate(
        &self,
        key: K,
        value: Value,
        func: fn(&mut Item, Value) -> Result<(), anyhow::Error>,
        version: Option<u64>,
    ) -> Result<Option<Item>, anyhow::Error> {
        match self.map.entry(key) {
            Entry::Occupied(mut o) => {
                let item = o.get_mut();
                version
                    .is_none_or(|v| item.version == v)
                    .then(|| -> Result<_, _> {
                        func(item, value)?;
                        Ok(item.clone())
                    })
                    .transpose()
            }
            Entry::Vacant(v) => version
                .is_none()
                .then(|| {
                    let item = Item { value, version: 1 };
                    v.insert(item.clone());
                    Ok(item)
                })
                .transpose(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::task::JoinSet;

    fn new_store() -> KvStore<String> {
        KvStore::new(4).unwrap()
    }

    // Shard count must be a power of two (DashMap requirement).
    // Verify that invalid values like 0, 1, 3, and 5 are rejected with an error.
    #[test]
    fn new_rejects_non_power_of_two() {
        assert!(KvStore::<String>::new(0).is_err());
        assert!(KvStore::<String>::new(1).is_err());
        assert!(KvStore::<String>::new(3).is_err());
        assert!(KvStore::<String>::new(5).is_err());
    }

    // Verify that valid powers of two should construct a store successfully.
    #[test]
    fn new_accepts_power_of_two() {
        assert!(KvStore::<String>::new(2).is_ok());
        assert!(KvStore::<String>::new(4).is_ok());
        assert!(KvStore::<String>::new(16).is_ok());
    }

    // Verify that reading a key that was never inserted should return None, not panic.
    #[tokio::test]
    async fn get_missing_key_returns_none() {
        let store = new_store();
        assert!(store.get(&"missing".into()).await.is_none());
    }

    // Verify that setting a key for the first time should insert it with
    // version 1 and return the newly created item.
    #[tokio::test]
    async fn set_inserts_new_key() {
        let store = new_store();
        let item = store.set("k".into(), json!(42), None).await.unwrap();
        let item = item.unwrap();
        assert_eq!(item.value, json!(42));
        assert_eq!(item.version, 1);
    }

    // Verify that setting an existing key without a version guard should overwrite
    // the value and bump the version from 1 to 2.
    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let store = new_store();
        store.set("k".into(), json!(1), None).await.unwrap();
        let item = store
            .set("k".into(), json!(2), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!(2));
        assert_eq!(item.version, 2);
    }

    // Verify that after a set, a subsequent get on the same key should reflect the
    // latest value and its version.
    #[tokio::test]
    async fn get_returns_latest_value() {
        let store = new_store();
        store.set("k".into(), json!("hello"), None).await.unwrap();
        let item = store.get(&"k".into()).await.unwrap();
        assert_eq!(item.value, json!("hello"));
        assert_eq!(item.version, 1);
    }

    // Verify that keys() should return every key currently in the store, regardless
    // of insertion order or shard placement.
    #[tokio::test]
    async fn keys_returns_all_keys() {
        let store = new_store();
        store.set("a".into(), json!(1), None).await.unwrap();
        store.set("b".into(), json!(2), None).await.unwrap();
        let mut keys = store.keys().await;
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    }

    // Verify that a set with a version that matches the item's current version should
    // succeed—this is the "happy path" for compare-and-swap.
    #[tokio::test]
    async fn set_with_matching_version_succeeds() {
        let store = new_store();
        store.set("k".into(), json!(1), None).await.unwrap();
        let item = store.set("k".into(), json!(2), Some(1)).await.unwrap();
        assert!(item.is_some());
        assert_eq!(item.unwrap().version, 2);
    }

    // Verify that a set with a stale version that doesn't match the current version should
    // be no-op, return none and leave the existing value untouched.
    #[tokio::test]
    async fn set_with_stale_version_returns_none() {
        let store = new_store();
        store.set("k".into(), json!(1), None).await.unwrap();
        store.set("k".into(), json!(2), None).await.unwrap(); // version is now 2
        let result = store.set("k".into(), json!(3), Some(1)).await.unwrap();
        assert!(result.is_none(), "stale version should not mutate");
        assert_eq!(store.get(&"k".into()).await.unwrap().value, json!(2));
    }

    // Verify that trying to set a key that doesn't exist yet while providing a version should fail
    #[tokio::test]
    async fn set_with_version_on_vacant_key_returns_none() {
        let store = new_store();
        let result = store.set("k".into(), json!(1), Some(1)).await.unwrap();
        assert!(result.is_none(), "cannot insert with a version requirement");
        assert!(store.get(&"k".into()).await.is_none());
    }

    // Verify that merging two JSON objects should combine their fields. New fields from
    // the incoming object are added to the existing one.
    #[tokio::test]
    async fn merge_objects_extends() {
        let store = new_store();
        store.set("k".into(), json!({"a": 1}), None).await.unwrap();
        let item = store
            .merge("k".into(), json!({"b": 2}), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!({"a": 1, "b": 2}));
        assert_eq!(item.version, 2);
    }

    // Verify that when merging objects with overlapping keys, the incoming value should
    // overwrite the existing field while preserving non overlapping fields.
    #[tokio::test]
    async fn merge_objects_overwrites_existing_fields() {
        let store = new_store();
        store
            .set("k".into(), json!({"a": 1, "b": 2}), None)
            .await
            .unwrap();
        let item = store
            .merge("k".into(), json!({"b": 99}), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!({"a": 1, "b": 99}));
    }

    // Verify that merging two numbers overwrites the existing value (number
    // addition is not currently enabled).
    #[tokio::test]
    async fn merge_numbers_overwrites() {
        let store = new_store();
        store.set("k".into(), json!(10), None).await.unwrap();
        let item = store
            .merge("k".into(), json!(5), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!(5));
        assert_eq!(item.version, 2);
    }

    // Verify that merging floats also falls through to overwrite.
    #[tokio::test]
    async fn merge_float_numbers_overwrites() {
        let store = new_store();
        store.set("k".into(), json!(1.5), None).await.unwrap();
        let item = store
            .merge("k".into(), json!(2.5), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!(2.5));
        assert_eq!(item.version, 2);
    }

    // Verify that when the existing value and the merge value have incompatible types full overwrite occurs.
    #[tokio::test]
    async fn merge_mismatched_types_overwrites() {
        let store = new_store();
        store.set("k".into(), json!({"a": 1}), None).await.unwrap();
        let item = store
            .merge("k".into(), json!("overwrite"), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!("overwrite"));
    }

    // Verify that merging into a key that doesn't exist yet should behave like an insert and
    // create a new item with version 1.
    #[tokio::test]
    async fn merge_on_vacant_key_inserts() {
        let store = new_store();
        let item = store
            .merge("k".into(), json!(42), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.value, json!(42));
        assert_eq!(item.version, 1);
    }

    // Verify that merge respects the same optimistic locking as set and a stale version
    // causes the merge to be skipped entirely.
    #[tokio::test]
    async fn merge_with_stale_version_returns_none() {
        let store = new_store();
        store.set("k".into(), json!(1), None).await.unwrap();
        store.set("k".into(), json!(2), None).await.unwrap(); // version 2
        let result = store.merge("k".into(), json!(10), Some(1)).await.unwrap();
        assert!(result.is_none());
        assert_eq!(store.get(&"k".into()).await.unwrap().value, json!(2));
    }

    // Verify that 100 concurrent tasks all set the same key without version guards
    // and the final version must be 100.
    #[tokio::test]
    async fn concurrent_sets_on_same_key() {
        let store = Arc::new(new_store());
        let mut set = JoinSet::new();

        for i in 0..100 {
            let store = store.clone();
            set.spawn(async move {
                store.set("counter".into(), json!(i), None).await.unwrap();
            });
        }

        while set.join_next().await.is_some() {}

        let item = store.get(&"counter".into()).await.unwrap();
        assert_eq!(item.version, 100);
    }

    // Verify that 100 concurrent tasks each write to a unique key and no contention occurs
    // so every key should exist at version 1 with the correct value.
    #[tokio::test]
    async fn concurrent_sets_on_different_keys() {
        let store = Arc::new(new_store());
        let mut set = JoinSet::new();

        for i in 0..100 {
            let store = store.clone();
            set.spawn(async move {
                store.set(format!("key_{i}"), json!(i), None).await.unwrap();
            });
        }

        while set.join_next().await.is_some() {}

        let keys = store.keys().await;
        assert_eq!(keys.len(), 100);
        for i in 0..100 {
            let item = store.get(&format!("key_{i}")).await.unwrap();
            assert_eq!(item.value, json!(i));
            assert_eq!(item.version, 1);
        }
    }

    // Verify that 100 concurrent merges on the same key all complete without
    // data races. Since number merge is an overwrite, the final value will be
    // 1 (every writer wrote the same value). The version must be 101 (1 initial
    // set + 100 merges), proving every write was serialised.
    #[tokio::test]
    async fn concurrent_merges_on_same_key() {
        let store = Arc::new(new_store());
        store.set("sum".into(), json!(0), None).await.unwrap();

        let mut set = JoinSet::new();
        for _ in 0..100 {
            let store = store.clone();
            set.spawn(async move {
                store.merge("sum".into(), json!(1), None).await.unwrap();
            });
        }

        while set.join_next().await.is_some() {}

        let item = store.get(&"sum".into()).await.unwrap();
        assert_eq!(item.value, json!(1));
        assert_eq!(item.version, 101); // 1 from initial set + 100 merges
    }

    // Verify that 10 concurrent tasks all attempt a compare-and-swap at version 1 and
    // only one task holds the entry lock at a time and the first writer bumps version to 2
    // and all subsequent writers see a version mismatch. Exactly one should succeed.
    #[tokio::test]
    async fn concurrent_optimistic_locking_only_one_wins() {
        let store = Arc::new(new_store());
        store.set("k".into(), json!("init"), None).await.unwrap();

        let mut set = JoinSet::new();
        for i in 0..10 {
            let store = store.clone();
            set.spawn(async move {
                store
                    .set("k".into(), json!(format!("writer_{i}")), Some(1))
                    .await
                    .unwrap()
            });
        }

        let mut successes = 0;
        while let Some(result) = set.join_next().await {
            if result.unwrap().is_some() {
                successes += 1;
            }
        }

        assert_eq!(successes, 1);
        let item = store.get(&"k".into()).await.unwrap();
        assert_eq!(item.version, 2);
    }

    // Verify that 50 writers and 50 readers run concurrently on the same key.
    // Readers must never observe a missing key or a non-numeric value. Since
    // number merge is an overwrite, the final value will be 1 (all writers
    // wrote the same value). Version must be 51 (1 set + 50 merges).
    #[tokio::test]
    async fn concurrent_mixed_reads_and_writes() {
        let store = Arc::new(new_store());
        store.set("k".into(), json!(0), None).await.unwrap();

        let mut set = JoinSet::new();

        for _ in 0..50 {
            let store = store.clone();
            set.spawn(async move {
                store.merge("k".into(), json!(1), None).await.unwrap();
            });
        }

        for _ in 0..50 {
            let store = store.clone();
            set.spawn(async move {
                let item = store.get(&"k".into()).await;
                assert!(item.is_some());
                assert!(item.unwrap().value.is_number());
            });
        }

        while set.join_next().await.is_some() {}

        let item = store.get(&"k".into()).await.unwrap();
        assert_eq!(item.value, json!(1));
        assert_eq!(item.version, 51); // 1 from initial set + 50 merges
    }
}
