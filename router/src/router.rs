use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug)]
pub struct Router {
    nodes: Vec<String>,
}

impl Router {
    pub fn new(nodes: Vec<String>) -> Result<Self, anyhow::Error> {
        if nodes.is_empty() {
            return Err(anyhow::anyhow!("at least one node is required"));
        }
        Ok(Self { nodes })
    }

    // Returns the base URL of the node that owns the given key.
    pub fn route(&self, key: &str) -> &str {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.nodes.len();
        &self.nodes[index]
    }

    // Returns all node addresses.
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_is_deterministic() {
        let router = Router::new(vec![
            "http://127.0.0.1:7001".into(),
            "http://127.0.0.1:7002".into(),
            "http://127.0.0.1:7003".into(),
        ])
        .unwrap();
        let node1 = router.route("user:42");
        let node2 = router.route("user:42");
        assert_eq!(node1, node2);
    }

    #[test]
    fn different_keys_can_route_to_different_nodes() {
        let router = Router::new(vec![
            "http://127.0.0.1:7001".into(),
            "http://127.0.0.1:7002".into(),
            "http://127.0.0.1:7003".into(),
        ])
        .unwrap();
        // With enough keys, at least two should map to different nodes
        let mut seen = std::collections::HashSet::new();
        for i in 0..100 {
            seen.insert(router.route(&format!("key:{i}")).to_string());
        }
        assert!(seen.len() > 1, "keys should spread across multiple nodes");
    }
}
