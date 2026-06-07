//! Host-provided key-value store with per-component namespace scoping (W4).
//!
//! One shared backing map underlies the whole pipeline, but every key a
//! component touches is transparently prefixed with that component's declared
//! namespace. A node granted the `prices` namespace cannot read or write
//! `verdicts` or `hold` — isolation enforced by the host, invisible to the guest.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::HostState;

/// The shared backing store for an entire pipeline run. Cloned (by `Arc`) into
/// each granted component's state; the namespace prefix keeps them apart.
pub type SharedStore = Arc<Mutex<HashMap<String, Vec<u8>>>>;

pub fn new_store() -> SharedStore {
    Arc::new(Mutex::new(HashMap::new()))
}

/// A single component's view of the store: its namespace plus the shared map.
#[derive(Clone)]
pub struct KvScope {
    namespace: String,
    store: SharedStore,
}

impl KvScope {
    pub fn new(namespace: impl Into<String>, store: SharedStore) -> Self {
        Self { namespace: namespace.into(), store }
    }

    /// The actual storage key: namespace + unit-separator + caller's key. The
    /// separator can't appear in the namespace, so namespaces can't collide.
    fn scoped(&self, key: &str) -> String {
        format!("{}\u{1f}{}", self.namespace, key)
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.lock().unwrap().get(&self.scoped(key)).cloned()
    }
    pub fn set(&self, key: &str, value: Vec<u8>) {
        self.store.lock().unwrap().insert(self.scoped(key), value);
    }
    pub fn delete(&self, key: &str) {
        self.store.lock().unwrap().remove(&self.scoped(key));
    }
    pub fn exists(&self, key: &str) -> bool {
        self.store.lock().unwrap().contains_key(&self.scoped(key))
    }
}

mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "kv-provider",
    });
}

/// Add the host's key-value store to a component linker.
pub fn add_to_linker(linker: &mut wasmtime::component::Linker<HostState>) -> wasmtime::Result<()> {
    bindings::liminal::node::store::add_to_linker::<
        HostState,
        wasmtime::component::HasSelf<HostState>,
    >(linker, |s: &mut HostState| s)
}

/// The host implementation of `liminal:node/store`. A component reaching this
/// without a granted scope is a wiring bug, so we treat a missing scope as an
/// empty store rather than panicking.
impl bindings::liminal::node::store::Host for HostState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        self.kv.as_ref().and_then(|s| s.get(&key))
    }
    fn set(&mut self, key: String, value: Vec<u8>) {
        if let Some(s) = &self.kv {
            s.set(&key, value);
        }
    }
    fn delete(&mut self, key: String) {
        if let Some(s) = &self.kv {
            s.delete(&key);
        }
    }
    fn exists(&mut self, key: String) -> bool {
        self.kv.as_ref().is_some_and(|s| s.exists(&key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_isolated() {
        let store = new_store();
        let prices = KvScope::new("prices", store.clone());
        let verdicts = KvScope::new("verdicts", store.clone());

        prices.set("WETH", b"3000".to_vec());
        verdicts.set("0xbad", b"flagged".to_vec());

        // Each sees only its own namespace.
        assert_eq!(prices.get("WETH"), Some(b"3000".to_vec()));
        assert_eq!(verdicts.get("0xbad"), Some(b"flagged".to_vec()));

        // Cross-namespace reads miss, even for identical keys.
        assert_eq!(prices.get("0xbad"), None, "prices must not see the verdicts namespace");
        assert_eq!(verdicts.get("WETH"), None, "verdicts must not see the prices namespace");

        // Same key in two namespaces is two distinct cells.
        prices.set("k", b"a".to_vec());
        verdicts.set("k", b"b".to_vec());
        assert_eq!(prices.get("k"), Some(b"a".to_vec()));
        assert_eq!(verdicts.get("k"), Some(b"b".to_vec()));
    }
}
