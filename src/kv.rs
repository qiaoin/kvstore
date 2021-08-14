use std::collections::HashMap;

/// The `KvStore` used HashMap, storing in memroy, not on a disk
///
/// Example:
///
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::new();
///
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// assert_eq!(store.get("key2".to_owned()), None);
///
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), None);
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// new a KvStore, default return an empty HashMap
    pub fn new() -> Self {
        KvStore {
            store: Default::default(),
        }
    }

    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Get the string value of the a string key.
    ///
    /// If the key does not exist, return None.
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).map(|s| s.to_string())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
