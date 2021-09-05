use super::KvsEngine;
use crate::{KvsError, Result};

use sled::{Db, Tree};
use std::fs;
use std::path::PathBuf;

/// sled engine
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// open sled engine
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let db = sled::open(&path)?;

        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.db;
        // 这里感觉 map 将 Option<IVec> 映射为 ()，感觉没啥用
        // tree.insert(key, value.into_bytes())?;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.db;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}
