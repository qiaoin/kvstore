#![deny(missing_docs)]
//! A simple kvstore

pub use kv::KvStore;
pub use error::{KvsError, Result};

mod error;
mod kv;
