#![deny(missing_docs)]
//! A simple kvstore

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;
