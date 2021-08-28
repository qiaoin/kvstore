use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
/// define custome error - KvsError
pub enum KvsError {
    #[error("IO error.")]
    /// IO error.
    Io(#[from] io::Error),
    #[error("Serialization or deserialization error.")]
    /// Serialization or deserialization error.
    Serde(#[from] serde_json::Error),
    #[error("Removing non-existent key error.")]
    /// Removing non-existent key error, key = [`key`]}.
    KeyNotFound,
    #[error("unknown command type")]
    /// unknown command type
    UnexpectedCommandType,
}

/// A specialized [`Result`] type for kvs operations.
pub type Result<T> = std::result::Result<T, KvsError>;
