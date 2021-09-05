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
    #[error("Key not found")]
    /// Removing non-existent key error, key = [`key`]}.
    KeyNotFound,
    #[error("Unknown command type")]
    /// unknown command type
    UnexpectedCommandType,
    #[error("{}", _0)]
    /// Error with a string message
    StringError(String),
    #[error("UTF-8 error.")]
    /// Key or value is invalid UTF-8 sequence
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Sled error.")]
    /// Sled error
    Sled(#[from] sled::Error),
}

/// A specialized [`Result`] type for kvs operations.
pub type Result<T> = std::result::Result<T, KvsError>;
