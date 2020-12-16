use crate::crypto::PublicKey;
use crate::store::StoreCommand;
use ed25519_dalek::ed25519;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[macro_export]
macro_rules! diem_bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! diem_ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            diem_bail!($e);
        }
    };
}

#[derive(Error, Debug, Serialize)]
pub enum DiemError {
    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Received an unexpected or late vote from {0:?}")]
    UnexpectedOrLateVote(PublicKey),

    #[error("Received more than one vote from {0:?}")]
    AuthorityReuse(PublicKey),

    #[error("Received vote from unknown authority {0:?}")]
    UnknownAuthority(PublicKey),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Serialization error. {0}")]
    SerializationError(String),

    #[error("Network error. {0}")]
    NetworkError(String),

    #[error("Store error. {0}")]
    StoreError(String),

    #[error("Channel error. {0}")]
    ChannelError(String),
}

impl From<ed25519::Error> for DiemError {
    fn from(_e: ed25519::Error) -> Self {
        DiemError::InvalidSignature
    }
}

impl From<Box<bincode::ErrorKind>> for DiemError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        DiemError::SerializationError(e.to_string())
    }
}

impl From<std::io::Error> for DiemError {
    fn from(e: std::io::Error) -> Self {
        DiemError::NetworkError(e.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for DiemError {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        DiemError::ChannelError(e.to_string())
    }
}

impl From<SendError<StoreCommand>> for DiemError {
    fn from(e: SendError<StoreCommand>) -> Self {
        DiemError::ChannelError(format!("Failed to send message to store: {}", e))
    }
}

impl From<rocksdb::Error> for DiemError {
    fn from(e: rocksdb::Error) -> Self {
        DiemError::StoreError(e.to_string())
    }
}
