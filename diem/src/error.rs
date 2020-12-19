use crate::core::CoreMessage;
use crate::crypto::PublicKey;
use crate::store::StoreError;
use ed25519_dalek::ed25519;
use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type DiemResult<T> = Result<T, DiemError>;

#[derive(Error, Debug)]
pub enum DiemError {
    #[error("Network error: {0}")]
    NetworkError(std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(Box<bincode::ErrorKind>),

    #[error("Store error: {0}")]
    StoreError(String),

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

    #[error("Received unexpected message {0:?}")]
    UnexpectedMessage(Box<CoreMessage>),
}

impl From<Box<bincode::ErrorKind>> for DiemError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        DiemError::SerializationError(e)
    }
}

impl From<std::io::Error> for DiemError {
    fn from(e: std::io::Error) -> Self {
        DiemError::NetworkError(e)
    }
}

impl From<ed25519::Error> for DiemError {
    fn from(_e: ed25519::Error) -> Self {
        DiemError::InvalidSignature
    }
}

impl From<StoreError> for DiemError {
    fn from(e: StoreError) -> Self {
        DiemError::StoreError(e.to_string())
    }
}
