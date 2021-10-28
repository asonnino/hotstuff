use crypto::{CryptoError, PublicKey};
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

pub type MempoolResult<T> = Result<T, MempoolError>;

#[derive(Error, Debug)]
pub enum MempoolError {
    #[error("Received payload from unknown authority {0:?}")]
    UnknownAuthority(PublicKey),

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Inclusion proof failed")]
    BadInclusionProof,

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(PublicKey),

    #[error("Failed to reconstruct batch for erasure-coded shards")]
    MalformedCodedBatch,

    #[error("Failed to reconstruct batch")]
    FailedToReconstructBatch(#[from] reed_solomon_erasure::Error),
}
