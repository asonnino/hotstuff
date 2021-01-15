use crypto::{CryptoError, PublicKey};
use store::StoreError;
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
    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("Received payload from unknown authority {0:?}")]
    UnknownAuthority(PublicKey),

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Payload exceed max size")]
    PayloadTooBig,

    #[error("Mempool full: dropping transaction")]
    MempoolFull,

    #[error("Node {0:?} is not in the committee")]
    NotInCommittee(PublicKey),
}
