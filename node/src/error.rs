use crate::core::RoundNumber;
use crypto::crypto::{Digest, PublicKey, CryptoError};
use store::store::StoreError;
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

pub type ConsensusResult<T> = Result<T, ConsensusError>;

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("Failed to read config file '{0}': {1}")]
    ConfigError(String, String),

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Received more than one vote from {0:?}")]
    AuthorityReuse(PublicKey),

    #[error("Received vote from unknown authority {0:?}")]
    UnknownAuthority(PublicKey),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Malformed block {0:?}")]
    MalformedBlock(Digest),

    #[error("Received block {digest:?} from leader {leader:?} at round {round}")]
    WrongLeader {
        digest: Digest,
        leader: PublicKey,
        round: RoundNumber,
    },

    #[error("Invalid payload")]
    InvalidPayload,
}

impl From<CryptoError> for ConsensusError {
    fn from(_e: CryptoError) -> Self {
        ConsensusError::InvalidSignature
    }
}
