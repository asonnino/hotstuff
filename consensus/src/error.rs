use crate::core::SeqNumber;
use crypto::{CryptoError, Digest, PublicKey};
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

pub type ConsensusResult<T> = Result<T, ConsensusError>;

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(PublicKey),

    #[error("Received vote from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Received TC without a quorum")]
    TCRequiresQuorum,

    #[error("Received RandomCoin without a quorum")]
    RandomCoinRequiresQuorum,

    #[error("Malformed block {0}")]
    MalformedBlock(Digest),

    #[error("Received block {digest} from leader {leader} at round {round}")]
    WrongLeader {
        digest: Digest,
        leader: PublicKey,
        round: SeqNumber,
    },

    #[error("Invalid payload")]
    InvalidPayload,

    #[error("Block rounds not consecutive! rounds {rd1}, {rd2} and {rd3}")]
    NonConsecutiveRounds {
        rd1: SeqNumber,
        rd2: SeqNumber,
        rd3: SeqNumber,
    },
}
