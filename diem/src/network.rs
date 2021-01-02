use crate::crypto::{Digest, PublicKey};
use crate::messages::{Block, Vote, TV};

pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    Timeout(TV, PublicKey),
    SyncRequest(Digest),
    SyncReply(Block, PublicKey),
}
