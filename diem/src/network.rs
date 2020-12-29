use crate::crypto::{Digest, PublicKey};
use crate::messages::{Block, Vote};

pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    SyncRequest(Digest),
    SyncReply(Block, PublicKey),
}
