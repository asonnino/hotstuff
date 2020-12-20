use crate::crypto::PublicKey;
use crate::messages::{Block, Vote};

pub enum NetMessage {
    Block(Block, Vote, PublicKey),
    Vote(Vote, PublicKey),
}
