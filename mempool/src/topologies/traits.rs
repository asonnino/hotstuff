//! Contains the traits and structures required to broadcast a batch of message
//! or to send an acknowledgement after receiving a batch of request.

use crate::topologies::tree::Tree;

use crypto::PublicKey;

/// `Topology` represents the basic expectations of a topology structure.
pub trait Topology: Clone + Send + Sync + 'static {
    /// `broadcast_peers` returns a slice of the peers to broadcast a batch to.
    fn broadcast_peers(&mut self, source: PublicKey) -> Option<Tree>;
}
