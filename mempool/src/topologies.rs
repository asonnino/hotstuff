//! Contains the traits and structured required to broadcast a batch of message
//! or to send an acknowledgement after receiving a batch of request.

use crypto::PublicKey;
use std::net::SocketAddr;
/// `Topology` represents the basic expectations of a topology structure.
pub trait Topology {
    /// `broadcast_peers` returns a slice of the peers to broadcast a batch to.
    fn broadcast_peers(&self, sender: &PublicKey) -> &[(PublicKey, SocketAddr)];

    /// `acknowledgement_peers` returns a slice of the peers to multicast an ack to.
    fn acknowledgement_peers(&self, sender: &PublicKey) -> &[(PublicKey, SocketAddr)];
}

/// `TopologyBuilder` is a trait that allows to build a topology.
pub trait TopologyBuilder<T: Topology>: Clone {
    /// `build` builds a topology from a list of peers.
    fn build(peers: Vec<(PublicKey, SocketAddr)>) -> T;
}

/// `FullMeshTopology` is a topology where every node is connected to every other node.
pub struct FullMeshTopology {
    peers: Vec<(PublicKey, SocketAddr)>,
}

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder;

impl TopologyBuilder<FullMeshTopology> for FullMeshTopologyBuilder {
    fn build(peers: Vec<(PublicKey, SocketAddr)>) -> FullMeshTopology {
        FullMeshTopology { peers }
    }
}

impl Topology for FullMeshTopology {
    fn broadcast_peers(&self, _: &PublicKey) -> &[(PublicKey, SocketAddr)] {
        &self.peers
    }

    fn acknowledgement_peers(&self, _: &PublicKey) -> &[(PublicKey, SocketAddr)] {
        &[]
    }
}
