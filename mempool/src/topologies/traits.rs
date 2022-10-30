//! Contains the traits and structures required to broadcast a batch of message
//! or to send an acknowledgement after receiving a batch of request.

use std::net::SocketAddr;

use crypto::PublicKey;

use crate::Parameters;

use crate::topologies::error::TopologyError;

/// `Topology` represents the basic expectations of a topology structure.
pub trait Topology: Clone + Send + Sync + 'static {
    /// `broadcast_peers` returns a slice of the peers to broadcast a batch to.
    fn broadcast_peers(&mut self, name: PublicKey) -> Vec<(PublicKey, SocketAddr)>;

    /// `indirect peers` returns a slice of the peers to broadcast a batch to if an ack is not received before a given period.
    fn indirect_peers(&mut self) -> Vec<(PublicKey, SocketAddr)>;
}

/// `TopologyBuilder` is a trait that allows to build a topology.
pub trait TopologyBuilder: Clone {
    type Topology: Topology;

    /// 'set_params' sets the parameters of the topology.
    fn set_params(&mut self, params: &Parameters, name: PublicKey);

    /// `build` builds a topology from a list of peers.
    fn build(&self, peers: Vec<(PublicKey, SocketAddr)>) -> Result<Self::Topology, TopologyError>;
}
