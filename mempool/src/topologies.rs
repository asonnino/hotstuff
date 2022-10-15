//! Contains the traits and structured required to broadcast a batch of message
//! or to send an acknowledgement after receiving a batch of request.

use crypto::PublicKey;
use log::info;
use std::{cmp::min, net::SocketAddr};
use thiserror::Error;

use crate::Parameters;

/// Errors that can occur when building a topology
#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("Missing params : '{param}'")]
    MissingParameters { param: String },
    #[error("No peers found.")]
    NoPeers,
    #[error("Invalid parameter : {param}")]
    InvalidParameter { param: String },
}

/// `Topology` represents the basic expectations of a topology structure.
pub trait Topology {
    /// `broadcast_peers` returns a slice of the peers to broadcast a batch to.
    fn broadcast_peers(&mut self, name: PublicKey) -> Vec<(PublicKey, SocketAddr)>;
}

/// `TopologyBuilder` is a trait that allows to build a topology.
pub trait TopologyBuilder: Clone {
    type Topology: Topology;

    /// 'set_params' sets the parameters of the topology.
    fn set_params(&mut self, params: &Parameters, name: PublicKey);

    /// `build` builds a topology from a list of peers.
    fn build(&self, peers: Vec<(PublicKey, SocketAddr)>) -> Result<Self::Topology, TopologyError>;
}

/// `FullMeshTopology` is a topology where every node is connected to every other node.
pub struct FullMeshTopology {
    peers: Vec<(PublicKey, SocketAddr)>,
}

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder;

impl TopologyBuilder for FullMeshTopologyBuilder {
    type Topology = FullMeshTopology;

    fn set_params(&mut self, _params: &Parameters, _name: PublicKey) {}

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<FullMeshTopology, TopologyError> {
        if !peers.is_empty() {
            Ok(FullMeshTopology { peers })
        } else {
            Err(TopologyError::NoPeers)
        }
    }
}

impl Topology for FullMeshTopology {
    fn broadcast_peers(&mut self, _name: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.peers.clone()
    }
}

#[derive(Clone, Debug)]
pub struct KauriTopologyBuilder {
    pub fanout: Option<usize>,
}

impl TopologyBuilder for KauriTopologyBuilder {
    type Topology = KauriTopology;

    fn set_params(&mut self, params: &Parameters, _name: PublicKey) {
        self.fanout = params.fanout;
    }

    // Builds an n-ary tree and add the children of id to peers
    fn build(&self, peers: Vec<(PublicKey, SocketAddr)>) -> Result<KauriTopology, TopologyError> {
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }

        let fanout = self
            .fanout
            .ok_or_else(|| TopologyError::MissingParameters {
                param: "fanout".to_string(),
            })?;

        Ok(KauriTopology::new(peers, fanout))
    }
}

impl Topology for KauriTopology {
    fn broadcast_peers(&mut self, id: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();
        self.peers.swap(0, index);
        let mut processes_on_level = 1;
        let mut res = Vec::new();
        let mut i = 0;
        'building: while i < self.peers.len() {
            let remaining = self.peers.len() - i;
            let max_fanout = remaining / processes_on_level;
            let curr_fanout = std::cmp::min(self.fanout, max_fanout);

            let mut start = i + processes_on_level;

            for _ in 1..processes_on_level + 1 {
                for j in start..start + curr_fanout {
                    if j >= self.peers.len() {
                        break 'building;
                    }
                    if id == self.peers[i].0 {
                        res.push(self.peers[j]);
                    }
                }
                start += curr_fanout;
                i += 1;
            }
            processes_on_level = min(curr_fanout * processes_on_level, remaining);
        }
        self.peers.swap(0, index);
        info!("Broadcasting to {} peers", res.len());
        res
    }
}

#[derive(Clone, Debug)]
pub struct KauriTopology {
    peers: Vec<(PublicKey, SocketAddr)>,
    fanout: usize,
}

impl KauriTopology {
    pub fn new(mut peers: Vec<(PublicKey, SocketAddr)>, fanout: usize) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        KauriTopology { peers, fanout }
    }
}
