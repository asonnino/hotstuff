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
    fn broadcast_peers(&self) -> &[(PublicKey, SocketAddr)];
}

/// `TopologyBuilder` is a trait that allows to build a topology.
pub trait TopologyBuilder<T: Topology>: Clone {
    /// 'set_params' sets the parameters of the topology.
    fn set_params(&mut self, params: &Parameters, name: &PublicKey) -> ();

    /// `build` builds a topology from a list of peers.
    fn build(&self, peers: Vec<(PublicKey, SocketAddr)>) -> Result<T, TopologyError>;
}

/// `FullMeshTopology` is a topology where every node is connected to every other node.
pub struct FullMeshTopology {
    peers: Vec<(PublicKey, SocketAddr)>,
}

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder;

impl TopologyBuilder<FullMeshTopology> for FullMeshTopologyBuilder {
    fn set_params(&mut self, _params: &Parameters, _name: &PublicKey) -> () {}

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<FullMeshTopology, TopologyError> {
        if peers.len() > 0 {
            Ok(FullMeshTopology { peers })
        } else {
            Err(TopologyError::NoPeers)
        }
    }
}

impl Topology for FullMeshTopology {
    fn broadcast_peers(&self) -> &[(PublicKey, SocketAddr)] {
        &self.peers
    }
}

#[derive(Clone, Debug)]
pub struct KauriTopologyBuilder {
    pub fanout: Option<usize>,
    pub id: Option<PublicKey>,
}

impl TopologyBuilder<KauriTopology> for KauriTopologyBuilder {
    fn set_params(&mut self, params: &Parameters, name: &PublicKey) -> () {
        self.fanout = params.fanout;
        self.id = Some(name.clone());
    }

    // Builds an n-ary tree and add the children of id to peers
    fn build(&self, peers: Vec<(PublicKey, SocketAddr)>) -> Result<KauriTopology, TopologyError> {
        if peers.len() < 1 {
            return Err(TopologyError::NoPeers);
        }

        let fanout = self
            .fanout
            .ok_or_else(|| TopologyError::MissingParameters {
                param: "fanout".to_string(),
            })?;
        let id = self.id.ok_or_else(|| TopologyError::MissingParameters {
            param: "id".to_string(),
        })?;
        let mut processes_on_level = 1;
        let mut new_peers = peers.clone();
        new_peers.sort_by(|a, b| a.0.cmp(&b.0));

        let mut res = Vec::new();
        let mut i = 0;
        'building: while i < new_peers.len() {
            let remaining = new_peers.len() - i;
            let max_fanout = remaining / processes_on_level;
            let curr_fanout = std::cmp::min(fanout, max_fanout);

            let mut start = i + processes_on_level;

            for _ in 1..processes_on_level + 1 {
                for j in start..start + curr_fanout {
                    if j >= new_peers.len() {
                        break 'building;
                    }
                    if id == new_peers[i].0 {
                        res.push(new_peers[j]);
                    }
                }
                start += curr_fanout;
                i += 1;
            }
            processes_on_level = min(curr_fanout * processes_on_level, remaining);
        }
        info!("{} Kauri topology built with {:?} peers", id, res);
        Ok(KauriTopology { peers: res })
    }
}

impl Topology for KauriTopology {
    fn broadcast_peers(&self) -> &[(PublicKey, SocketAddr)] {
        &self.peers
    }
}

#[derive(Clone, Debug)]
pub struct KauriTopology {
    peers: Vec<(PublicKey, SocketAddr)>,
}
