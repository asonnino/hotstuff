use std::cmp::min;
use std::net::SocketAddr;

use crate::Parameters;
use crypto::PublicKey;

use crate::topologies::error::TopologyError;
use crate::topologies::traits::{Topology, TopologyBuilder};
use crate::topologies::types::{
    BinomialTreeTopology, BinomialTreeTopologyBuilder, FullMeshTopology, FullMeshTopologyBuilder,
    KauriTopology, KauriTopologyBuilder,
};

impl TopologyBuilder for FullMeshTopologyBuilder {
    type Topology = FullMeshTopology;

    fn set_params(&mut self, _params: &Parameters, name: PublicKey) {
        self.name = Some(name);
    }

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<FullMeshTopology, TopologyError> {
        let name = self.name.ok_or(TopologyError::MissingParameters {
            param: "name".to_string(),
        })?;
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }
        Ok(FullMeshTopology { peers, name })
    }
}

impl Topology for FullMeshTopology {
    fn broadcast_peers(&mut self, name: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        if name == self.name {
            self.peers.clone()
        } else {
            vec![]
        }
    }
}

impl TopologyBuilder for KauriTopologyBuilder {
    type Topology = KauriTopology;

    fn set_params(&mut self, params: &Parameters, name: PublicKey) {
        self.fanout = params.fanout;
        self.name = Some(name)
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
        let name = self.name.ok_or_else(|| TopologyError::MissingParameters {
            param: "name".to_string(),
        })?;
        Ok(KauriTopology::new(peers, fanout, name))
    }
}

impl Topology for KauriTopology {
    fn broadcast_peers(&mut self, id: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        // id - x + 1
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();

        // Place him at the beginning of the list
        self.peers.swap(0, index);

        let mut processes_on_level = 1;
        let mut res = Vec::new();
        let mut i = 0;
        'building: while i < self.peers.len() {
            let remaining = self.peers.len() - i;
            let max_fanout = remaining / processes_on_level;
            let curr_fanout = std::cmp::min(self.fanout, max_fanout);

            let mut start = i + processes_on_level;
            if self.name == self.peers[i].0 {
                for _ in 0..processes_on_level {
                    for j in start..start + curr_fanout {
                        if j >= self.peers.len() {
                            break 'building;
                        }
                        res.push(self.peers[j]);
                    }
                    start += curr_fanout;
                    i += 1;
                }
            } else {
                i += processes_on_level;
            }
            processes_on_level = min(curr_fanout * processes_on_level, remaining);
        }
        self.peers.swap(0, index);
        res
    }
}

impl TopologyBuilder for BinomialTreeTopologyBuilder {
    type Topology = BinomialTreeTopology;

    fn set_params(&mut self, _params: &Parameters, name: PublicKey) {
        self.name = Some(name)
    }

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<BinomialTreeTopology, TopologyError> {
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }

        let name = self.name.ok_or_else(|| TopologyError::MissingParameters {
            param: "name".to_string(),
        })?;
        Ok(BinomialTreeTopology { peers, name })
    }
}

impl Topology for BinomialTreeTopology {
    fn broadcast_peers(&mut self, id: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        // id - x + 1
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();

        // Place him at the beginning of the list
        self.peers.swap(0, index);

        let mut res = Vec::new();

        // TODO
        self.peers.swap(0, index);
        res
    }
}
