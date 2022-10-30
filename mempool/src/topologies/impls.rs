use std::cmp::min;
use std::collections::HashSet;
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
        let peers = peers.into_iter().filter(|(p, _)| p != &name).collect();
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

    fn indirect_peers(&mut self) -> Vec<(PublicKey, SocketAddr)> {
        vec![]
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
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

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
        // Place the sender at the end of the list
        self.peers.rotate_right(index);
        res
    }

    fn indirect_peers(&mut self) -> Vec<(PublicKey, SocketAddr)> {
        // Returns the difference of self.peers and self.broadcast_peers(self.name)
        let broadcast_set: HashSet<(PublicKey, SocketAddr)> =
            self.broadcast_peers(self.name).iter().cloned().collect();

        self.peers
            .iter()
            .filter(|peer| !broadcast_set.contains(peer) || peer.0 == self.name)
            .cloned()
            .collect()
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
        let my_index = peers
            .iter()
            .position(|(peer_id, _)| peer_id == &name)
            .expect("Peer not found in peers list");
        Ok(BinomialTreeTopology {
            peers,
            name,
            my_index,
        })
    }
}

impl Topology for BinomialTreeTopology {
    fn broadcast_peers(&mut self, sender: PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &sender)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

        let mut res = Vec::new();
        let mut base = 1;
        if sender == self.name {
            // If the sender is the current node, then the result is self.peers[2^i] for i in [0, log2(peers.len())]
            while base < self.peers.len() {
                base <<= 1;
            }
            base >>= 1;
            while base > 0 {
                res.push(self.peers[base].clone());
                base >>= 1;
            }
        } else {
            let mut my_index = (self.my_index - index) % self.peers.len();
            while my_index != 0 && my_index % 2 == 0 {
                my_index >>= 1;
                base <<= 1;
            }
            base >>= 1;
            while base > 0 {
                res.push(self.peers[my_index + base].clone());
                base >>= 1;
            }
        }
        // Place the sender at the end of the list
        self.peers.rotate_right(index);
        res
    }

    fn indirect_peers(&mut self) -> Vec<(PublicKey, SocketAddr)> {
        self.peers.rotate_left(self.my_index);
        let children: HashSet<_> = self.broadcast_peers(self.name).into_iter().collect();
        let mut res = children.clone();
        let mut bitmask = 1;
        while bitmask < self.peers.len() {
            bitmask <<= 1;
        }
        bitmask >>= 1;
        let mut subchildren = vec![bitmask, 0];

        while bitmask > 0 {
            bitmask >>= 1;
            for i in 0..subchildren.len() {
                let v = subchildren[i] | bitmask;
                subchildren.push(v);
                if v < self.peers.len() && !children.contains(&self.peers[v]) {
                    res.insert(self.peers[v].clone());
                }
            }
        }

        self.peers.rotate_right(self.my_index);

        res.into_iter()
            .filter(|peer| !children.contains(peer) && peer.0 != self.name)
            .collect()
    }
}
