use std::net::SocketAddr;

use crypto::PublicKey;

use crate::topologies::builders::lib::TopologyBuilder;
use crate::Parameters;

use crate::topologies::builders::types::BinomialTreeTopologyBuilder;
use crate::topologies::builders::types::FullMeshTopologyBuilder;
use crate::topologies::builders::types::KauriTopologyBuilder;

use crate::topologies::types::BinomialTreeTopology;
use crate::topologies::types::FullMeshTopology;
use crate::topologies::types::KauriTopology;

use crate::topologies::builders::error::TopologyError;

impl TopologyBuilder for FullMeshTopologyBuilder {
    type Topology = FullMeshTopology;

    fn set_params(&mut self, _params: &Parameters, pub_key: PublicKey, addr: SocketAddr) {
        self.pub_key = Some(pub_key);
        self.addr = Some(addr);
    }

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<FullMeshTopology, TopologyError> {
        let pub_key = self.pub_key.ok_or(TopologyError::MissingParameters {
            param: "pub_key".to_string(),
        })?;
        let addr = self.addr.ok_or(TopologyError::MissingParameters {
            param: "addr".to_string(),
        })?;
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }

        let peers = peers.into_iter().filter(|(p, _)| p != &pub_key).collect();

        Ok(FullMeshTopology {
            peers,
            pub_key,
            addr,
        })
    }
}

impl TopologyBuilder for BinomialTreeTopologyBuilder {
    type Topology = BinomialTreeTopology;

    fn set_params(&mut self, _params: &Parameters, pub_key: PublicKey, addr: SocketAddr) {
        self.pub_key = Some(pub_key);
        self.addr = Some(addr);
    }

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<BinomialTreeTopology, TopologyError> {
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }

        let pub_key = self
            .pub_key
            .ok_or_else(|| TopologyError::MissingParameters {
                param: "name".to_string(),
            })?;

        let addr = self.addr.ok_or_else(|| TopologyError::MissingParameters {
            param: "addr".to_string(),
        })?;
        Ok(BinomialTreeTopology::new(peers, pub_key, addr))
    }
}

impl TopologyBuilder for KauriTopologyBuilder {
    type Topology = KauriTopology;

    fn set_params(&mut self, params: &Parameters, pub_key: PublicKey, addr: SocketAddr) {
        self.fanout = params.fanout;
        self.pub_key = Some(pub_key);
        self.addr = Some(addr);
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
        let pub_key = self
            .pub_key
            .ok_or_else(|| TopologyError::MissingParameters {
                param: "pub_key".to_string(),
            })?;

        let addr = self.addr.ok_or_else(|| TopologyError::MissingParameters {
            param: "addr".to_string(),
        })?;
        Ok(KauriTopology::new(peers, fanout, pub_key, addr))
    }
}
