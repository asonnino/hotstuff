use std::net::SocketAddr;

use crypto::PublicKey;

use crate::topologies::builders::traits::TopologyBuilder;
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

    fn new() -> Self {
        FullMeshTopologyBuilder { pub_key: None }
    }

    fn set_params(&mut self, _params: &Parameters, pub_key: PublicKey) {
        self.pub_key = Some(pub_key);
    }

    fn build(
        &self,
        peers: Vec<(PublicKey, SocketAddr)>,
    ) -> Result<FullMeshTopology, TopologyError> {
        let pub_key = self.pub_key.ok_or(TopologyError::MissingParameters {
            param: "pub_key".to_string(),
        })?;
        if peers.is_empty() {
            return Err(TopologyError::NoPeers);
        }

        Ok(FullMeshTopology { peers, pub_key })
    }
}

impl TopologyBuilder for BinomialTreeTopologyBuilder {
    type Topology = BinomialTreeTopology;

    fn new() -> Self {
        BinomialTreeTopologyBuilder { pub_key: None }
    }

    fn set_params(&mut self, _params: &Parameters, pub_key: PublicKey) {
        self.pub_key = Some(pub_key);
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

        Ok(BinomialTreeTopology::new(peers, pub_key))
    }
}

impl TopologyBuilder for KauriTopologyBuilder {
    type Topology = KauriTopology;

    fn new() -> Self {
        KauriTopologyBuilder {
            pub_key: None,
            fanout: None,
        }
    }

    fn set_params(&mut self, params: &Parameters, pub_key: PublicKey) {
        self.fanout = params.fanout;
        self.pub_key = Some(pub_key);
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

        Ok(KauriTopology::new(peers, fanout, pub_key))
    }
}
