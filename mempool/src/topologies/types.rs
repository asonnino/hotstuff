use std::net::SocketAddr;

use crypto::PublicKey;

/// `FullMeshTopology` is a topology where every node is connected to every other node.
#[derive(Clone, Debug)]
pub struct FullMeshTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) name: PublicKey,
}

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder {
    pub name: Option<PublicKey>,
}

/// `KauriTopology` is a simple tree topology parametrized by the number of children per node.
#[derive(Clone, Debug)]
pub struct KauriTopology {
    pub peers: Vec<(PublicKey, SocketAddr)>,
    pub fanout: usize,
    pub name: PublicKey,
}

#[derive(Clone, Debug)]
pub struct KauriTopologyBuilder {
    pub fanout: Option<usize>,
    pub name: Option<PublicKey>,
}

impl KauriTopology {
    pub fn new(mut peers: Vec<(PublicKey, SocketAddr)>, fanout: usize, name: PublicKey) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        KauriTopology {
            peers,
            fanout,
            name,
        }
    }
}

/// `BinomialTreeTopology` is a topology where the leader will reach every node.
#[derive(Clone, Debug)]
pub struct BinomialTreeTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) name: PublicKey,
    pub my_index: usize,
}

#[derive(Clone, Debug)]
pub struct BinomialTreeTopologyBuilder {
    pub name: Option<PublicKey>,
}
