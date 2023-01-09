use std::{collections::HashMap, net::SocketAddr};

use crate::topologies::tree::Tree;

use crypto::PublicKey;

/// `FullMeshTopology` is a topology where every node is connected to every other node.
#[derive(Clone, Debug)]
pub struct FullMeshTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) pub_key: PublicKey,
    pub(crate) addr: SocketAddr,
}

/// `KauriTopology` is a simple tree topology parametrized by the number of children per node.
#[derive(Clone, Debug)]
pub struct KauriTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) fanout: usize,
    pub(crate) pub_key: PublicKey,
    pub(crate) addr: SocketAddr,
}

impl KauriTopology {
    pub fn new(
        mut peers: Vec<(PublicKey, SocketAddr)>,
        fanout: usize,
        pub_key: PublicKey,
        addr: SocketAddr,
    ) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        KauriTopology {
            peers,
            fanout,
            pub_key,
            addr,
        }
    }
}

/// `BinomialTreeTopology` is a topology where the leader will reach every node.
#[derive(Clone, Debug)]
pub struct BinomialTreeTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) pub_key: PublicKey,
    pub(crate) addr: SocketAddr,
    pub(crate) my_index: usize,
}

impl BinomialTreeTopology {
    pub fn new(
        mut peers: Vec<(PublicKey, SocketAddr)>,
        pub_key: PublicKey,
        addr: SocketAddr,
    ) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        let my_index = peers.iter().position(|(p, _)| p == &pub_key).unwrap();
        BinomialTreeTopology {
            peers,
            pub_key,
            addr,
            my_index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheTopology<T> {
    pub(crate) inner: T,
    pub(crate) direct_peers_cache: HashMap<PublicKey, Tree>,
}

impl<T> CacheTopology<T> {
    pub fn new(inner: T) -> Self {
        CacheTopology {
            inner,
            direct_peers_cache: HashMap::new(),
        }
    }
}
