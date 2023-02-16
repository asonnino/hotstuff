use crate::topologies::tree::TreeNodeRef;
use crypto::PublicKey;
use std::{collections::HashMap, net::SocketAddr};

/// `FullMeshTopology` is a topology where every node is connected to every other node.
#[derive(Clone, Debug)]
pub struct FullMeshTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) pub_key: PublicKey,
}

impl FullMeshTopology {
    pub fn new(mut peers: Vec<(PublicKey, SocketAddr)>, pub_key: PublicKey) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        FullMeshTopology { peers, pub_key }
    }
}

/// `KauriTopology` is a simple tree topology parametrized by the number of children per node.
#[derive(Clone, Debug)]
pub struct KauriTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) fanout: usize,
    pub(crate) pub_key: PublicKey,
}

impl KauriTopology {
    pub fn new(mut peers: Vec<(PublicKey, SocketAddr)>, fanout: usize, pub_key: PublicKey) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        KauriTopology {
            peers,
            fanout,
            pub_key,
        }
    }
}

/// `BinomialTreeTopology` is a topology where the leader will reach every node.
#[derive(Clone, Debug)]
pub struct BinomialTreeTopology {
    pub(crate) peers: Vec<(PublicKey, SocketAddr)>,
    pub(crate) pub_key: PublicKey,
}

impl BinomialTreeTopology {
    pub fn new(mut peers: Vec<(PublicKey, SocketAddr)>, pub_key: PublicKey) -> Self {
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        BinomialTreeTopology { peers, pub_key }
    }
}

#[derive(Clone, Debug)]
pub struct CacheTopology<T> {
    pub(crate) inner: T,
    pub(crate) direct_peers_cache: HashMap<PublicKey, Option<TreeNodeRef>>,
}

impl<T> CacheTopology<T> {
    pub fn new(inner: T) -> Self {
        CacheTopology {
            inner,
            direct_peers_cache: HashMap::new(),
        }
    }
}
