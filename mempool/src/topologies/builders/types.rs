use std::net::SocketAddr;

use crypto::PublicKey;

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder {
    pub(crate) pub_key: Option<PublicKey>,
    pub(crate) addr: Option<SocketAddr>,
}

#[derive(Clone, Debug)]
pub struct KauriTopologyBuilder {
    pub fanout: Option<usize>,
    pub(crate) pub_key: Option<PublicKey>,
    pub(crate) addr: Option<SocketAddr>,
}

#[derive(Clone, Debug)]
pub struct BinomialTreeTopologyBuilder {
    pub pub_key: Option<PublicKey>,
    pub addr: Option<SocketAddr>,
}
