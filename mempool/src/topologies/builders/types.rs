use crypto::PublicKey;

#[derive(Clone, Debug)]
pub struct FullMeshTopologyBuilder {
    pub(crate) pub_key: Option<PublicKey>,
}

#[derive(Clone, Debug)]
pub struct KauriTopologyBuilder {
    pub fanout: Option<usize>,
    pub(crate) pub_key: Option<PublicKey>,
}

#[derive(Clone, Debug)]
pub struct BinomialTreeTopologyBuilder {
    pub pub_key: Option<PublicKey>,
}
