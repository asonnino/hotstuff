use std::net::SocketAddr;

use crypto::PublicKey;

/// Tree is a data structure to store a peer and its children.
/// It is used to build a tree topology.
#[derive(Clone, Debug)]
pub struct Tree {
    pub pub_key: PublicKey,
    pub addr: SocketAddr,
    pub children: Vec<Tree>,
}

impl Tree {
    pub fn new(pub_key: PublicKey, addr: SocketAddr) -> Self {
        Tree {
            pub_key,
            addr,
            children: Vec::new(),
        }
    }
    pub fn add_child(&mut self, child: Tree) {
        self.children.push(child);
    }

    pub fn add_children(&mut self, children: Vec<Tree>) {
        self.children.extend(children);
    }

    pub fn get_children(&self) -> &[Tree] {
        &self.children
    }
}
