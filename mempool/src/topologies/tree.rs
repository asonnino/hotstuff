use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use crypto::PublicKey;

pub type TreeNodeRef = Arc<RwLock<Tree>>;

/// Tree is a data structure to store a peer and its children.
/// It is used to build a tree topology.
#[derive(Clone, Debug)]
pub struct Tree {
    pub pub_key: PublicKey,
    pub addr: SocketAddr,
    pub children: Vec<TreeNodeRef>,
}

impl Tree {
    pub fn new(pub_key: PublicKey, addr: SocketAddr) -> Self {
        Tree {
            pub_key,
            addr,
            children: Vec::new(),
        }
    }

    pub fn add_child(&mut self, child: TreeNodeRef) {
        self.children.push(child);
    }

    pub fn add_children(&mut self, children: Vec<TreeNodeRef>) {
        self.children.extend(children);
    }

    pub fn get_children(&self) -> Vec<TreeNodeRef> {
        self.children.clone()
    }

    pub fn get_descendants_bfs(&self) -> Vec<(PublicKey, SocketAddr)> {
        let mut values = Vec::new();
        let mut queue = VecDeque::new();
        queue.extend(self.get_children());

        while let Some(node) = queue.pop_front() {
            let node = node.read().unwrap();
            values.push((node.pub_key, node.addr));
            for child in node.get_children() {
                queue.push_back(child.clone());
            }
        }

        values
    }
}
