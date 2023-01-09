use std::cmp::min;
use std::sync::{Arc, RwLock};

use crypto::PublicKey;

use crate::topologies::traits::Topology;
use crate::topologies::tree::{Tree, TreeNodeRef};
use crate::topologies::types::{
    BinomialTreeTopology, CacheTopology, FullMeshTopology, KauriTopology,
};

impl Topology for FullMeshTopology {
    fn broadcast_peers(&mut self, name: PublicKey) -> Option<TreeNodeRef> {
        if name == self.pub_key {
            let mut tree = Tree::new(self.pub_key, self.addr);
            let children = self
                .peers
                .clone()
                .into_iter()
                .map(|(key, addr)| Tree::new(key, addr))
                .collect();
            tree.add_children(children);
            Some(Arc::new(RwLock::new(tree)))
        } else {
            None
        }
    }
}

impl Topology for KauriTopology {
    fn broadcast_peers(&mut self, id: PublicKey) -> Option<TreeNodeRef> {
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

        let root = Arc::new(RwLock::new(Tree::new(
            self.peers[index].0,
            self.peers[index].1,
        )));

        let mut tree_on_level = vec![root.clone()];
        let mut i = 0;
        let mut res = None;

        'building: loop {
            let mut start = i + tree_on_level.len();
            let remaining = self.peers.len() - start;
            if remaining == 0 {
                break 'building;
            }
            let max_fanout = remaining / tree_on_level.len();
            let curr_fanout = min(self.fanout, max_fanout);

            for elem in 0..tree_on_level.len() {
                if i >= self.peers.len() || start >= self.peers.len() {
                    break 'building;
                }
                let mut tree = tree_on_level[elem].write().unwrap();
                tree.add_children(
                    self.peers[start..min(start + curr_fanout, self.peers.len())]
                        .iter()
                        .map(|(pub_key, addr)| Tree::new(pub_key.clone(), addr.clone()))
                        .collect(),
                );

                if tree.pub_key == self.pub_key {
                    res = Some(tree_on_level[elem].clone())
                }

                start += curr_fanout;
                i += 1;
            }

            // Update tree_on_level to be the children of the current level
            tree_on_level = tree_on_level
                .into_iter()
                .flat_map(|tree| tree.read().unwrap().get_children())
                .collect();
        }

        // Place the sender at the end of the list
        self.peers.rotate_right(index);

        res
    }
}

impl Topology for BinomialTreeTopology {
    fn broadcast_peers(&mut self, sender: PublicKey) -> Option<TreeNodeRef> {
        if self.peers.is_empty() {
            return None;
        }

        let mut res = None;
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &sender)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

        // The sender is the root of the tree
        let root = Arc::new(RwLock::new(Tree::new(
            self.peers[index].0,
            self.peers[index].1,
        )));

        if self.peers.len() == 1 {
            return Some(root);
        }

        let mut base = 1;
        while base < self.peers.len() {
            base <<= 1;
        }
        base >>= 1;

        let mut node_queue = vec![(root.clone(), index, base)];

        while !node_queue.is_empty() {
            let (parent, parent_index, mut bitmask) = node_queue.pop().unwrap();
            if parent.read().unwrap().pub_key == self.pub_key {
                res = Some(parent.clone());
            }

            while bitmask > 0 && bitmask + parent_index < self.peers.len() {
                let child_index = parent_index + bitmask;

                let child = Arc::new(RwLock::new(Tree::new(
                    self.peers[child_index].0,
                    self.peers[child_index].1,
                )));
                parent.write().unwrap().add_child(child.clone());

                bitmask <<= 1;
                node_queue.push((child, child_index, bitmask));
            }
        }

        self.peers.rotate_right(index);
        res
    }
}

impl<T> Topology for CacheTopology<T>
where
    T: Topology,
{
    fn broadcast_peers(&mut self, id: PublicKey) -> Option<TreeNodeRef> {
        if let Some(peers) = self.direct_peers_cache.get(&id) {
            Some(peers.clone())
        } else {
            let peers = self.inner.broadcast_peers(id);
            match peers {
                Some(peers) => {
                    self.direct_peers_cache.insert(id, peers.clone());
                    Some(peers)
                }
                None => None,
            }
        }
    }
}
