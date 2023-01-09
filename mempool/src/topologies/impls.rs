use std::cmp::min;

use crypto::PublicKey;

use crate::topologies::traits::Topology;
use crate::topologies::tree::Tree;
use crate::topologies::types::{
    BinomialTreeTopology, CacheTopology, FullMeshTopology, KauriTopology,
};

impl Topology for FullMeshTopology {
    fn broadcast_peers(&mut self, name: PublicKey) -> Option<Tree> {
        if name == self.pub_key {
            let mut tree = Tree::new(self.pub_key, self.addr);
            let children = self
                .peers
                .clone()
                .into_iter()
                .map(|(key, addr)| Tree::new(key, addr))
                .collect();
            tree.add_children(children);
            Some(tree)
        } else {
            None
        }
    }
}

impl Topology for KauriTopology {
    fn broadcast_peers(&mut self, id: PublicKey) -> Option<Tree> {
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &id)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

        let mut processes_on_level = 1;
        let mut res = Vec::new();
        let mut i = 0;

        'building: loop {
            let mut start = i + processes_on_level;
            let remaining = self.peers.len() - start;
            if remaining == 0 {
                break 'building;
            }
            let max_fanout = remaining / processes_on_level;
            let curr_fanout = min(self.fanout, max_fanout);

            for _ in 0..processes_on_level {
                if i >= self.peers.len() || start >= self.peers.len() {
                    break 'building;
                }

                if self.name == self.peers[i].0 {
                    (start..min(start + curr_fanout, self.peers.len())).for_each(|j| {
                        res.push(self.peers[j]);
                    });
                }
                start += curr_fanout;
                i += 1;
            }
            processes_on_level = min(curr_fanout * processes_on_level, remaining);
        }

        // Place the sender at the end of the list
        self.peers.rotate_right(index);

        res
    }
}

impl Topology for BinomialTreeTopology {
    fn broadcast_peers(&mut self, sender: PublicKey) -> Option<Tree> {
        // Find the index of the peer in the list
        let index = self
            .peers
            .iter()
            .position(|(peer_id, _)| peer_id == &sender)
            .unwrap();

        // Place the sender at the beginning of the list
        self.peers.rotate_left(index);

        let mut res = Vec::new();
        let mut base = 1;
        if sender == self.pub_key {
            // If the sender is the current node, then the result is self.peers[2^i] for i in [0, log2(peers.len())]
            while base < self.peers.len() {
                base <<= 1;
            }
            base >>= 1;
            while base > 0 {
                res.push(self.peers[base]);
                base >>= 1;
            }
        } else {
            let mut tmp =
                (self.my_index as i32 - index as i32).rem_euclid(self.peers.len() as i32) as usize;
            let fixed_index = tmp;
            while tmp != 0 && tmp % 2 == 0 {
                tmp >>= 1;
                base <<= 1;
            }
            base >>= 1;
            while base > 0 {
                let child_index = fixed_index + base;
                if child_index < self.peers.len() {
                    res.push(self.peers[child_index]);
                }
                base >>= 1;
            }
        }
        // Place the sender at the end of the list
        self.peers.rotate_right(index);
        res
    }
}

impl<T> Topology for CacheTopology<T>
where
    T: Topology,
{
    fn broadcast_peers(&mut self, id: PublicKey) -> Option<Tree> {
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
