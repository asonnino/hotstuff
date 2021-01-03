use crate::committee::Committee;
use crate::core::RoundNumber;
use crate::crypto::PublicKey;

pub struct LeaderElector {
    committee: Committee,
}

impl LeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }

    pub fn get_leader(&self, round: RoundNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }
}
