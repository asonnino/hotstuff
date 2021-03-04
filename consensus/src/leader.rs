use crate::config::Committee;
use crate::core::SeqNumber;
use crypto::PublicKey;

pub type LeaderElector = RRLeaderElector;

pub struct RRLeaderElector {
    committee: Committee,
}

impl RRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }

    pub fn get_leader(&self, round: SeqNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }

    // daniel: round robin for now, need to implement common random coin
    pub fn get_leader_async(&self, view: SeqNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[view as usize % self.committee.size()]
    }
}
