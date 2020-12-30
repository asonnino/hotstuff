use crate::committee::Committee;
use crate::core::RoundNumber;
use crate::crypto::PublicKey;

pub trait LeaderElector {
    fn get_leader(&self, round: RoundNumber) -> PublicKey;
}

struct RRLeaderElector {
    committee: Committee,
}

impl LeaderElector for RRLeaderElector {
    fn get_leader(&self, round: RoundNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }
}
