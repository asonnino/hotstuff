use crate::committee::Committee;
use crate::core::RoundNumber;
use crate::crypto::PublicKey;

pub trait LeaderElection {
    fn get_leader(&self, round: RoundNumber, committee: &Committee) -> PublicKey;
}

struct RRLeaderElection {}

impl LeaderElection for RRLeaderElection {
    fn get_leader(&self, round: RoundNumber, committee: &Committee) -> PublicKey {
        let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % committee.size()]
    }
}
