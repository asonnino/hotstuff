use crate::store::Store;
use crate::committee::Committee;
use crate::message::{RoundNumber, QC};


struct Core {
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    committee: Committee,
    store: Store
}


impl Core {
    fn new(store: Store, committee: Committee) -> Self {
        // TODO: add genesis to the store.
        Self {
            round: 3,
            last_voted_round: 2,
            preferred_round: 1,
            highest_qc: QC::genesis().last(),
            committee
            store
        }
    }
}
