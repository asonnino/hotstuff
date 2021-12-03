use super::*;
use crate::common::{batch, committee, keys};
use crypto::generate_keypair;
use rand::{rngs::StdRng, SeedableRng as _};

#[test]
fn make_coded_batch() {
    let (batch, batch_size) = batch();

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch.clone(), batch_size, &committee());

    // Ensure the number of shards is as expected.
    let (data_shards, parity_shards) = committee().shards();
    assert_eq!(coded_batch.shards.len(), data_shards + parity_shards);

    // Ensure the first shards are the batch itself.
    assert_eq!(
        coded_batch.shards.iter().flatten().collect::<Vec<_>>()[..batch_size],
        batch.iter().flatten().collect::<Vec<_>>()
    );
}

#[test]
fn make_small_coded_batch() {
    let batch = vec![vec![1; 1], vec![2; 1]];
    let batch_size = 2; // Smaller than the committee size.

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch.clone(), batch_size, &committee());

    // Ensure the number of shards is as expected.
    let (data_shards, parity_shards) = committee().shards();
    assert_eq!(coded_batch.shards.len(), data_shards + parity_shards);

    // Ensure the first shards are the batch itself.
    assert_eq!(
        coded_batch.shards.iter().flatten().collect::<Vec<_>>()[..batch_size],
        batch.iter().flatten().collect::<Vec<_>>()
    );
}

#[test]
fn make_coded_batch_arbitrary_committee() {
    let mut rng = StdRng::from_seed([0; 32]);
    let committee = Committee::new(
        (0..20) // Not divisible by 3f+1
            .map(|_| generate_keypair(&mut rng))
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let stake = 1;
                let front = format!("127.0.0.1:{}", 100 + i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", 300 + i).parse().unwrap();
                (name, stake, front, mempool)
            })
            .collect(),
        /* epoch */ 100,
    );

    // Encode a batch.
    let (batch, batch_size) = batch();
    let coded_batch = CodedBatch::new(batch.clone(), batch_size, &committee);

    // Ensure the number of shards is as expected.
    let (data_shards, parity_shards) = committee.shards();
    assert_eq!(coded_batch.shards.len(), data_shards + parity_shards);
}

#[test]
fn reconstruct_coded_batch() {
    let (_, parity_shards) = committee().shards();
    let (batch, batch_size) = batch();

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch, batch_size, &committee());

    // Loose all parity shards.
    let coded_shards = coded_batch
        .shards
        .clone()
        .into_iter()
        .enumerate()
        .map(|(i, shard)| if i < parity_shards { None } else { Some(shard) })
        .collect();

    // Attempt to reconstruct the coded batch.
    let result = CodedBatch::reconstruct(coded_shards, &committee());
    assert!(result.is_ok());
    let reconstructed = result.unwrap();
    assert_eq!(coded_batch.shards, reconstructed.shards);
}

#[test]
fn reconstruct_coded_batch_fail() {
    let (_, parity_shards) = committee().shards();
    let (batch, batch_size) = batch();

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch, batch_size, &committee());

    // Loose too many shards.
    let coded_shards = coded_batch
        .shards
        .clone()
        .into_iter()
        .enumerate()
        .map(|(i, shard)| {
            if i <= parity_shards {
                None
            } else {
                Some(shard)
            }
        })
        .collect();

    // Attempt to reconstruct the coded batch.
    let result = CodedBatch::reconstruct(coded_shards, &committee());
    assert!(result.is_err());
}

#[test]
fn compress_coded_batch() {
    let (data_shards, _) = committee().shards();
    let (batch, batch_size) = batch();

    // Encode the batch.
    let mut coded_batch = CodedBatch::new(batch, batch_size, &committee());
    let original_shards = coded_batch.shards.clone();

    // Compress the coded batch.
    coded_batch.compress(&committee());
    assert_eq!(coded_batch.shards.len(), data_shards);

    // Re-expand the batch and verify that we get the original.
    coded_batch.expand(&committee()).unwrap();
    assert_eq!(coded_batch.shards, original_shards);
}

#[tokio::test]
async fn verify_coded_shards() {
    let (batch, batch_size) = batch();

    // Encode the batch and commit to it.
    let coded_batch = CodedBatch::new(batch, batch_size, &committee());
    let tree = coded_batch.commit();

    // Make and verify each coded shard.
    let (author, secret) = keys().pop().unwrap();
    let mut signature_service = SignatureService::new(secret);
    for (i, shard) in coded_batch.shards.iter().enumerate() {
        // Make the coded shard.
        let authenticated_shard =
            AuthenticatedShard::new(shard.clone(), i, &tree, author, &mut signature_service).await;

        // Verify the coded shard.
        let name = committee().name(i).unwrap();
        assert!(authenticated_shard.verify(&name, &committee()).is_ok());
    }
}
