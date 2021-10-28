use super::*;
use crate::common::{committee, keys};

#[test]
fn make_coded_batch() {
    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch.clone(), batch_size, &committee());

    // Ensure the number of shards is as expected.
    let (data_shards, parity_shards) = committee().shards();
    assert_eq!(coded_batch.shards.len(), data_shards + parity_shards);

    // Ensure the first shards are the batch itself.
    assert_eq!(coded_batch.shards[..batch.len()], batch);
}

#[test]
fn reconstruct_coded_batch() {
    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch, batch_size, &committee());

    // Loose 2 shards.
    let coded_shards = coded_batch
        .shards
        .clone()
        .into_iter()
        .enumerate()
        .map(|(i, shard)| match i {
            1 | 3 => None,
            _ => Some(shard),
        })
        .collect();

    // Attempt to reconstruct the coded batch.
    let result = CodedBatch::reconstruct(coded_shards, &committee());
    assert!(result.is_ok());
    let reconstructed = result.unwrap();
    assert_eq!(coded_batch.shards, reconstructed.shards);
}

#[test]
fn reconstruct_coded_batch_fail() {
    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];

    // Encode the batch.
    let coded_batch = CodedBatch::new(batch, batch_size, &committee());

    // Loose 3 shards.
    let coded_shards = coded_batch
        .shards
        .clone()
        .into_iter()
        .enumerate()
        .map(|(i, shard)| match i {
            1 | 2 | 3 => None,
            _ => Some(shard),
        })
        .collect();

    // Attempt to reconstruct the coded batch.
    let result = CodedBatch::reconstruct(coded_shards, &committee());
    assert!(result.is_err());
}

#[tokio::test]
async fn verify_coded_shards() {
    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];

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
