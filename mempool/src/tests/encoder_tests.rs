use super::*;
use crate::common::Committee;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn encode_batch() {
    let (tx_batch, rx_batch) = channel(1);

    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];

    Encoder::spawn(committee(), rx_batch);
}
