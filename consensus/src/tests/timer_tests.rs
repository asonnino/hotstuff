use super::*;

#[tokio::test]
async fn schedule() {
    let timer = Timer::new(50);
    let now = Instant::now();
    timer.await;
    assert!(now.elapsed().as_millis() > 40);
}
