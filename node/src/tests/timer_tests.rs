use super::*;

#[tokio::test]
async fn schedule() {
    // Make a timer manager and give it enough time to boot.
    let mut timer_manager = TimerManager::new().await;
    let (tx_timer, mut rx_timer) = channel(100);
    sleep(Duration::from_millis(50)).await;

    // schedule a timer.
    let timer_id = "id".to_string();
    timer_manager.schedule(50, timer_id.clone(), tx_timer).await;
    match rx_timer.recv().await {
        Some(id) => assert_eq!(id, timer_id),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn cancel() {
    // Make a timer manager and give it enough time to boot.
    let mut timer_manager = TimerManager::new().await;
    let (tx_timer, mut rx_timer) = channel(100);
    sleep(Duration::from_millis(50)).await;

    // schedule two timers.
    let timer_1 = "timer_1".to_string();
    timer_manager
        .schedule(50, timer_1.clone(), tx_timer.clone())
        .await;
    let timer_2 = "timer_2".to_string();
    timer_manager.schedule(100, timer_2.clone(), tx_timer).await;

    // Cancel timer 1.
    timer_manager.cancel(timer_1.clone()).await;

    // Ensure we receive timer 2 (and not timer 1).
    match rx_timer.recv().await {
        Some(id) => assert_eq!(id, timer_2),
        _ => assert!(false),
    }
}
