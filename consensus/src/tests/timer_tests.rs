use super::*;

#[tokio::test]
async fn schedule() {
    // Make a timer and give it enough time to boot.
    let mut timer = Timer::new();
    sleep(Duration::from_millis(50)).await;

    // schedule a timer.
    let timer_id = "id".to_string();
    timer.schedule(50, timer_id.clone()).await;
    match timer.notifier.recv().await {
        Some(id) => assert_eq!(id, timer_id),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn cancel() {
    // Make a timer and give it enough time to boot.
    let mut timer = Timer::new();
    sleep(Duration::from_millis(50)).await;

    // schedule two timers.
    let timer_1 = "timer_1".to_string();
    timer.schedule(50, timer_1.clone()).await;
    let timer_2 = "timer_2".to_string();
    timer.schedule(100, timer_2.clone()).await;

    // Cancel timer 1.
    timer.cancel(timer_1.clone()).await;

    // Ensure we receive timer 2 (and not timer 1).
    match timer.notifier.recv().await {
        Some(id) => assert_eq!(id, timer_2),
        _ => assert!(false),
    }
}
