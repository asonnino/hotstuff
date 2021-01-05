use futures::future::FutureExt as _;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Sender};
use tokio::time::sleep;

#[cfg(test)]
#[path = "tests/timer_tests.rs"]
pub mod timer_tests;

type TimerDuration = u64;
pub type TimerId = String;

enum TimerMessage {
    Schedule(TimerDuration, TimerId, Sender<TimerId>),
    Cancel(TimerId),
}

#[derive(Clone)]
pub struct TimerManager {
    channel: Sender<TimerMessage>,
}

impl TimerManager {
    pub async fn new() -> Self {
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut canceled = HashSet::new();
            loop {
                select! {
                    message = rx.next().fuse() => {
                        if let Some(message) = message {
                            match message {
                                TimerMessage::Schedule(delay, id, notifier) => {
                                    let fut = Self::waiter(delay, id, notifier);
                                    waiting.push(fut);
                                },
                                TimerMessage::Cancel(id) => {
                                    let _ = canceled.insert(id);
                                }
                            }
                        }
                    },
                    (id, notifier) = waiting.select_next_some() => {
                        if !canceled.remove(&id) {
                            let _ = notifier.send(id).await;
                        }
                    },
                }
            }
        });
        Self { channel: tx }
    }

    async fn waiter(
        delay: TimerDuration,
        id: TimerId,
        notifier: Sender<TimerId>,
    ) -> (TimerId, Sender<TimerId>) {
        sleep(Duration::from_millis(delay)).await;
        (id, notifier)
    }

    pub async fn schedule(&mut self, delay: TimerDuration, id: TimerId, notifier: Sender<TimerId>) {
        let message = TimerMessage::Schedule(delay, id, notifier);
        if let Err(e) = self.channel.send(message).await {
            panic!("Failed to schedule timer: {}", e);
        }
    }

    pub async fn cancel(&mut self, id: TimerId) {
        let message = TimerMessage::Cancel(id);
        if let Err(e) = self.channel.send(message).await {
            panic!("Failed to cancel timer: {}", e);
        }
    }
}
