use std::marker::Unpin;
use std::pin::Pin;
use futures::task::{Context, Poll};
use futures::prelude::*;
use pin_project_lite::pin_project;
use  std::thread_local;
use std::cell::RefCell;
use futures::task::ArcWake;
use std::task::{Waker,};
use std::sync::Arc;
use futures::task::waker;
pub use std::sync::atomic::{AtomicUsize, Ordering};
pub use log::*;


use tokio::sync::mpsc::{channel, Receiver, Sender};

pub static TASK_NUM: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    pub static CURRENT_PROFILE_TASK: RefCell<Option<usize>> = RefCell::new(None);
}

pub struct ProfileWaker {
    inner_waker : Waker,
    from: usize,
}

impl ArcWake for ProfileWaker {
    fn wake(self: Arc<Self>) {

        let mut this_name = None;
        CURRENT_PROFILE_TASK.with(|f| {
            this_name = f.borrow().clone();
        });

        debug!("<APROF> Wake: {:?} -> {:?}", this_name, self.from);
        self.inner_waker.clone().wake();
    }

    fn wake_by_ref(arc_self: &Arc<Self>){

        let mut this_name = None;
        CURRENT_PROFILE_TASK.with(|f| {
            this_name = f.borrow().clone();
        });

        debug!("<APROF> Wake: {:?} -> {:?}", this_name, arc_self.from);
        arc_self.inner_waker.clone().wake();
    }
}

pin_project! {
    pub struct ProfiledTask<Fut> {
        name: usize,
        #[pin]
        fut: Fut,
    }
}

impl<Fut> ProfiledTask<Fut> {
    pub fn new_waker(&self, cx: &mut Context<'_>) -> Waker {
        let pw = Arc::new(ProfileWaker {
            inner_waker : cx.waker().clone(),
            from : self.name,
        });
        waker(pw)
    }

}

impl<Fut: Future> ProfiledTask<Fut> {
    pub fn new(name: usize, fut: Fut) -> ProfiledTask<Fut> {
        ProfiledTask {
            name,
            fut,
        }
    }
}

impl<Fut: Future> Future for ProfiledTask<Fut> {
    type Output = Fut::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>{

        let mut old_name = None;
        CURRENT_PROFILE_TASK.with(|f| {
            old_name = f.borrow().clone();
        });

        // Set the name of the task in thread
        CURRENT_PROFILE_TASK.with(|f| {
            *f.borrow_mut() = Some(self.name);
        });
        debug!("<APROF> Resume task: {:?}", self.name);

        // Make a waker with name
        let w = self.new_waker(cx);
        let mut ct = Context::from_waker(&w);

        let ret = self.as_mut().project().fut.poll(&mut ct);

        // Set the name of the task in thread, back to None
        CURRENT_PROFILE_TASK.with(|f| {
            *f.borrow_mut() = old_name;
        });
        debug!("<APROF> Pause task: {:?}", self.name);

        ret
    }

}

pub fn profile_name_thread(name : &str) {
    let num = TASK_NUM.fetch_add(1, Ordering::Relaxed);

    // Set the name of the task in thread
    CURRENT_PROFILE_TASK.with(|f| {
        *f.borrow_mut() = Some(num);
    });
    debug!("<APROF> Task {:?} from None defined {}", num, name);

}

#[macro_export]
macro_rules! pspawn {
    ($task_name:expr, $x:block) => {

        {
        let num = TASK_NUM.fetch_add(1, Ordering::Relaxed);
        let _h2 = tokio::spawn(ProfiledTask::new(num, async move {
            $x
        }));

        let mut this_name = None;
        CURRENT_PROFILE_TASK.with(|f| {
            this_name = f.borrow().clone();
        });

        let current_file = file!();
        let current_line = line!();
        debug!("<APROF> Task {:?} from {:?} defined {}:{}:{}", num, this_name, $task_name, current_file, current_line);
        // Potentially dump a back-trace here to help identify deeper dependencies?

        _h2
        }

    };
}

// Tokio testing reactor loop (single thread)
#[tokio::test]
async fn test_profile() {

    let (mut tx, mut rx) = channel(10);


    let h1 = tokio::spawn(ProfiledTask::new(0, async move {
        let _x = rx.recv().await;
    }));

    let _h2 = tokio::spawn(ProfiledTask::new(1, async move {
        let _x = tx.send(10usize).await;
    }));

    h1.await.unwrap();

}

#[tokio::test]
async fn test_profile_macro() {

    let (mut tx, mut rx) = channel(10);

    let h1 = pspawn!("Name1", {
        let _x = rx.recv().await;
    });

    let _h2 =  pspawn!("Name2", {
        let _x = tx.send(10usize).await;
    });

    h1.await.unwrap();

}
