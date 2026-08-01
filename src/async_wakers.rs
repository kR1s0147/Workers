use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
    thread,
};

use crate::worker::WakeSignal;

pub struct AsyncWaker {
    lock: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    task_waker: Arc<Waker>,
}

impl Clone for AsyncWaker {
    fn clone(&self) -> Self {
        Self {
            lock: self.lock.clone(),
            woken: self.woken.clone(),
            task_waker: self.task_waker.clone(),
        }
    }
}

impl WakeSignal for AsyncWaker {
    fn wake(&self) -> bool {
        self.lock();
        self.woken.store(true, Ordering::SeqCst);
        self.task_waker.wake_by_ref();
        self.unlock();
        true
    }
}

impl Drop for AsyncWaker {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::SeqCst);
    }
}

impl AsyncWaker {
    pub fn new(waker: Waker) -> Self {
        Self {
            lock: Arc::new(AtomicBool::new(false)),
            woken: Arc::new(AtomicBool::new(false)),
            task_waker: Arc::new(waker),
        }
    }
    pub fn lock(&self) {
        while let Err(_) =
            self.lock
                .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
        {
            thread::yield_now();
        }
    }

    pub fn unlock(&self) {
        self.lock.store(false, Ordering::SeqCst);
    }

    pub fn update_waker(&mut self, waker: Waker) {
        self.lock();
        if self.woken.load(Ordering::SeqCst) {
            waker.wake_by_ref();
        }
        self.task_waker = Arc::new(waker);
        self.unlock();
    }
}
