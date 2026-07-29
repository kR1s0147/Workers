use std::{cell::UnsafeCell, sync::atomic::AtomicBool, thread};

use crate::worker::WakeSignal;

pub struct WakersGuard<'a> {
    pub wakers: &'a Wakers,
}

impl Drop for WakersGuard<'_> {
    fn drop(&mut self) {
        self.wakers.release();
    }
}

pub struct Wakers {
    lock: AtomicBool,
    pub wakers: UnsafeCell<Vec<Box<dyn WakeSignal>>>,
}

impl Wakers {
    /// Create a new wakers object
    pub fn new() -> Self {
        Self {
            lock: AtomicBool::new(false),
            wakers: UnsafeCell::new(Vec::new()),
        }
    }

    /// Implement a spin lock
    pub fn lock<'a>(&'a self) -> WakersGuard<'a> {
        loop {
            for _ in 0..100 {
                if !self
                    .lock
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return WakersGuard { wakers: self };
                }
                std::hint::spin_loop();
            }
            thread::yield_now();
        }
    }

    /// Release the lock (automatically called on drop on WakersGuard)
    fn release(&self) {
        self.lock.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
