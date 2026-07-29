use std::thread::{self, Thread};

use crate::worker::WakeSignal;

pub struct SyncWaker {
    handle: Thread,
}

impl WakeSignal for SyncWaker {
    fn wake(&self) -> bool {
        self.handle.unpark();
        true
    }
}

impl SyncWaker {
    pub fn new() -> Self {
        Self {
            handle: thread::current(),
        }
    }
}
