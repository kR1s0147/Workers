pub mod async_chan;
pub mod async_wakers;
pub mod syncwaker;
pub mod wakers;
pub mod worker;

// Implement Dymitri Vyukov's algorithm for lock-free queue.
// Should support the sync and async version of the channels.
