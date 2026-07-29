use ::worker::worker::channel;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use worker::worker;

// --- Tracking Allocator Setup ---

struct TrackingAllocator;

static TOTAL_ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static TOTAL_DEALLOCATED: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static CURRENT_ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            TOTAL_ALLOCATED.fetch_add(size, Ordering::SeqCst);
            ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
            let current = CURRENT_ALLOCATED.fetch_add(size, Ordering::SeqCst) + size;
            loop {
                let peak = PEAK_ALLOCATED.load(Ordering::SeqCst);
                if current <= peak {
                    break;
                }
                if PEAK_ALLOCATED
                    .compare_exchange_weak(peak, current, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        let size = layout.size();
        TOTAL_DEALLOCATED.fetch_add(size, Ordering::SeqCst);
        CURRENT_ALLOCATED.fetch_sub(size, Ordering::SeqCst);
    }
}

#[global_allocator]
static A: TrackingAllocator = TrackingAllocator;

// --- Benchmark Result Struct ---

struct BenchResult {
    name: &'static str,
    duration: Duration,
    allocations: usize,
    allocated_bytes: usize,
    peak_memory: usize,
}

// --- Worker Channel Benchmarks ---

fn run_worker_spsc(capacity: usize, count: usize) -> (Duration, usize, usize, usize) {
    // Warmup / setup allocator state
    let (tx, rx) = channel::<usize>(capacity);
    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..count {
                tx.send(i);
            }
        });

        s.spawn(|| {
            let mut received = 0;
            while received < count {
                if let Some(mut buf) = rx.recv() {
                    received += buf.len();
                    buf.clear();
                    rx.ack(buf);
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_worker_mpsc(
    capacity: usize,
    count: usize,
    num_senders: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = worker::new_channel::<usize>(capacity);
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    tx.send(i);
                }
            });
        }
        drop(tx);

        s.spawn(|| {
            let mut received = 0;
            while received < total_expected {
                if let Some(mut buf) = rx.recv() {
                    received += buf.len();
                    buf.clear();
                    rx.ack(buf);
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_worker_spmc(
    capacity: usize,
    count: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = worker::new_channel::<usize>(capacity);
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..count {
                tx.send(i);
            }
        });

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                loop {
                    if total_received.load(Ordering::Relaxed) >= count {
                        break;
                    }
                    if let Some(mut buf) = rx.recv() {
                        let len = buf.len();
                        total_received.fetch_add(len, Ordering::Relaxed);
                        buf.clear();
                        rx.ack(buf);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_worker_mpmc(
    capacity: usize,
    count: usize,
    num_senders: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = worker::new_channel::<usize>(capacity);
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    tx.send(i);
                }
            });
        }
        drop(tx);

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                loop {
                    if total_received.load(Ordering::Relaxed) >= total_expected {
                        break;
                    }
                    if let Some(mut buf) = rx.recv() {
                        let len = buf.len();
                        total_received.fetch_add(len, Ordering::Relaxed);
                        buf.clear();
                        rx.ack(buf);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

// --- Flume Channel Benchmarks (Blocking) ---

fn run_flume_blocking_spsc(
    bounded: Option<usize>,
    count: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..count {
                let _ = tx.send(i);
            }
        });

        s.spawn(|| {
            let mut received = 0;
            while received < count {
                if let Ok(_) = rx.recv() {
                    received += 1;
                } else {
                    break;
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_blocking_mpsc(
    bounded: Option<usize>,
    count: usize,
    num_senders: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    let _ = tx.send(i);
                }
            });
        }
        drop(tx);

        s.spawn(|| {
            let mut received = 0;
            while received < total_expected {
                if let Ok(_) = rx.recv() {
                    received += 1;
                } else {
                    break;
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_blocking_spmc(
    bounded: Option<usize>,
    count: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..count {
                let _ = tx.send(i);
            }
        });

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                while total_received.load(Ordering::Relaxed) < count {
                    if let Ok(_) = rx.recv() {
                        total_received.fetch_add(1, Ordering::Relaxed);
                    } else {
                        break;
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_blocking_mpmc(
    bounded: Option<usize>,
    count: usize,
    num_senders: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    let _ = tx.send(i);
                }
            });
        }
        drop(tx);

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                while total_received.load(Ordering::Relaxed) < total_expected {
                    if let Ok(_) = rx.recv() {
                        total_received.fetch_add(1, Ordering::Relaxed);
                    } else {
                        break;
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

// --- Flume Channel Benchmarks (Polling/Spinning) ---

fn run_flume_polling_spsc(bounded: Option<usize>, count: usize) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..count {
                let _ = tx.send(i);
            }
        });

        s.spawn(|| {
            let mut received = 0;
            while received < count {
                if let Ok(_) = rx.try_recv() {
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_polling_mpsc(
    bounded: Option<usize>,
    count: usize,
    num_senders: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    let _ = tx.send(i);
                }
            });
        }
        drop(tx);

        s.spawn(|| {
            let mut received = 0;
            while received < total_expected {
                if let Ok(_) = rx.try_recv() {
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_polling_spmc(
    bounded: Option<usize>,
    count: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..count {
                let _ = tx.send(i);
            }
        });

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                while total_received.load(Ordering::Relaxed) < count {
                    if let Ok(_) = rx.try_recv() {
                        total_received.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_polling_mpmc(
    bounded: Option<usize>,
    count: usize,
    num_senders: usize,
    num_receivers: usize,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };
    let items_per_sender = count / num_senders;
    let total_expected = items_per_sender * num_senders;
    let total_received = Arc::new(AtomicUsize::new(0));

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        for _ in 0..num_senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..items_per_sender {
                    let _ = tx.send(i);
                }
            });
        }
        drop(tx);

        for _ in 0..num_receivers {
            let rx = rx.clone();
            let total_received = total_received.clone();
            s.spawn(move || {
                while total_received.load(Ordering::Relaxed) < total_expected {
                    if let Ok(_) = rx.try_recv() {
                        total_received.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
        }
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_worker_busy_receiver(
    capacity: usize,
    count: usize,
    sleep_ms: u64,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = worker::new_channel::<usize>(capacity);

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..count {
                tx.send(i);
            }
        });

        s.spawn(|| {
            let mut received = 0;
            while received < count {
                if let Some(mut buf) = rx.recv() {
                    received += buf.len();
                    // Simulate processing delay
                    std::thread::sleep(Duration::from_millis(sleep_ms));
                    buf.clear();
                    rx.ack(buf);
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

fn run_flume_busy_receiver(
    bounded: Option<usize>,
    count: usize,
    sleep_ms: u64,
) -> (Duration, usize, usize, usize) {
    let (tx, rx) = match bounded {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };

    let start_current = CURRENT_ALLOCATED.load(Ordering::SeqCst);
    PEAK_ALLOCATED.store(start_current, Ordering::SeqCst);
    let start_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let start_count = ALLOC_COUNT.load(Ordering::SeqCst);

    let start_time = std::time::Instant::now();

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..count {
                let _ = tx.send(i);
            }
        });

        s.spawn(|| {
            let mut received = 0;
            while received < count {
                let mut batch_count = 0;
                while let Ok(_) = rx.try_recv() {
                    batch_count += 1;
                    received += 1;
                    if received == count {
                        break;
                    }
                }

                if batch_count > 0 {
                    // Simulate processing delay
                    std::thread::sleep(Duration::from_millis(sleep_ms));
                } else {
                    std::hint::spin_loop();
                }
            }
        });
    });

    let duration = start_time.elapsed();
    let end_total = TOTAL_ALLOCATED.load(Ordering::SeqCst);
    let end_count = ALLOC_COUNT.load(Ordering::SeqCst);
    let end_peak = PEAK_ALLOCATED.load(Ordering::SeqCst);

    (
        duration,
        end_count - start_count,
        end_total - start_total,
        if end_peak >= start_current {
            end_peak - start_current
        } else {
            0
        },
    )
}

// --- Benchmark Runner Helpers ---

fn print_results(title: &str, results: &[BenchResult], total_messages: usize) {
    println!("\n### {}\n", title);
    println!(
        "| Channel Type | Time (ms) | Throughput (msgs/sec) | Allocations | Cumulative Mem (MB) | Peak Heap Mem (KB) |"
    );
    println!(
        "|--------------|-----------|----------------------|-------------|---------------------|--------------------|"
    );
    for r in results {
        let ms = r.duration.as_millis();
        let throughput = if r.duration.as_secs_f64() > 0.0 {
            (total_messages as f64 / r.duration.as_secs_f64()) as usize
        } else {
            0
        };
        let mb = r.allocated_bytes as f64 / 1024.0 / 1024.0;
        let peak_kb = r.peak_memory as f64 / 1024.0;
        println!(
            "| {:<28} | {:>9} | {:>20} | {:>11} | {:>19.3} | {:>18.3} |",
            r.name, ms, throughput, r.allocations, mb, peak_kb
        );
    }
}

fn main() {
    let count = 1_000_000;
    println!(
        "Running Channel Benchmarks with {} messages per topology",
        count
    );

    // --- SPSC Topology (1 Sender, 1 Receiver) ---
    {
        let mut results = Vec::new();

        let (d, a, b, p) = run_worker_spsc(1000, count);
        results.push(BenchResult {
            name: "Worker (Cap 1,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_worker_spsc(10000, count);
        results.push(BenchResult {
            name: "Worker (Cap 10,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spsc(Some(1000), count);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spsc(Some(10000), count);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spsc(None, count);
        results.push(BenchResult {
            name: "Flume Unbounded (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spsc(Some(1000), count);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spsc(Some(10000), count);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spsc(None, count);
        results.push(BenchResult {
            name: "Flume Unbounded (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        print_results("SPSC Topology (1 Sender, 1 Receiver)", &results, count);
    }

    // --- MPSC Topology (4 Senders, 1 Receiver) ---
    {
        let mut results = Vec::new();
        let senders = 4;

        let (d, a, b, p) = run_worker_mpsc(1000, count, senders);
        results.push(BenchResult {
            name: "Worker (Cap 1,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_worker_mpsc(10000, count, senders);
        results.push(BenchResult {
            name: "Worker (Cap 10,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpsc(Some(1000), count, senders);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpsc(Some(10000), count, senders);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpsc(None, count, senders);
        results.push(BenchResult {
            name: "Flume Unbounded (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpsc(Some(1000), count, senders);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpsc(Some(10000), count, senders);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpsc(None, count, senders);
        results.push(BenchResult {
            name: "Flume Unbounded (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        print_results("MPSC Topology (4 Senders, 1 Receiver)", &results, count);
    }

    // --- SPMC Topology (1 Sender, 4 Receivers) ---
    {
        let mut results = Vec::new();
        let receivers = 4;

        let (d, a, b, p) = run_worker_spmc(1000, count, receivers);
        results.push(BenchResult {
            name: "Worker (Cap 1,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_worker_spmc(10000, count, receivers);
        results.push(BenchResult {
            name: "Worker (Cap 10,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spmc(Some(1000), count, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spmc(Some(10000), count, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_spmc(None, count, receivers);
        results.push(BenchResult {
            name: "Flume Unbounded (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spmc(Some(1000), count, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spmc(Some(10000), count, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_spmc(None, count, receivers);
        results.push(BenchResult {
            name: "Flume Unbounded (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        print_results("SPMC Topology (1 Sender, 4 Receivers)", &results, count);
    }

    // --- MPMC Topology (4 Senders, 4 Receivers) ---
    {
        let mut results = Vec::new();
        let senders = 4;
        let receivers = 4;

        let (d, a, b, p) = run_worker_mpmc(1000, count, senders, receivers);
        results.push(BenchResult {
            name: "Worker (Cap 1,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_worker_mpmc(10000, count, senders, receivers);
        results.push(BenchResult {
            name: "Worker (Cap 10,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpmc(Some(1000), count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpmc(Some(10000), count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_blocking_mpmc(None, count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Unbounded (Block)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpmc(Some(1000), count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 1,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpmc(Some(10000), count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Bounded 10,000 (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_polling_mpmc(None, count, senders, receivers);
        results.push(BenchResult {
            name: "Flume Unbounded (Poll)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        print_results("MPMC Topology (4 Senders, 4 Receivers)", &results, count);
    }

    // --- Busy Receiver Scenario ---
    {
        let mut results = Vec::new();
        let sleep_ms = 5;

        let (d, a, b, p) = run_worker_busy_receiver(1000, count, sleep_ms);
        results.push(BenchResult {
            name: "Worker (Cap 1,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_worker_busy_receiver(10000, count, sleep_ms);
        results.push(BenchResult {
            name: "Worker (Cap 10,000)",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_busy_receiver(Some(1000), count, sleep_ms);
        results.push(BenchResult {
            name: "Flume Bounded 1,000",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_busy_receiver(Some(10000), count, sleep_ms);
        results.push(BenchResult {
            name: "Flume Bounded 10,000",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        let (d, a, b, p) = run_flume_busy_receiver(None, count, sleep_ms);
        results.push(BenchResult {
            name: "Flume Unbounded",
            duration: d,
            allocations: a,
            allocated_bytes: b,
            peak_memory: p,
        });

        print_results(
            &format!("Busy Receiver Scenario ({}ms processing delay)", sleep_ms),
            &results,
            count,
        );
    }
}
