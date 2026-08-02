use std::{
    thread,
    time::{Duration, Instant},
};

use tokio::runtime::Runtime;
use worker::worker;

struct BenchResult {
    name: &'static str,
    duration: Duration,
}

fn bench_sync_worker(capacity: usize, count: usize) -> Duration {
    let (tx, rx) = worker::channel::<usize>(capacity);
    let start = Instant::now();

    thread::scope(|s| {
        let tx_for_send = tx.clone();
        let rx_for_recv = rx.clone();

        s.spawn(move || {
            for i in 0..count {
                tx_for_send.send(i).unwrap();
            }
        });

        s.spawn(move || {
            for _ in 0..count {
                rx_for_recv.recv().unwrap();
            }
        });
    });

    start.elapsed()
}

fn bench_sync_flume(capacity: Option<usize>, count: usize) -> Duration {
    let (tx, rx) = match capacity {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };

    let start = Instant::now();

    thread::scope(|s| {
        let tx_for_send = tx.clone();
        let rx_for_recv = rx.clone();

        s.spawn(move || {
            for i in 0..count {
                tx_for_send.send(i).unwrap();
            }
        });

        s.spawn(move || {
            for _ in 0..count {
                rx_for_recv.recv().unwrap();
            }
        });
    });

    start.elapsed()
}

fn bench_async_worker(capacity: usize, count: usize) -> Duration {
    let (tx, rx) = worker::channel::<usize>(capacity);
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();

    runtime.block_on(async {
        let send_task = tokio::spawn(async move {
            for i in 0..count {
                tx.send_async(i).await.unwrap();
            }
        });

        let recv_task = tokio::spawn(async move {
            for _ in 0..count {
                rx.recv_async().await.unwrap();
            }
        });

        send_task.await.unwrap();
        recv_task.await.unwrap();
    });

    start.elapsed()
}

fn bench_async_flume(capacity: Option<usize>, count: usize) -> Duration {
    let (tx, rx) = match capacity {
        Some(cap) => flume::bounded::<usize>(cap),
        None => flume::unbounded::<usize>(),
    };

    let runtime = Runtime::new().unwrap();
    let start = Instant::now();

    runtime.block_on(async {
        let send_task = tokio::spawn(async move {
            for i in 0..count {
                tx.send_async(i).await.unwrap();
            }
        });

        let recv_task = tokio::spawn(async move {
            for _ in 0..count {
                rx.recv_async().await.unwrap();
            }
        });

        send_task.await.unwrap();
        recv_task.await.unwrap();
    });

    start.elapsed()
}

fn print_results(title: &str, results: &[BenchResult], count: usize) {
    println!("\n### {title}");
    println!("| Channel | Time (ms) | Throughput (msg/s) |");
    println!("|---------|-----------|-------------------|");

    for result in results {
        let through = if result.duration.as_secs_f64() > 0.0 {
            (count as f64 / result.duration.as_secs_f64()) as usize
        } else {
            0
        };
        let ms = format!("{:.3}", result.duration.as_secs_f64() * 1000.0);
        println!("| {:<20} | {:>9} | {:>18} |", result.name, ms, through);
    }
}

fn main() {
    let count = std::env::var("WORKER_BENCH_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000);

    let worker_sync_bounded = bench_sync_worker(1024, count);
    let flume_sync_bounded = bench_sync_flume(Some(1024), count);
    let worker_sync_unbounded = bench_sync_worker(count + 1, count);
    let flume_sync_unbounded = bench_sync_flume(None, count);

    print_results(
        "Synchronous channels",
        &[
            BenchResult {
                name: "worker (bounded)",
                duration: worker_sync_bounded,
            },
            BenchResult {
                name: "flume (bounded)",
                duration: flume_sync_bounded,
            },
            BenchResult {
                name: "worker (large capacity)",
                duration: worker_sync_unbounded,
            },
            BenchResult {
                name: "flume (unbounded)",
                duration: flume_sync_unbounded,
            },
        ],
        count,
    );

    let worker_async_bounded = bench_async_worker(1024, count);
    let flume_async_bounded = bench_async_flume(Some(1024), count);
    let worker_async_unbounded = bench_async_worker(count + 1, count);
    let flume_async_unbounded = bench_async_flume(None, count);

    print_results(
        "Asynchronous channels",
        &[
            BenchResult {
                name: "worker (bounded)",
                duration: worker_async_bounded,
            },
            BenchResult {
                name: "flume (bounded)",
                duration: flume_async_bounded,
            },
            BenchResult {
                name: "worker (large capacity)",
                duration: worker_async_unbounded,
            },
            BenchResult {
                name: "flume (unbounded)",
                duration: flume_async_unbounded,
            },
        ],
        count,
    );
}
