# worker

A lightweight channel implementation with both synchronous and asynchronous send/receive support.

## Overview

This crate exposes a `worker::channel` API that provides:

- synchronous `send` / `recv`
- asynchronous `send_async` / `recv_async`
- bounded and large-capacity channel variants for basic performance comparisons

## Benchmarks

The repository includes a benchmark target at [benches/channel_benchmark.rs](benches/channel_benchmark.rs) that compares the crate’s worker channel against flume for both synchronous and asynchronous message passing.

### Run the benchmark

From the repository root:

```bash
cargo bench --bench channel_benchmark
```

You can also control the message count with:

```bash
WORKER_BENCH_COUNT=1000 cargo bench --bench channel_benchmark
```

### Example output

```text
### Synchronous channels
| Channel | Time (ms) | Throughput (msg/s) |
|---------|-----------|-------------------|
| worker (bounded)     |     0.323 |            3094327 |
| flume (bounded)      |     0.114 |            8804754 |
| worker (large capacity) |     0.212 |            4715602 |
| flume (unbounded)    |     0.103 |            9750865 |

### Asynchronous channels
| Channel | Time (ms) | Throughput (msg/s) |
|---------|-----------|-------------------|
| worker (bounded)     |     0.229 |            4374969 |
| flume (bounded)      |     0.118 |            8453514 |
| worker (large capacity) |     0.205 |            4868335 |
| flume (unbounded)    |     0.190 |            5256794 |
```

## Development

Run the test suite with:

```bash
cargo test
```
