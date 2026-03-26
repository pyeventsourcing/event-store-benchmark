[![Logo](images/banner-v2-1280x640.png)](https://)


# Event Store Benchmark Suite

A rigorous, reproducible, open-source benchmark framework for evaluating event sourcing databases.

This project exists to define a **credible performance standard** for event stores — one that measures real-world behavior under realistic workloads, not synthetic best-case scenarios.

This project is implemented with Rust and Python:

* **es-bench** — workload execution implemented in Rust 
* **report_generator.py** — analysis and visualization in Python


# Quick Start

Clone the `event-store-benchmark` repository from GitHub.

```bash
git clone https://github.com/pyeventsourcing/event-store-benchmark.git
```

Install the Rust toolchain (you will need `cargo`), the protobuf compiler, and Python 3.11+.

Then, create a Python virtual environment for report generation, and build the benchmark tool.

For convenience, a `Makefile` is provided to simplify common tasks.

- **Print available Makefile targets**: `make help`
- **Make a Python virtual environment**: `make venv`
- **Build the benchmark tool**: `make build`
- **Run the 'smoke test' workload**: `make run-smoke-test`
- **Run the 'scaling readers' workload**: `make scaling-readers`
- **Run the 'scaling writers' workload**: `make scaling-writers`
- **Generate HTML reports**: `make report`
- **Read HTML reports**: Open `results/published/index.html` in your brower


# Why This Exists

Most existing benchmarks for event stores:

* Measure only peak append throughput
* Ignore latency percentiles
* Skip recovery and crash behavior
* Do not model realistic workload shapes
* Are difficult to reproduce
* Favor a specific implementation

This project aims to correct that.

We treat benchmarking as an engineering discipline — not a marketing exercise.

# Core Principles

This benchmark suite is built around the following principles:

## 1. Workload Realism

Benchmarks must model real event-sourced applications:

* Many small streams
* Some hot streams
* Heavy-tailed (Zipf-like) distributions
* Tag/category filtering
* Concurrent writers
* Catch-up subscribers
* Mixed read/write workloads

Synthetic “write 1 million events to one stream” tests are insufficient.


## 2. Percentiles Over Averages

We measure latency percentiles using the HDR (high dynamic range) Histogram.

Average throughput alone is misleading.

Latency distribution under contention is what matters.


## 3. Reproducibility

All benchmarks must be:

* Deterministic (fixed random seeds)
* Configurable via versioned YAML definitions
* Hardware documented
* OS and fsync mode documented
* Repeatable across environments

Raw results must be published alongside summarized results.


## 4. Store-Neutral Design

The benchmark must not favor a specific implementation.

Adapters are used to interface with different systems, but workloads are defined independently of implementation details.


## Metrics

Benchmark runs capture:

* **Throughput**: Events per second
* **Latency percentiles**: p50, p95, p99, p999
* **Container metrics**: CPU, memory, startup time
* **Raw samples**: Per-operation timing data
* **Environment**: Hardware, OS, disk, runtime info
* **Reproducibility**: Git commit hash, seed, exact config

Each published result must document:

* CPU model
* Core count
* RAM
* Disk type (NVMe, SSD, HDD)
* Filesystem
* OS version
* Fsync configuration
* Kernel tuning (if any)
* Store configuration


# Architecture Overview

## Rust Layer — Benchmark Engine

Responsible for:

* Event store adaption
* Workload execution
* Raw metrics output

No analysis logic lives in Rust — only measurement.

### Adapter Model

Event stores are adapted using common Rust traits:

```rust
trait StoreManager {
    /// Start the container and return success status
    async fn start(&mut self) -> anyhow::Result<()>;

    /// Stop and cleanup the container
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Get the container ID for stats collection (if applicable)
    fn container_id(&self) -> Option<String>;
    
    /// Store name (adapter name)
    fn name(&self) -> &'static str;

    /// Create a new adapter instance (client)
    fn create_adapter(&self) -> anyhow::Result<Arc<dyn EventStoreAdapter>>;
}

trait EventStoreAdapter {
    /// Append an event
    async fn append(&self, evt: EventData) -> anyhow::Result<()>;

    /// Read events
    async fn read(&self, req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>>;
}
```

This allows the same workload to run across different systems.

### Adapted Event Stores

In alphabetical order:

* Axon Server
* EventsourcingDB
* KurrentDB
* UmaDB

### Workload Types

The benchmark supports four workload categories:

#### 1. Performance Workloads
Generic event store usage patterns with configurable concurrency and operations:
- **Write mode**: Concurrent writers appending events
- **Read mode**: Concurrent readers consuming events
- **Mixed mode**: Combined read/write operations

#### 2. Durability Workloads *(stub)*
Testing persistence guarantees:
- Crash recovery testing
- fsync timing analysis
- WAL replay verification

#### 3. Consistency Workloads *(stub)*
Testing correctness guarantees:
- Optimistic concurrency conflict detection
- Read-after-write verification
- Event ordering validation

#### 4. Operational Workloads *(stub)*
Testing operational characteristics:
- Startup/shutdown performance
- Backup/restore speed
- Storage growth measurement

### Named Workload Configurations

Each workload is defined by a named YAML file. The `name` field identifies the workload, and `workload_type` specifies which implementation to use.

### Example: Smoke Test

```yaml
# configs/smoke-test.yaml
name: smoke-test
workload_type: performance
mode: write
duration_seconds: 10
concurrency:
  writers: [1, 4]
operations:
  write:
    event_size_bytes: 256
stores: [umadb, dummy]
```

### Example: Scaling Writers

```yaml
# configs/scaling/writers.yaml
name: scaling-writers
workload_type: performance
mode: write
duration_seconds: 120
concurrency:
  writers: [1, 2, 4, 8, 16, 32]
operations:
  write:
    event_size_bytes: 256
stores: [umadb, kurrentdb, axonserver, eventsourcingdb]
```

### Example: Read Workload

```yaml
# configs/concurrent_readers.yaml
name: scaling-readers
workload_type: performance
mode: read
duration_seconds: 6
concurrency:
  readers: [1, 2, 4, 8, 16, 32]
operations:
  write:
    event_size_bytes: 256
  read:
    batch_size: 100
setup:
  prepopulate_events: 50000
  prepopulate_streams: 5000
stores: [umadb, kurrentdb, axonserver, eventsourcingdb]
```

## Python Layer — Analysis & Visualization

Responsible for:

* Aggregating benchmark runs
* Computing statistical comparisons
* Plotting latency distributions
* Generating tables for publication
* Producing PDF/HTML reports
* Detecting regressions between runs

### Publishing Results

Published benchmark reports must include:

* Workload definition
* Raw metrics
* Summary tables
* Latency distribution graphs
* Environment specification
* Exact commit hash of benchmark suite
* Exact version of target system

Transparency is mandatory.

# Non-Goals

This benchmark suite does not:

* Optimize systems for artificial workloads
* Hide durability settings
* Benchmark in-memory-only configurations
* Publish results without reproducibility metadata
* Declare “winners”

The goal is measurement, not marketing.


# Contribution Guidelines

Contributions are welcome for:

* New workload definitions
* New system adapters
* Improved statistical analysis
* Improved reporting templates
* Environment automation scripts

All contributions must preserve:

* Determinism
* Reproducibility
* Neutrality


# Long-Term Vision

This project aims to become:

* A reference benchmark for event sourcing systems
* A research-grade measurement framework
* A regression detection tool for event store developers
* A shared standard for comparing durability and performance trade-offs

If adopted broadly, this could meaningfully improve the quality of performance claims in the event sourcing ecosystem.


# License

Open source under MIT.
