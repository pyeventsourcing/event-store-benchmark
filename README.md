[![Logo](images/banner-v2-1280x640.png)](https://)


# Event Store Benchmark Suite

A rigorous, reproducible, open-source benchmark framework for evaluating event sourcing databases.

This project exists to define a **credible performance standard** for event stores — one that measures real-world behavior under realistic workloads, not synthetic best-case scenarios.

This project is implemented with Rust and Python.

# Quick Start

Clone the project repository from GitHub.

```bash
git clone https://github.com/pyeventsourcing/event-store-benchmark.git
```

Install the Rust toolchain, the protobuf compiler, and Python 3.11+.

Then, create a Python virtual environment (for report generation) and build the benchmark tool.

For convenience, a `Makefile` is provided to simplify common tasks.

- **Make a Python virtual environment**: `make venv`
- **Build the benchmark tool**: `make build`
- **Run the 'smoke test' workload**: `make run-smoke-test`
- **Run the 'scaling readers' workload**: `make run-scaling-readers`
- **Run the 'scaling writers' workload**: `make run-scaling-writers`
- **Generate HTML reports**: `make report`
- **Read HTML reports**: Open `results/published/index.html` in your brower
- **Print available Makefile targets**: `make help`


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
    async fn append(&self, events: Vec<EventData>) -> anyhow::Result<()>;

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

### Level Playing Ground (Technical Summary)

To ensure fair and reproducible comparisons between KurrentDB, Axon Server, and UmaDB, the benchmark suite establishes
a "level playing ground" by standardizing low-level transport settings and client instantiation strategies.

#### 1. Standardized gRPC Transport Settings

All three adapters utilize the `tonic` gRPC library in Rust, and they are configured with identical network and flow control parameters:

* **TCP NoDelay**: Set to `true` for all clients. This disables Nagle's algorithm, ensuring that small packets (like event append requests) are sent immediately, which is critical for accurate latency measurement.
* **HTTP/2 Keep-Alive**: Configured with a 5-second interval and a 10-second timeout.
* **Window Sizes (Flow Control)**: 
    * **Initial Stream Window Size**: 4 MB (`4 * 1024 * 1024`).
    * **Initial Connection Window Size**: 8 MB (`8 * 1024 * 1024`).
    * These enlarged window sizes prevent the benchmark from being throttled by default small gRPC flow-control limits, allowing higher throughput over single connections.

#### 2. Client Instance Management
The benchmark follows a consistent "One Client Per Worker" model:
* **Independent Connections**: Each worker task (reader or writer) establishes its own independent gRPC connection during initialization.
* **Adapter Instances**: When the benchmark starts worker tasks, it calls `create_adapter()` for each worker.
    * For **KurrentDB**, **Axon Server**, and **UmaDB**, this creates a new instance of the respective minimalist gRPC client, which establishes a dedicated connection to the store.
* **Concurrency**: The number of these connections is strictly controlled by the `concurrency` settings in the benchmark configuration (e.g., `writers: [1, 4]`), ensuring that all databases are tested with the same number of active client connections.

#### 3. Implementation Consistency
* **Minimalist Clients**: The suite uses "minimal" gRPC client implementations for KurrentDB. This implementation strips away high-level background state machines or complex coordination logic found in the official client SDK, ensuring the benchmark measures the database's performance rather than the client library's overhead. A Rust gRPC client for Axon Server has also been implemented with the same design, because no official SDK exists.
* **Standardized Payload**: All adapters transform the internal `EventData` (binary payload + type + tags) into their respective proto formats just before the gRPC call, keeping the transformation overhead comparable across all tests.

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

Each workload is defined by a named YAML file. The top-level key (e.g., `performance:`) specifies the workload type, and all configuration is nested within it.

### Example: Smoke Test

```yaml
# configs/smoke-test.yaml
performance:
  name: smoke-test
  mode: write
  duration_seconds: 10
  concurrency:
    writers: [1, 4]
  operations:
    write:
      event_size_bytes: 256
  stores:
    - umadb
    - dummy
```

### Example: Scaling Writers

```yaml
# configs/scaling/writers.yaml
performance:
  name: scaling-writers
  mode: write
  duration_seconds: 120
  concurrency:
    writers: [1, 2, 4, 8, 16, 32]
  operations:
    write:
      event_size_bytes: 256
  stores:
    - umadb
    - kurrentdb
    - axonserver
    - eventsourcingdb
```

### Example: Read Workload

```yaml
# configs/scaling/readers.yaml
performance:
  name: scaling-readers
  mode: read
  duration_seconds: 6
  concurrency:
    readers: [1, 2, 4, 8, 16, 32]
  operations:
    write:
      event_size_bytes: 256
    read:
      limit: 100
  setup:
    prepopulate_events: 50000
    prepopulate_streams: 5000
  stores:
    - umadb
    - kurrentdb
    - axonserver
    - eventsourcingdb
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

### Performance Workload Data Analysis

The benchmark suite generates a comprehensive set of reports for performance workloads, transforming raw measurements into actionable insights through three levels of analysis.

#### 1. Data Collection
During each benchmark run, the engine collects high-frequency samples of:
*   **Throughput**: The number of events appended or read per second.
*   **Latency**: Exact timing for every individual operation, used to build high-resolution distribution profiles (p50 to p99.9).
*   **Resource Usage**: CPU and Memory consumption of both the database store and the benchmark process.

#### 2. Individual Run Reports
For every combination of database and worker count, a detailed report is generated. This is the most granular view, showing the raw "shape" of a single run. It helps identify if a specific store is stable or if it suffers from periodic background tasks like compaction or garbage collection.

#### 3. Worker Slices (Comparing Stores)
To compare different databases fairly, we "slice" the entire dataset by **Worker Count** (concurrency). A Worker Slice presents all databases side-by-side at a fixed load (e.g., "8 Readers").
*   **Purpose**: Shows which database performs best under specific conditions.
*   **Visuals**: Comparative time-series plots and Latency CDFs (Cumulative Distribution Functions).
*   **Insight**: Identifies which store has the most consistent performance and the lowest tail latency at a given concurrency level.

#### 4. Grouping By Workers (Trend Analysis)
Finally, we group all runs together to observe how the workflow performs as a whole. This analysis "collapses" the time dimension of each run into summary statistics and plots them against the worker count.
*   **Purpose**: Reveals how well a database scales as you add more concurrent users.
*   **Visuals**: Trend lines showing how throughput increases (or plateaus) and how latency increases as the system is loaded.
*   **Insight**: Helps determine the saturation point and the efficiency of each database as it scales.

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
