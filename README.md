[![Logo](images/banner-v2-1280x640.png)](https://)


# Event Store Benchmark Suite

A rigorous, reproducible, open-source benchmark framework for evaluating event sourcing databases.

This project exists to define a **credible performance standard** for event stores — one that measures real-world behavior under realistic workloads, not synthetic best-case scenarios.

This project is implemented with Rust and Python:

* **es-bench** — workload execution implemented in Rust 
* **report_generator.py** — analysis and visualization in Python

# Getting Started

1. Clone the repository
2. Install Rust (stable toolchain)
3. Install Python 3.11+
4. Run sample workloads
5. Generate example reports
6. Validate results on your hardware

## Clone the Repository

There are many different ways to clone the repository on GitHub.

```bash
git clone https://github.com/pyeventsourcing/event-store-benchmark.git
```

## Install Rust

To build the `es-bench` executable, you will need `cargo` and other build tools such as `protoc`.

# Quick Start with Makefile

For convenience, a `Makefile` is provided to simplify common tasks.

- **Build the CLI**: `make build`
- **Setup Python Environment**: `make venv`
- **Run a benchmark**: `make run smoke-test`, `make smoke-test`, or `make configs/scaling/writers.yaml`
- **Generate reports**: `make report`

# CLI Commands

The benchmark CLI is `es-bench`. Build it first:

```bash
cargo build --release
```

## list-stores

List available store adapters:

```bash
./target/release/es-bench list-stores
```

Output:
```
dummy
umadb
kurrentdb
axonserver
eventsourcingdb
```

## run

Execute a workload against one or more event stores:

```bash
./target/release/es-bench run --config <CONFIG> [--seed <SEED>]
```

Parameters:
- `--config <path>`: Path to workload YAML configuration file
- `--seed <u64>`: Deterministic RNG seed (optional, defaults to random)
- `--log <level>`: Log verbosity: `trace`, `debug`, `info`, `warn`, `error` (default: `info`)

### Examples

**Run a simple write workload:**
```bash
./target/release/es-bench run --config configs/baseline-writes-w4.yaml --seed 42
```

This will:
- Run the workload against all stores (or stores specified in the config)
- Generate a session ID based on the current timestamp
- Collect system environment information
- Execute the workload for each store
- Write results to `results/raw/sessions/{timestamp}/`

**Run a read workload:**
```bash
./target/release/es-bench run --config configs/baseline-reads-r4.yaml --seed 42
```

**Run with specific stores (specified in config):**
```yaml
# configs/my_workload.yaml
name: my-custom-workload
workload_type: performance
mode: write
stores: [umadb, kurrentdb]  # Only these stores
# ... rest of config
```

### Output Structure

Results are organized by session:

```
results/raw/sessions/
  2026-03-13T20-54-26/              # Session timestamp
    session.json                     # Session metadata (stores, seed, git hash)
    environment.json                 # Hardware/system info
    config.yaml                      # Copy of workload config
    concurrent-writers-w4/           # Workload name
      umadb/
        summary.json                 # Aggregated metrics
        samples.jsonl                # Raw per-operation samples
      kurrentdb/
        summary.json
        samples.jsonl
      dummy/
        summary.json
        samples.jsonl
```



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

We measure:

* p50
* p95
* p99
* p99.9

Average throughput alone is misleading.

Latency distribution under contention is what matters.


## 3. Durability is Not Optional

We explicitly test:

* Crash during write
* Restart and recovery
* WAL replay cost
* Index rebuild time
* Checkpoint recovery

If a store claims durability, it must survive termination mid-transaction.


## 4. Reproducibility

All benchmarks must be:

* Deterministic (fixed random seeds)
* Configurable via versioned YAML definitions
* Hardware documented
* OS and fsync mode documented
* Repeatable across environments

Raw results must be published alongside summarized results.


## 5. Store-Neutral Design

The benchmark must not favor a specific implementation.

Adapters are used to interface with different systems, but workloads are defined independently of implementation details.


# Architecture Overview

## Rust Layer — Benchmark Engine

Responsible for:

* Event store adaption
* Workload execution
* Raw metrics output

No analysis logic lives in Rust — only measurement.

## Python Layer — Analysis & Visualization

Responsible for:

* Aggregating benchmark runs
* Computing statistical comparisons
* Plotting latency distributions
* Generating tables for publication
* Producing PDF/HTML reports
* Detecting regressions between runs

This separation prevents analytical overhead from contaminating benchmark execution.

# Adapter Model

Each target system implements a common Rust trait:

```rust
trait EventStoreAdapter {
    fn append(&self, stream: &str, events: &[Event]) -> Result<()>;
    fn read_stream(&self, stream: &str, from: u64) -> Result<Vec<Event>>;
    fn query_by_tag(&self, tag: &str) -> Result<Vec<Event>>;
    fn crash(&self);
    fn recover(&self);
}
```

This allows the same workload to run across different systems.

# Workload Architecture

Workloads are defined by **named YAML configuration files** that specify both the workload type and its parameters.

## Workload Types

The benchmark supports four workload categories:

### 1. Performance Workloads
Generic event store usage patterns with configurable concurrency and operations:
- **Write mode**: Concurrent writers appending events
- **Read mode**: Concurrent readers consuming events
- **Mixed mode**: Combined read/write operations

### 2. Durability Workloads *(stub)*
Testing persistence guarantees:
- Crash recovery testing
- fsync timing analysis
- WAL replay verification

### 3. Consistency Workloads *(stub)*
Testing correctness guarantees:
- Optimistic concurrency conflict detection
- Read-after-write verification
- Event ordering validation

### 4. Operational Workloads *(stub)*
Testing operational characteristics:
- Startup/shutdown performance
- Backup/restore speed
- Storage growth measurement

## Named Workload Configurations

Each workload is defined by a named YAML file. The `name` field identifies the workload, and `workload_type` specifies which implementation to use.

### Example: Write Workload

```yaml
# configs/concurrent_writers.yaml
name: concurrent-writers-w4
workload_type: performance
mode: write
duration_seconds: 6
concurrency:
  writers: 4
operations:
  write:
    event_size_bytes: 256
streams:
  distribution: uniform  # or "zipf" for heavy-tailed
  count: 10000
```

### Example: Read Workload

```yaml
# configs/concurrent_readers.yaml
name: concurrent-readers-r4
workload_type: performance
mode: read
duration_seconds: 6
concurrency:
  readers: 4
operations:
  read:
    batch_size: 100
streams:
  distribution: uniform
  count: 1000
setup:
  prepopulate_events: 10000
  prepopulate_streams: 1000
```

### Example: Mixed Read/Write Workload

```yaml
# configs/mixed_workload.yaml
name: mixed-70-30
workload_type: performance
mode: mixed
duration_seconds: 60
concurrency:
  writers: 4
  readers: 12
operations:
  write:
    event_size_bytes: 256
    probability: 0.3  # 30% writes
  read:
    batch_size: 50
    probability: 0.7  # 70% reads
streams:
  distribution: zipf
  count: 10000
```

### Workload Naming Convention

Workload names should be descriptive and include key parameters:
- `baseline-writes-w4` - Baseline write performance with 4 writers
- `heavy-reads-zipf-r16` - Read-heavy with Zipf distribution, 16 readers
- `mixed-read-heavy-70-30` - Mixed workload with 70% reads, 30% writes

This naming makes results self-documenting when browsing the results directory.

## Available Workload Configurations

The `configs/` directory includes a variety of example workloads:

### Quick Testing
Fast configuration for smoke tests and development:

- **`smoke-test.yaml`** - 5-second test with 2 writers (quick sanity check)

### Baseline Workloads
Standard performance benchmarks for common scenarios:

- **`baseline-writes-w1.yaml`** - Single writer baseline (raw single-threaded performance)
- **`baseline-writes-w4.yaml`** - 4 concurrent writers (typical production)
- **`baseline-writes-w16.yaml`** - 16 concurrent writers (high-concurrency)
- **`baseline-reads-r4.yaml`** - 4 concurrent readers
- **`baseline-reads-r16.yaml`** - 16 concurrent readers (high read throughput)

### Distribution Variations
Testing skewed access patterns (hot streams):

- **`heavy-writes-zipf-w4.yaml`** - Write with Zipf distribution (hot aggregates)
- **`heavy-reads-zipf-r16.yaml`** - Read with Zipf distribution (popular streams)

### Mixed Workloads
Combined read/write scenarios:

- **`mixed-read-heavy-70-30.yaml`** - 70% reads, 30% writes (read-dominant)
- **`mixed-balanced-50-50.yaml`** - 50% reads, 50% writes (balanced)
- **`mixed-write-heavy-30-70.yaml`** - 30% reads, 70% writes (write-dominant)

### Scaling Sweeps (`scaling/`)
Measure scalability across different dimensions:

- **`scaling/writers.yaml`** - Writer concurrency sweep: [1, 2, 4, 8, 16, 32]
- **`scaling/readers.yaml`** - Reader concurrency sweep: [1, 2, 4, 8, 16, 32]
- **`scaling/event-size.yaml`** - Event size variations (template)

### Real-World Scenarios (`scenarios/`)
Simulate production patterns:

- **`scenarios/microservices-aggregate.yaml`** - Typical microservices event sourcing (8 writers, Zipf, 512-byte events)
- **`scenarios/cqrs-read-model.yaml`** - CQRS pattern (85% reads, 15% writes, 24 readers, 4 writers)
- **`scenarios/high-throughput-ingestion.yaml`** - High-volume data ingestion (32 writers, 50K streams)
- **`scenarios/audit-log.yaml`** - Audit logging pattern (16 writers, uniform distribution)

### Running Examples

```bash
# Quick smoke test (5 seconds)
./target/release/es-bench run --config configs/smoke-test.yaml

# Baseline write performance
./target/release/es-bench run --config configs/baseline-writes-w4.yaml

# Test with hot streams
./target/release/es-bench run --config configs/heavy-writes-zipf-w4.yaml

# CQRS read model simulation
./target/release/es-bench run --config configs/scenarios/cqrs-read-model.yaml

# Scaling analysis (sweep across multiple writer counts and stores)
./target/release/es-bench run --config configs/scaling/writers.yaml
```

# Results Structure

All benchmark results are organized by **session** - a single execution of `es-bench run`.

## Session Directory

```
results/raw/sessions/{ISO-timestamp}/
├── session.json          # Session metadata
├── environment.json      # Hardware/system information
├── config.yaml           # Copy of workload configuration
└── {workload-name}/      # Named workload directory
    ├── {store-1}/
    │   ├── summary.json
    │   └── samples.jsonl
    ├── {store-2}/
    │   ├── summary.json
    │   └── samples.jsonl
    └── ...
```

## Metadata Files

### session.json
Contains session-level metadata for reproducibility:

```json
{
  "session_id": "2026-03-13T20-54-26",
  "benchmark_version": "bb583fd",  // Git commit hash
  "workload_name": "concurrent-writers-w4",
  "workload_type": "performance",
  "config_file": "configs/concurrent_writers.yaml",
  "seed": 42,
  "stores_run": ["umadb", "kurrentdb", "dummy"],
  "is_sweep": false
}
```

### environment.json
Hardware and system information for performance context:

```json
{
  "os": {
    "name": "macOS",
    "version": "26.3",
    "kernel": "Darwin ...",
    "arch": "aarch64"
  },
  "cpu": {
    "model": "Apple M4 Pro",
    "cores": 14
  },
  "memory": {
    "total_bytes": 51539607552
  },
  "disk": {
    "type": "NVMe",
    "filesystem": "apfs"
  },
  "container_runtime": {
    "type": "docker",
    "version": "28.0.4"
  }
}
```

### summary.json
Per-store aggregated metrics:

```json
{
  "workload": "concurrent-writers-w4",
  "adapter": "umadb",
  "writers": 4,
  "readers": 0,
  "events_written": 28923,
  "events_read": 0,
  "duration_s": 6.0,
  "throughput_eps": 4820.5,
  "latency": {
    "p50_ms": 0.65,
    "p95_ms": 1.23,
    "p99_ms": 2.45,
    "p999_ms": 12.8
  },
  "container": {
    "image_size_bytes": 892790634,
    "startup_time_s": 2.38,
    "avg_cpu_percent": 85.3,
    "peak_cpu_percent": 142.1,
    "avg_memory_bytes": 653291520,
    "peak_memory_bytes": 721420288
  }
}
```

### samples.jsonl
Raw per-operation measurements (JSONL format - one JSON object per line):

```jsonl
{"t_ms":1772419900760,"op":"append","latency_us":650,"ok":true}
{"t_ms":1772419900762,"op":"append","latency_us":720,"ok":true}
{"t_ms":1772419900764,"op":"append","latency_us":685,"ok":true}
...
```

Each sample contains:
- `t_ms`: Timestamp in milliseconds since epoch
- `op`: Operation type (`append` or `read`)
- `latency_us`: Latency in microseconds
- `ok`: Whether the operation succeeded

# Metrics Captured

Benchmark runs capture:

* **Throughput**: Events per second
* **Latency percentiles**: p50, p95, p99, p999
* **Container metrics**: CPU, memory, startup time
* **Raw samples**: Per-operation timing data
* **Environment**: Hardware, OS, disk, runtime info
* **Reproducibility**: Git commit hash, seed, exact config


# Environmental Controls

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

Benchmarks must be run on isolated machines.


# Publishing Results

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
