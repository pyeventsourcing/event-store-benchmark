[![Logo](images/banner-v2-1280x640.png)](https://)


# Event Store Benchmark Suite

A rigorous, reproducible, open benchmark framework for evaluating event sourcing databases and append-only log systems.

This project exists to define a **credible performance standard** for event stores — one that measures real-world behavior under realistic workloads, not synthetic best-case scenarios.

The benchmark is built with:

* **Rust** — high-precision workload execution and measurement
* **Python** — analysis, visualization, and reporting

# Getting Started

1. Install Rust (stable toolchain)
2. Install Python 3.11+
3. Clone the repository
4. Run sample workloads
5. Generate example reports
6. Validate results on your hardware


# CLI Commands

The benchmark CLI is `es-bench`. You can run it from `./target/release/es-bench` after building:

```bash
cargo build --release
./target/release/es-bench <command> [options]
```

Or via Cargo:

```bash
cargo run --release -p es-bench -- <command> [options]
```

Global options:
- `--log <level>`: set log verbosity (`trace`, `debug`, `info`, `warn`, `error`). Default: `info`.
- `-h, --help`: show help
- `-V, --version`: show version

## help

You can print help for subcommands, such as `run`:

```bash
es-bench help run
```

## list-workloads
Print available workloads.

Usage:
```bash
es-bench list-workloads
```

## list-stores
Print adapted event stores.

Usage:
```bash
es-bench list-stores
```

## run

Execute a workload with an event store and write raw results to a folder.

Usage:
```bash
es-bench run \
  --store <STORE> \
  --config <CONFIG> \
  [--output results/raw] \
  [--seed <seed>]
```

Parameters:
- `--store`: adapter name (e.g. umadb).
- `--config <CONFIG>`: workload configuration.
- `--output <dir>`: base directory for raw results. Default: `results/raw`.
- `--seed <u64>`: deterministic RNG seed. Default: `42`.

Examples:
```bash
# Benchmark writing to UmaDB
es-bench run --store umadb \
  --config configs/concurrent_writers.yaml \
  --seed 42

# Benchmark writing to KurrentDB
es-bench run --store kurrentdb \
  --config configs/concurrent_writers.yaml \
  --seed 42
```

Tips:
- Prefer `--seed` to make runs comparable across machines.



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

# Workload Definitions

Workload types are defined in Rust and configured in YAML.

Example:

```yaml
workload: concurrent_writers
duration_seconds: 6
writers: 4
event_size_bytes: 256
streams:
  distribution: uniform
  unique_streams: 10000
```


# Workload Suite

The suite includes:

## Concurrent Writers

Multiple concurrent writers.

## Concurrent Readers

Multiple concurrent readers.

## Mixed Read/Write

Write-heavy workloads with background reads.

# Metrics Captured

Benchmark runs capture:

* Throughput (events/sec)
* Latency percentiles
* CPU utilization
* Memory usage
* Disk I/O
* Index size growth
* Write amplification (if available)
* Recovery time
* Error/conflict rates


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
