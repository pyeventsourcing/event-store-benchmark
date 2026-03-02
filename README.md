[![Logo](images/banner-v2-1280x640.png)](https://)


# Event Store Benchmark Suite

A rigorous, reproducible, open benchmark framework for evaluating event sourcing databases and append-only log systems.

This project exists to define a **credible performance standard** for event stores — one that measures real-world behavior under realistic workloads, not synthetic best-case scenarios.

The benchmark is built with:

* **Rust** — high-precision workload execution and measurement
* **Python** — analysis, visualization, and reporting

---

# Running Benchmarks

Run the `run_and_report.sh` script to execute the benchmark. You may need to install Rust and Python.

```bash
# Setup Python virtual environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r python/requirements.txt

# Run the benchmark...
./scripts/run_and_report.sh
```

Then open `results/published/index.html` in your browser to view the results.

---

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

---

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

---

## 2. Percentiles Over Averages

We measure:

* p50
* p95
* p99
* p99.9

Average throughput alone is misleading.

Latency distribution under contention is what matters.

---

## 3. Durability is Not Optional

We explicitly test:

* Crash during write
* Restart and recovery
* WAL replay cost
* Index rebuild time
* Checkpoint recovery

If a store claims durability, it must survive termination mid-transaction.

---

## 4. Reproducibility

All benchmarks must be:

* Deterministic (fixed random seeds)
* Configurable via versioned YAML definitions
* Hardware documented
* OS and fsync mode documented
* Repeatable across environments

Raw results must be published alongside summarized results.

---

## 5. Store-Neutral Design

The benchmark must not favor a specific implementation.

Adapters are used to interface with different systems, but workloads are defined independently of implementation details.

---

# Repository Structure

```
event-store-benchmark/
├── README.md
├── SPEC.md
├── workloads/
│   ├── append_only.yaml
│   ├── concurrent_writers.yaml
│   ├── mixed_read_write.yaml
│   ├── tag_queries.yaml
│   ├── replay.yaml
│   └── crash_recovery.yaml
├── rust/
│   ├── bench-core/
│   ├── adapters/
│   │   ├── eventstore/
│   │   ├── postgres/
│   │   ├── sqlite/
│   │   └── kafka/
│   └── cli/
├── python/
│   ├── analysis/
│   ├── plotting/
│   ├── notebooks/
│   └── report_generator.py
├── results/
│   ├── raw/
│   ├── processed/
│   └── published/
└── docs/
    ├── methodology.md
    ├── environment.md
    └── reproducibility.md
```

---

# Architecture Overview

## Rust Layer — Benchmark Engine

Responsible for:

* Workload execution
* Concurrency control
* Precise latency measurement
* Resource usage tracking
* Crash injection
* Raw metrics output

The Rust engine produces structured output (JSON or CSV):

```json
{
  "workload": "concurrent_writers",
  "events_written": 1000000,
  "throughput_eps": 425000,
  "latency": {
    "p50": 0.8,
    "p95": 2.1,
    "p99": 5.4,
    "p999": 11.2
  },
  "cpu_percent": 78,
  "memory_mb": 512,
  "recovery_time_ms": 1420
}
```

No analysis logic lives in Rust — only measurement.

---

## Python Layer — Analysis & Visualization

Responsible for:

* Aggregating benchmark runs
* Computing statistical comparisons
* Plotting latency distributions
* Generating tables for publication
* Producing PDF/HTML reports
* Detecting regressions between runs

This separation prevents analytical overhead from contaminating benchmark execution.

---

# Workload Definitions

All workloads are defined declaratively in YAML.

Example:

```yaml
duration_seconds: 60
writers: 8
event_size_bytes: 1024
streams:
  distribution: zipf
  unique_streams: 100000
conflict_rate: 0.05
durability: fsync_on
```

Each workload defines:

* Event size
* Stream distribution
* Writer concurrency
* Conflict behavior
* Tag cardinality
* Read/write ratio
* Durability mode
* Duration or target event count

---

# Workload Suite

The suite includes:

## 1. Append Only

Single writer, sequential appends.

## 2. Parallel Writers

Multiple concurrent writers with configurable conflict injection.

## 3. Mixed Read/Write

Write-heavy workloads with background reads.

## 4. Tag/Category Queries

High- and low-cardinality tag distributions.
Intersection queries.

## 5. Replay / Catch-Up

Subscriber catching up while writes continue.

## 6. Crash & Recovery

Random process termination.
Measure recovery time and consistency.

## 7. Long-Run Stability

Sustained load (hours).
Measure drift, fragmentation, index growth.

---

# Metrics Captured

Each benchmark run captures:

* Throughput (events/sec)
* Latency percentiles
* CPU utilization
* Memory usage
* Disk I/O
* Index size growth
* Write amplification (if available)
* Recovery time
* Error/conflict rates

---

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

---

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

---

# CLI Commands

The benchmark CLI is `es-bench`. You can run it from `./target/release/es-bench` after building, or via Cargo:

```bash
cargo run -p es-bench -- <command> [options]
```

Global options:
- `--log <level>`: set log verbosity (`trace`, `debug`, `info`, `warn`, `error`). Default: `info`.
- `-h, --help`: show help
- `-V, --version`: show version

## es-bench run
Execute a workload against a specific adapter and write raw results to a timestamped folder.

Usage:
```bash
es-bench run \
  --store <adapter> \
  --workload <path/to/workload.yaml> \
  [--output results/raw] \
  [--uri <connection-uri>] \
  [--option key=value ...] \
  [--seed <u64>]
```

Parameters:
- `--store <adapter>`: adapter name. Currently supported: `umadb`.
- `--workload <file>`: path to a YAML workload definition (see `workloads/`).
- `--output <dir>`: base directory for raw results. Default: `results/raw`.
- `--uri <string>`: connection URI for the store (e.g., `http://localhost:50051`).
- `--option key=value`: repeatable key/value options for the adapter. For UmaDB:
  - `api_key=umadb:...`
  - `ca_path=/path/to/server.pem` (TLS with self-signed certs)
  - `batch_size=1000` (read hint)
- `--seed <u64>`: deterministic RNG seed. Default: `42`.

Examples:
```bash
# Basic UmaDB run (insecure gRPC)
es-bench run --store umadb \
  --workload workloads/concurrent_writers.yaml \
  --uri http://localhost:50051 \
  --seed 42

# UmaDB with TLS and API key
es-bench run --store umadb \
  --workload workloads/concurrent_writers.yaml \
  --uri https://localhost:50051 \
  --option ca_path=server.pem \
  --option api_key=umadb:example-api-key \
  --option batch_size=1000

# Custom results location
es-bench run --store umadb \
  --workload workloads/concurrent_writers.yaml \
  --uri http://localhost:50051 \
  --output results/raw/lab-a
```

Outputs:
- `summary.json`: overall stats (duration, throughput, latency percentiles, adapter/workload IDs)
- `samples.jsonl`: per-append samples (timestamp, latency, ok/error)
- `run.meta.json`: minimal locator for the Python reporting layer

## es-bench list-workloads
List workload YAML files in a directory (defaults to `workloads/`).

Usage:
```bash
es-bench list-workloads [--path <dir>]
```

Example:
```bash
es-bench list-workloads
```

## es-bench list-stores
List available adapters.

Usage:
```bash
es-bench list-stores
```

Current output:
```text
umadb
```

Tips:
- Prefer `--seed` to make runs comparable across machines.
- Use `--log debug` for more detailed progress during development.
- Start UmaDB quickly with the provided `docker-compose.yml` (`docker compose up -d umadb`).

---

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

---

# Non-Goals

This benchmark suite does not:

* Optimize systems for artificial workloads
* Hide durability settings
* Benchmark in-memory-only configurations
* Publish results without reproducibility metadata
* Declare “winners”

The goal is measurement, not marketing.

---

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

---

# Long-Term Vision

This project aims to become:

* A reference benchmark for event sourcing systems
* A research-grade measurement framework
* A regression detection tool for event store developers
* A shared standard for comparing durability and performance trade-offs

If adopted broadly, this could meaningfully improve the quality of performance claims in the event sourcing ecosystem.

---

# Getting Started

1. Install Rust (stable toolchain)
2. Install Python 3.11+
3. Clone the repository
4. Run sample workloads
5. Generate example reports
6. Validate results on your hardware

Full setup instructions are in `docs/reproducibility.md`.

---

# License

Open source under MIT.
