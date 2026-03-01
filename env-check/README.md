Here you go — concise, clean, and ready to paste.

---

# Environment Check Container

This project builds a Docker container that reports structured environment metadata for benchmark runs.

It outputs a single JSON object containing:

* CPU model and core count
* Kernel version
* Total and available memory
* Filesystem type
* Disk size
* Sequential write bandwidth
* `fsync` latency (p50 / p95 / p99)

The purpose is to capture machine characteristics alongside benchmark results to improve reproducibility and comparability.

---

## Requirements

* Docker
* Make

---

## Build

```bash
make build
```

---

## Run

```bash
make run
```

This writes output to:

```
environment.json
```

---

## Run with Real Disk (Recommended for IO Benchmarks)

Docker’s default filesystem may distort disk and `fsync` results. To benchmark a real disk:

```bash
make run-mount MOUNT_DIR=/path/to/real/disk
```

Example:

```bash
make run-mount MOUNT_DIR=/mnt/nvme0
```

---

## Rebuild Without Cache

```bash
make rebuild
```

---

## Clean

```bash
make clean
```

---

## Configuration Overrides

You can override variables at runtime:

```bash
make run IMAGE_NAME=my-env IMAGE_TAG=1.0 OUTPUT=env.json
```

Available variables:

* `IMAGE_NAME`
* `IMAGE_TAG`
* `OUTPUT`
* `MOUNT_DIR` (for `run-mount`)

---

## Recommended Workflow

For reliable benchmarking:

1. Run the environment check
2. Save `environment.json`
3. Run benchmarks
4. Store both together

Publishing benchmark numbers without environment metadata weakens credibility.
