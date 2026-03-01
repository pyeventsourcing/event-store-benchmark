#!/usr/bin/env bash
set -e

TMPDIR=$(mktemp -d)
cd "$TMPDIR"

# -------------------------
# CPU INFO
# -------------------------
CPU_MODEL=$(lscpu | grep "Model name" | cut -d: -f2 | xargs)
CPU_CORES=$(nproc)
KERNEL=$(uname -r)

# -------------------------
# MEMORY INFO
# -------------------------
MEM_TOTAL_BYTES=$(grep MemTotal /proc/meminfo | awk '{print $2 * 1024}')
MEM_AVAILABLE_BYTES=$(grep MemAvailable /proc/meminfo | awk '{print $2 * 1024}')

# -------------------------
# FILESYSTEM INFO
# -------------------------
FS_TYPE=$(df -T . | tail -1 | awk '{print $2}')
DISK_SIZE_BYTES=$(df -B1 . | tail -1 | awk '{print $2}')

# -------------------------
# SEQUENTIAL WRITE TEST
# -------------------------
fio --name=write_test \
    --filename=write_test_file \
    --size=512M \
    --bs=1M \
    --rw=write \
    --direct=1 \
    --iodepth=1 \
    --numjobs=1 \
    --output-format=json > write.json

WRITE_BW_BYTES=$(jq '.jobs[0].write.bw_bytes' write.json)

# -------------------------
# SEQUENTIAL READ TEST
# -------------------------
fio --name=read_test \
    --filename=read_test_file \
    --size=512M \
    --bs=1M \
    --rw=read \
    --direct=1 \
    --iodepth=1 \
    --numjobs=1 \
    --output-format=json > read.json

READ_BW_BYTES=$(jq '.jobs[0].read.bw_bytes' read.json)

# -------------------------
# CONCURRENT READ TEST (Assess Scaling)
# -------------------------
# We use multiple jobs to see if throughput scales with concurrency.
# Note: we use the same file to simulate concurrent access to a shared resource, 
# or multiple files if preferred. fio by default creates separate files per job if filename is not fixed.
# To test scaling, we'll use 4 concurrent jobs.
fio --name=concurrent_read_test \
    --size=512M \
    --bs=1M \
    --rw=read \
    --direct=1 \
    --iodepth=1 \
    --numjobs=4 \
    --group_reporting \
    --output-format=json > concurrent_read.json

CONCURRENT_READ_BW_BYTES=$(jq '.jobs[0].read.bw_bytes' concurrent_read.json)

# -------------------------
# FSYNC LATENCY TEST
# -------------------------
fio --name=fsync_test \
    --filename=fsync_test_file \
    --size=64M \
    --bs=4k \
    --rw=write \
    --direct=1 \
    --iodepth=1 \
    --numjobs=1 \
    --fsync=1 \
    --output-format=json > fsync.json

FSYNC_P50_NS=$(jq '.jobs[0].write.clat_ns.percentile["50.000000"]' fsync.json)
FSYNC_P95_NS=$(jq '.jobs[0].write.clat_ns.percentile["95.000000"]' fsync.json)
FSYNC_P99_NS=$(jq '.jobs[0].write.clat_ns.percentile["99.000000"]' fsync.json)

# -------------------------
# OUTPUT JSON
# -------------------------
jq -n \
  --arg cpu_model "$CPU_MODEL" \
  --arg kernel "$KERNEL" \
  --arg fs_type "$FS_TYPE" \
  --argjson cpu_cores "$CPU_CORES" \
  --argjson mem_total "$MEM_TOTAL_BYTES" \
  --argjson mem_available "$MEM_AVAILABLE_BYTES" \
  --argjson disk_size "$DISK_SIZE_BYTES" \
  --argjson write_bw "$WRITE_BW_BYTES" \
  --argjson read_bw "$READ_BW_BYTES" \
  --argjson concurrent_read_bw "$CONCURRENT_READ_BW_BYTES" \
  --argjson fsync_p50 "$FSYNC_P50_NS" \
  --argjson fsync_p95 "$FSYNC_P95_NS" \
  --argjson fsync_p99 "$FSYNC_P99_NS" \
  '{
    cpu: {
      model: $cpu_model,
      cores: $cpu_cores
    },
    kernel: $kernel,
    memory: {
      total_bytes: $mem_total,
      available_bytes: $mem_available
    },
    filesystem: {
      type: $fs_type,
      disk_size_bytes: $disk_size
    },
    disk: {
      sequential_write_bw_bytes_per_sec: $write_bw,
      sequential_read_bw_bytes_per_sec: $read_bw,
      concurrent_read_bw_bytes_per_sec: $concurrent_read_bw
    },
    fsync_latency_ns: {
      p50: $fsync_p50,
      p95: $fsync_p95,
      p99: $fsync_p99
    }
  }'