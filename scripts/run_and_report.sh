#!/usr/bin/env bash
set -euo pipefail

# TODO: Move this into the workload_type
#WRITERS=(1 2 4 8 16)
#READERS=(1 2 4 8 16)
WRITERS=(1 2)
READERS=(1 2)
#STORES=("umadb" "kurrentdb" "axonserver" "eventsourcingdb")
#STORES=("umadb" "kurrentdb")
STORES=("all")
RAW_DIR="results/raw"
PUBLISHED_DIR="results/published"

cargo build --release -p es-bench

# Clean previous results
rm -rf "$RAW_DIR" "$PUBLISHED_DIR"
mkdir -p "$RAW_DIR"

# Run environment check
echo "=== Running environment check (Docker) ==="
if [ -d "env-check" ]; then
  # Use make to build and run the container, directing output to the raw results dir
  (cd env-check && make run OUTPUT="../$RAW_DIR/env_check.json") || echo "Warning: Environment check failed (requires Docker and make)"
else
  echo "Warning: env-check/ directory not found."
fi

# Run concurrent_writers workload_type with different writer counts
WORKLOAD_NAME="concurrent_writers"
WORKLOAD_CONFIG_FILE_TEMPLATE="workloads/concurrent_writers.yaml"
for w in "${WRITERS[@]}"; do
  # Generate workload_type config file
  WORKLOAD_CONFIG_FILE="workloads/${WORKLOAD_NAME}_w${w}.yaml"
  sed "s/^writers:.*/writers: ${w}/" "$WORKLOAD_CONFIG_FILE_TEMPLATE" \
    | sed "s/^name:.*/name: ${WORKLOAD_NAME}_w${w}/" \
    > "$WORKLOAD_CONFIG_FILE"

  for store in "${STORES[@]}"; do
    echo "=== Running $store with workload $WORKLOAD_NAME and $w writers ==="
    ./target/release/es-bench run \
      --store "$store" \
      --workload "$WORKLOAD_NAME" \
      --config "$WORKLOAD_CONFIG_FILE" \
      --output "$RAW_DIR"
  done

  # Clean up generated workload_type file
  rm -f "$WORKLOAD_CONFIG_FILE"
done

# Run concurrent_readers workload_type with different reader counts
WORKLOAD_NAME="concurrent_readers"
WORKLOAD_CONFIG_FILE_TEMPLATE="workloads/concurrent_readers.yaml"
for r in "${READERS[@]}"; do
  # Generate workload_type config file
  WORKLOAD_CONFIG_FILE="workloads/${WORKLOAD_NAME}_r${r}.yaml"
  sed "s/^readers:.*/readers: ${r}/" "$WORKLOAD_CONFIG_FILE_TEMPLATE" \
    | sed "s/^name:.*/name: ${WORKLOAD_NAME}_r${r}/" \
    > "$WORKLOAD_CONFIG_FILE"

  for store in "${STORES[@]}"; do
    echo "=== Running $store with workload $WORKLOAD_NAME and $r readers ==="
    ./target/release/es-bench run \
      --store "$store" \
      --workload "$WORKLOAD_NAME" \
      --config "$WORKLOAD_CONFIG_FILE" \
      --output "$RAW_DIR"
  done

  # Clean up generated workload_type file
  rm -f "$WORKLOAD_CONFIG_FILE"
done

# Generate consolidated report
python python/report_generator.py --raw "$RAW_DIR" --out "$PUBLISHED_DIR"
echo "Done! Open $PUBLISHED_DIR/index.html"
