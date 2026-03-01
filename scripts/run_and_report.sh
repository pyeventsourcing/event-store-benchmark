#!/usr/bin/env bash
set -euo pipefail

# TODO: Move this into the workload
#WRITERS=(1 2 4 8 16)
#READERS=(1 2 4 8 16)
WRITERS=(1 2)
READERS=(1 2)
STORES=("umadb" "kurrentdb" "axonserver" "eventsourcingdb")
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

# TODO: Converge workload/workflow.
# Run concurrent_writers workflow with different writer counts
WORKFLOW="concurrent_writers"
WORKLOAD_TEMPLATE="workloads/concurrent_writers.yaml"
for w in "${WRITERS[@]}"; do
  # Generate a per-count workload YAML on the fly
  WORKLOAD_FILE="workloads/${WORKFLOW}_w${w}.yaml"
  sed "s/^writers:.*/writers: ${w}/" "$WORKLOAD_TEMPLATE" \
    | sed "s/^name:.*/name: ${WORKFLOW}_w${w}/" \
    > "$WORKLOAD_FILE"

  for store in "${STORES[@]}"; do
    echo "=== Running $store with workflow $WORKFLOW and $w writers ==="
    ./target/release/es-bench run \
      --store "$store" \
      --workflow "$WORKFLOW" \
      --workload "$WORKLOAD_FILE" \
      --output "$RAW_DIR"
  done

  # Clean up generated workload file
  rm -f "$WORKLOAD_FILE"
done

# Run concurrent_readers workflow with different reader counts
WORKFLOW="concurrent_readers"
WORKLOAD_TEMPLATE="workloads/concurrent_readers.yaml"
for r in "${READERS[@]}"; do
  # Generate a per-count workload YAML on the fly
  WORKLOAD_FILE="workloads/${WORKFLOW}_r${r}.yaml"
  sed "s/^readers:.*/readers: ${r}/" "$WORKLOAD_TEMPLATE" \
    | sed "s/^name:.*/name: ${WORKFLOW}_r${r}/" \
    > "$WORKLOAD_FILE"

  for store in "${STORES[@]}"; do
    echo "=== Running $store with workflow $WORKFLOW and $r readers ==="
    ./target/release/es-bench run \
      --store "$store" \
      --workflow "$WORKFLOW" \
      --workload "$WORKLOAD_FILE" \
      --output "$RAW_DIR"
  done

  # Clean up generated workload file
  rm -f "$WORKLOAD_FILE"
done

# Generate consolidated report
python python/report_generator.py --raw "$RAW_DIR" --out "$PUBLISHED_DIR"
echo "Done! Open $PUBLISHED_DIR/index.html"
