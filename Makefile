PYTHON ?= python3
ESB_CONTAINER_DATA_DIR ?= ./container-data
ESB_SEED ?= 42

.PHONY: build
.PHONY: venv
.PHONY: report
.PHONY: install-umadb
.PHONY: start-umadb
.PHONY: run-umadb-local
.PHONY: run-kurrentdb-local
.PHONY: run-smoke-test
.PHONY: run-scaling
.PHONY: run-scaling-readers
.PHONY: run-scaling-writers
.PHONY: help
.PHONY: run-kurrentdb-bench-python
.PHONY: run-kurrentdb-bench-rust
.PHONY: FORCE

# Default target
help:
	@echo "Available targets:"
	@echo "  build                 - Build the es-bench executable"
	@echo "  venv                  - Create a Python virtual environment and install dependencies"
	@echo "  report                - Run the Python report generator"
	@echo "  run-smoke-test        - Run the 'smoke-test' workload"
	@echo "  run-scaling-readers   - Run the 'scaling-readers' workload"
	@echo "  run-scaling-writers   - Run the 'scaling-writers' workload"
	@echo "  run-kurrentdb-bench-python - Run the Python KurrentDB benchmark"
	@echo "  run-kurrentdb-bench-rust   - Run the Rust KurrentDB benchmark"
	@echo "  configs/%.yaml        - Run a workload defined by the specified configuration file"

# Build the es-bench binary
build:
	cargo build --release

# Create Python virtual environment and install dependencies
venv:
	$(PYTHON) -m venv ./.venv
	./.venv/bin/pip install -r ./python/requirements.txt

# Generate report from raw results
report:
	PYTHONPATH=./python ./.venv/bin/python -m report_generator.main --raw results/raw --out results/published

# Install UmaDB
install-umadb:
	@cargo install umadb

# Start UmaDB
start-umadb:
	umadb &

# Run the umadb-local workload
run-umadb-local:
	@make ./configs/umadb-local.yaml

# Run the kurrentdb-local workload
run-kurrentdb-local:
	@make ./configs/kurrentdb-local.yaml

# Run the axonserver-local workload
run-axonserver-local:
	@make ./configs/axonserver-local.yaml

# Run the smoke-test workload
run-smoke-test:
	@make ./configs/smoke-test.yaml

# Run the scaling-readers workload
run-scaling-readers:
	@make ./configs/scaling/readers.yaml

# Run the scaling-writers workload
run-scaling-writers:
	@make ./configs/scaling/writers.yaml

# Run the scaling-writers workload
run-scaling:
	@make ./configs/scaling.yaml

# Run the Python KurrentDB benchmark
run-kurrentdb-bench-python:
	@./.venv/bin/python python/kurrentdb_benchmark.py

# Run the Rust KurrentDB benchmark
run-kurrentdb-bench-rust:
	@cargo run --release --package kurrentdb-benchmark

# Run a specific benchmark configuration
configs/%.yaml: FORCE
	./target/release/es-bench run --config $@ --seed $(ESB_SEED) --data-dir=$(ESB_CONTAINER_DATA_DIR)

FORCE:

#KURRENTDB_DOCKER_IMAGE ?= docker.cloudsmith.io/eventstore/eventstore-ce/eventstoredb-oss:23.10.7-bookworm-slim
#KURRENTDB_DOCKER_IMAGE ?= docker.cloudsmith.io/eventstore/eventstore/eventstoredb-ee:24.10.6-x64-8.0-bookworm-slim
#KURRENTDB_DOCKER_IMAGE ?= docker.kurrent.io/kurrent-latest/kurrentdb:25.0.1-x64-8.0-bookworm-slim
KURRENTDB_DOCKER_IMAGE ?= docker.kurrent.io/kurrent-latest/kurrentdb:25.1.0-x64-8.0-bookworm-slim

.PHONY: start-kurrentdb-insecure
start-kurrentdb-insecure:
	@docker run -d -i -t -p 2113:2113 \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --env "KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --env "KURRENTDB_ALLOW_UNKNOWN_OPTIONS=true" \
    --env "KURRENTDB_TELEMETRY_OPTOUT=true" \
    --env "KURRENTDB_MEM_DB=true" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "EVENTSTORE_RUN_PROJECTIONS=All" \
    --env "EVENTSTORE_START_STANDARD_PROJECTIONS=true" \
    --env "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --name my-kurrentdb-insecure \
    $(KURRENTDB_DOCKER_IMAGE) \
    --insecure

.PHONY: stop-kurrentdb-insecure
stop-kurrentdb-insecure:
	@docker stop my-kurrentdb-insecure
	@docker rm my-kurrentdb-insecure

.PHONY: python-kurrentdb-benchmark
python-kurrentdb-benchmark:
	./.venv/bin/python ./python/kurrentdb_benchmark.py
