PYTHON ?= python3
ESB_CONTAINER_DATA_DIR ?= ./container-data
ESB_SEED ?= 42
ESB_RUST_TARGET ?= release

ifeq ($(ESB_RUST_TARGET),release)
	CARGO_RELEASE_FLAG := --release
else
	CARGO_RELEASE_FLAG :=
endif

.PHONY: build
.PHONY: venv
.PHONY: report
.PHONY: run-smoke-test
.PHONY: run-scaling-in-docker
.PHONY: run-scaling-binaries
.PHONY: run-scaling-postgres
.PHONY: run-scaling-readers
.PHONY: run-scaling-writers
.PHONY: help
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
	cargo build $(CARGO_RELEASE_FLAG)

# Create Python virtual environment and install dependencies
venv:
	$(PYTHON) -m venv ./.venv
	./.venv/bin/pip install -r ./python/requirements.txt

# Generate report from raw results
report:
	PYTHONPATH=./python ./.venv/bin/python -m report_generator.main --raw results/raw --out results/published

# Run the smoke-test workload
run-smoke-test:
	@make ./configs/smoke-test.yaml

# Run the scaling-readers workload
run-scaling-readers:
	@make ./configs/scaling/readers.yaml

# Run the scaling-writers workload
run-scaling-writers:
	@make ./configs/scaling/writers.yaml

# Run the scaling-in-docker workload
run-scaling-in-docker:
	@make ./configs/scaling-in-docker.yaml

# Run the scaling-binaries workload
run-scaling-binaries:
	@make ./configs/scaling-binaries.yaml

# Run the scaling-postgres workload
run-scaling-postgres:
	@make ./configs/scaling-postgres.yaml

# Run a specific benchmark configuration
configs/%.yaml: FORCE
	./target/$(ESB_RUST_TARGET)/es-bench run --config $@ --seed $(ESB_SEED) --data-dir=$(ESB_CONTAINER_DATA_DIR)

FORCE:


# Stuff created when debugging the 43ms KurrentDB Rust client latency

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
    --env "KURRENTDB_TELEMETRY_OPTOUT=true" \
    --env "KURRENTDB_MEM_DB=true" \
    --name my-kurrentdb-insecure \
    $(KURRENTDB_DOCKER_IMAGE) \
    --insecure

.PHONY: stop-kurrentdb-insecure
stop-kurrentdb-insecure:
	@docker stop my-kurrentdb-insecure
	@docker rm my-kurrentdb-insecure

.PHONY: kurrentdb-benchmark-python
kurrentdb-benchmark-python:
	./.venv/bin/python ./python/kurrentdb_benchmark.py

.PHONY: kurrentdb-benchmark-rust-build
kurrentdb-benchmark-rust-build:
	@cargo build $(CARGO_RELEASE_FLAG) --package kurrentdb-benchmark

.PHONY: kurrentdb-benchmark-rust-official
kurrentdb-benchmark-rust-official:
	./target/$(ESB_RUST_TARGET)/kurrentdb-benchmark-official

.PHONY: kurrentdb-benchmark-rust-minimal
kurrentdb-benchmark-rust-minimal:
	./target/$(ESB_RUST_TARGET)/kurrentdb-benchmark-minimal

