PYTHON ?= python3
ESB_CONTAINER_DATA_DIR ?= ./container-data
ESB_SEED ?= 42
ESB_RUST_TARGET ?= release

# Source tree of the tephra project (sibling checkout, not published to a registry). The
# tephra Docker image and the tephra adapter's path dependency both resolve from here. The
# directory is still named `dcbdb` on disk; the crates inside are `tephra*`.
TEPHRA_SRC ?= $(HOME)/dev/tqwewe/dcbdb

ifeq ($(ESB_RUST_TARGET),release)
	CARGO_RELEASE_FLAG := --release
else
	CARGO_RELEASE_FLAG :=
endif

ifneq ($(strip $(ESB_FEATURES)),)
	CARGO_FEATURE_FLAG := --features $(ESB_FEATURES)
else
	CARGO_FEATURE_FLAG :=
endif


.PHONY: check
.PHONY: build
.PHONY: venv
.PHONY: report
.PHONY: run-smoke-test
.PHONY: run-scaling-streams-in-docker
.PHONY: run-scaling-streams
.PHONY: run-scaling-dcb
.PHONY: run-writeflood
.PHONY: run-scaling-streams-in-postgres
.PHONY: run-scaling-readers
.PHONY: run-scaling-writers
.PHONY: run-write-condititional
.PHONY: help
.PHONY: FORCE

# Default target
help:
	@echo "Available targets:"
	@echo "  check                 - Check all the code"
	@echo "  build                 - Build the es-bench executable"
	@echo "  venv                  - Create a Python virtual environment and install dependencies"
	@echo "  report                - Run the Python report generator"
	@echo "  run-smoke-test        - Run the 'smoke-test' workload"
	@echo "  run-scaling-readers   - Run the 'scaling-readers' workload"
	@echo "  run-scaling-writers   - Run the 'scaling-writers' workload"
	@echo "  run-kurrentdb-bench-python - Run the Python KurrentDB benchmark"
	@echo "  run-kurrentdb-bench-rust   - Run the Rust KurrentDB benchmark"
	@echo "  configs/%.yaml        - Run a workload defined by the specified configuration file"

check:
	cargo check --workspace --all-targets --all-features

# Build the es-bench binary
build:
	cargo build -p es-bench $(CARGO_RELEASE_FLAG) $(CARGO_FEATURE_FLAG)

# Build the local tephra:local Docker image the tephra adapter/testcontainer expects. The build
# context is the tephra source tree; the Dockerfile lives in this repo. The source commit is
# baked into the image (TEPHRA_GIT_SHA) so each run's manifest can pin the exact build.
.PHONY: build-tephra-image
build-tephra-image:
	DOCKER_BUILDKIT=1 docker build -f docker/tephra.Dockerfile \
		--build-arg TEPHRA_GIT_SHA="$$(git -C $(TEPHRA_SRC) rev-parse --short HEAD 2>/dev/null || echo unknown)" \
		-t tephra:local $(TEPHRA_SRC)

# Create Python virtual environment and install dependencies
venv:
	$(PYTHON) -m venv ./.venv
	./.venv/bin/pip install -r ./python/requirements.txt

# Generate report from raw results
report:
	PYTHONPATH=./python ./.venv/bin/python -m report_generator.main --raw results --out results

# Run the smoke-test workload
run-smoke-test:
	@make ./configs/smoke-test.yaml

# Run the scaling-readers workload
run-scaling-readers:
	@make ./configs/scaling/readers.yaml

# Run the scaling-writers workload
run-scaling-writers:
	@make ./configs/scaling/writers.yaml

# Run the scaling-streams-in-docker workload
run-scaling-streams-in-docker:
	@make ./configs/scaling-streams-in-docker.yaml

# Run the scaling-streams workload
run-scaling-streams:
	@make ./configs/scaling-streams.yaml

# Run the scaling-dcb workload
run-scaling-dcb:
	@make ./configs/scaling-dcb.yaml

# Run the writeflood workload
run-writeflood:
	@make ./configs/writeflood.yaml

# Run the scaling-streams-in-postgres workload
run-scaling-streams-in-postgres:
	@make ./configs/scaling-streams-in-postgres.yaml

# Run the write-unconditional workload
run-write-unconditional:
	@make ./configs/write-unconditional.yaml

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

.PHONY: start-axon-server-dcb
start-axon-server-dcb:
	docker pull axoniq/axonserver:2026.0.5-jdk-21-nonroot
	docker run -d --rm \
	  --name my-axon-server-dcb \
	  -p 8024:8024 \
	  -p 8124:8124 \
	  -e AXONIQ_AXONSERVER_NAME=my-axon-dcb-server \
	  -e AXONIQ_AXONSERVER_HOSTNAME=my-axon-dcb-server \
	  -e AXONIQ_AXONSERVER_STANDALONE_DCB="true" \
	  axoniq/axonserver:2026.0.5-jdk-21-nonroot
	sleep 15
#	  -e AXONIQ_AXONSERVER_EVENT_FORCE_INTERVAL="0" \
#	  axoniq/axonserver:latest-jdk-21-nonroot
#	@printf "Waiting for Axon Server to initialize DCB"
# 	@until curl -sf -X POST "http://127.0.0.1:8024/v2/cluster/init?dcb=true" \
#	      | grep -q "Accepted init cluster request"; do \
#		printf "."; \
#		sleep 1; \
#	done
#	@echo " done."

.PHONY: stop-axon-server-dcb
stop-axon-server-dcb:
	docker stop my-axon-server-dcb