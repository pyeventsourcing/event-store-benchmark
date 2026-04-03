PYTHON ?= python3
ESB_CONTAINER_DATA_DIR ?= ./container-data
ESB_SEED ?= 42

.PHONY: build
.PHONY: venv
.PHONY: report
.PHONY: run-smoke-test
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
	./.venv/bin/python python/report_generator.py --raw results/raw --out results/published

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

# Run a specific benchmark configuration
configs/%.yaml: FORCE
	./target/release/es-bench run --config $@ --seed $(ESB_SEED) --data-dir=$(ESB_CONTAINER_DATA_DIR)

FORCE:
