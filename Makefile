.PHONY: build venv report help

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the es-bench executable"
	@echo "  venv          - Create a Python virtual environment and install dependencies"
	@echo "  report        - Run the Python report generator"

# Build the es-bench binary
build:
	cargo build --release

# Create Python virtual environment and install dependencies
venv:
	python3 -m venv ./.venv
	./.venv/bin/pip install -r ./python/requirements.txt


# Generate report from raw results
report:
	./.venv/bin/python3 python/report_generator.py --raw results/raw --out results/published
