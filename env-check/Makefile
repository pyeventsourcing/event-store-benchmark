# --------------------------------------------------
# Config
# --------------------------------------------------

IMAGE_NAME ?= env-check
IMAGE_TAG  ?= latest
IMAGE      := $(IMAGE_NAME):$(IMAGE_TAG)

OUTPUT     ?= environment.json
MOUNT_DIR  ?=

# --------------------------------------------------
# Targets
# --------------------------------------------------

.PHONY: build run run-mount clean rebuild

build:
	docker build -t $(IMAGE) .

rebuild:
	docker build --no-cache -t $(IMAGE) .

run: build
	docker run --rm $(IMAGE) > $(OUTPUT)
	@echo "Wrote $(OUTPUT)"

run-mount: build
ifndef MOUNT_DIR
	$(error MOUNT_DIR is not set. Usage: make run-mount MOUNT_DIR=/real/ssd)
endif
	docker run --rm -v $(MOUNT_DIR):/envcheck $(IMAGE) > $(OUTPUT)
	@echo "Wrote $(OUTPUT) using mounted disk $(MOUNT_DIR)"

clean:
	docker rmi $(IMAGE) || true
	rm -f $(OUTPUT)