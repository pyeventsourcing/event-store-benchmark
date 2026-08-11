# syntax=docker/dockerfile:1
#
# Builds the tephra TCP server into a small runtime image tagged `tephra:local`, which the
# benchmark's testcontainer expects. The build context must be the tephra source tree, e.g.:
#
#   docker build -f docker/tephra.Dockerfile -t tephra:local ~/dev/tqwewe/dcbdb
#
# (the `build-tephra-image` Makefile target wraps this and passes TEPHRA_GIT_SHA).

# --- build stage ---
FROM rust:1-bookworm AS builder

# tephra-proto's build script uses rust-protobuf's codegen, which hard-pins protoc 35.1
# (Debian's protobuf-compiler is far older), so fetch that exact protoc from the official
# release. Map the Docker build arch to the release asset's arch string.
ARG PROTOC_VERSION=35.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(uname -m)" in \
         x86_64) PROTOC_ARCH=x86_64 ;; \
         aarch64) PROTOC_ARCH=aarch_64 ;; \
         *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/protoc.zip \
         "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip" \
    && unzip -q /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip \
    && protoc --version

WORKDIR /src
COPY . .

# Only the server binary is needed; the umadb-compare bench feature stays off, so the
# umadb git dependency is never fetched.
RUN cargo build --release -p tephra-server

# --- runtime stage ---
FROM debian:bookworm-slim AS runtime

# Provenance: the tephra source commit this image was built from, passed by the Makefile
# target and surfaced in each run's manifest so results are pinned to an exact build.
ARG TEPHRA_GIT_SHA=unknown
ENV TEPHRA_GIT_SHA=${TEPHRA_GIT_SHA}
LABEL tephra.git_sha=${TEPHRA_GIT_SHA}

# Run as a non-root user; /data holds the event store and is the testcontainer mount point.
RUN useradd --system --uid 10001 --create-home tephra \
    && mkdir -p /data \
    && chown tephra:tephra /data

COPY --from=builder /src/target/release/tephra-server /usr/local/bin/tephra-server

USER tephra
WORKDIR /data
EXPOSE 9000

ENV RUST_LOG=info
# Bind on all interfaces so the published container port is reachable from the host.
# The CLI is argh-based (launch essentials only): --bind/--data-dir, with all tuning in a
# TOML config or TEPHRA__* env vars. RUST_LOG above still drives the tracing filter.
ENTRYPOINT ["tephra-server", "--bind", "0.0.0.0:9000", "--data-dir", "/data"]
