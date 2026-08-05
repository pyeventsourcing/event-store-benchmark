#!/bin/bash
exec > >(tee /var/log/benchmark.log|logger -t user-data -s 2>/dev/console) 2>&1
set -e

# Explicitly set HOME for cloud-init background execution
export HOME="/root"

STORE="{{STORE}}"
SESSION_ID="{{SESSION_ID}}"
REPO_URL="{{REPO_URL}}"
BRANCH="{{BRANCH}}"
S3_BUCKET="s3://esb-benchmark-results"

# --- MOUNT LOCAL NVMe SSD TO /opt ---
echo "Searching for local NVMe SSD..."

# Locate the disk with the exact AWS "Instance Storage" model name
EPHEMERAL_DEV=$(lsblk -dno NAME,MODEL | grep -i "Instance Storage" | awk '{print "/dev/"$1}' | head -n 1)

if [ -n "$EPHEMERAL_DEV" ]; then
  echo "Found local NVMe SSD at $EPHEMERAL_DEV. Formatting with ext4..."
  mkfs.ext4 -F "$EPHEMERAL_DEV"
  mkdir -p /opt
  mount -o noatime "$EPHEMERAL_DEV" /opt
  echo "Local NVMe mounted successfully to /opt:"
  df -h /opt
else
  echo "Warning: No local NVMe Instance Storage found, falling back to root volume."
fi
# -----------------------------------

# ALWAYS SHUTDOWN ON EXIT (Even if an error triggers set -e)
cleanup() {
  EXIT_CODE=$?
  echo "Script exiting with code $EXIT_CODE. Syncing logs and shutting down..."

  # Copy the log file to S3 for debugging
  aws s3 cp /var/log/benchmark.log "$S3_BUCKET/$SESSION_ID/esb-$SESSION_ID/$STORE-benchmark.log" || true

  # Sync results folder if it exists
  if [ -d "results" ]; then
    aws s3 sync results/ "$S3_BUCKET/$SESSION_ID/" || true
  fi

  shutdown -h now
}
trap cleanup EXIT


echo "Starting benchmark for $STORE in session $SESSION_ID"

# 1. System Setup

# Target file descriptor count for high performance / concurrency
DESIRED_FD=65535

# Check current hard limit
HARD_FD=$(ulimit -Hn)

# Automatically cap at the OS hard limit if it's lower than desired
if [ "$HARD_FD" != "unlimited" ] && [ "$HARD_FD" -lt "$DESIRED_FD" ]; then
    echo "[WARN] Operating system hard limit ($HARD_FD) is lower than recommended ($DESIRED_FD)."
    echo "[WARN] Setting file descriptor limit to OS ceiling ($HARD_FD)."
    echo "[WARN] To increase this further, update /etc/security/limits.conf or systemd service settings."
    SET_FD=$HARD_FD
else
    SET_FD=$DESIRED_FD
fi

# Apply the new limit to this shell and any child processes
ulimit -n "$SET_FD"

echo "[INFO] Starting benchmark with soft file descriptor limit set to $(ulimit -n)..."

# AL2023 System Package Installation
dnf update -y
dnf groupinstall -y "Development Tools"
dnf install -y protobuf-compiler git protobuf-devel unzip

# Verify AWS CLI installation (Pre-installed on AL2023)
aws --version

# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Source Rust from explicit root path
source /root/.cargo/env
export PATH="/root/.cargo/bin:$PATH"

# Verify Rust & protoc versions
cargo --version
protoc --version

# 2. Clone repository and build with correct features
git clone -b $BRANCH $REPO_URL /opt/benchmark
cd /opt/benchmark
export ESB_FEATURES=$STORE
make build

# 3. Store-specific Setup & PID capturing
case $STORE in
  postgres-dcb-ttcte)
    # Install PostgreSQL 15 on AL2023
    dnf install -y postgresql15-server postgresql15

    NEW_DIR="/opt/postgresql/data"
    echo "=== Initializing PostgreSQL on NVMe ($NEW_DIR) ==="

    mkdir -p "$NEW_DIR"
    chown -R postgres:postgres /opt/postgresql

    # 1. Initialize data directory directly on NVMe using initdb
    sudo -u postgres initdb -D "$NEW_DIR"

    # 2. Tell systemd where the custom data directory is located
    mkdir -p /etc/systemd/system/postgresql.service.d/
    cat <<EOF > /etc/systemd/system/postgresql.service.d/override.conf
[Service]
Environment=PGDATA=$NEW_DIR
EOF

    # 3. Reload systemd units and start PostgreSQL
    systemctl daemon-reload
    systemctl enable postgresql
    systemctl start postgresql

    # Create PostgreSQL user, database, and assign ownership
    sudo -u postgres psql -c "CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';" || true
    sudo -u postgres psql -c "CREATE DATABASE eventsourcing OWNER eventsourcing;" || true
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE eventsourcing TO eventsourcing;" || true

    # Capture PostgreSQL PID
    pgrep -o -x postgres > /opt/benchmark/postgres-dcb-ttcte.pid || true

    echo "=== PostgreSQL Status ==="
    systemctl status postgresql --no-pager || true

    ./target/release/es-bench create-postgres-dcb-ttcte-tables
    ;;

  axonserver)
    dnf install -y java-21-amazon-corretto-devel
    curl -L https://download.axoniq.io/axonserver/AxonServer-2026.0.5.zip -o axonserver.zip
    unzip -q axonserver.zip
    cd AxonServer-2026.0.5
    AXONIQ_AXONSERVER_STANDALONE_DCB=true nohup java -jar axonserver.jar > /opt/benchmark/axonserver.log 2>&1 &
    echo $! > /opt/benchmark/axonserver.pid
    cd /opt/benchmark

    echo "Waiting 30 seconds for AxonServer to initialize..."
    sleep 30

    echo "=== AxonServer Startup Log ==="
    cat /opt/benchmark/axonserver.log || true
    echo "=============================="
    ;;

  umadb)
    # Download native AL2023 binary built with zigbuild (glibc 2.34 linked)
    curl -sSL https://github.com/umadb-io/umadb/releases/download/v0.7.3/umadb-x86_64-unknown-linux-gnu-v3.tar.gz -o umadb.tar.gz
    tar -xzf umadb.tar.gz && chmod +x umadb && mv umadb /usr/local/bin/
    UMADB_READ_METHOD=fileio UMADB_PAGE_CACHE_MAX_MB=6000 nohup umadb > /opt/benchmark/umadb.log 2>&1 &
    echo $! > /opt/benchmark/umadb.pid

    echo "Waiting 5 seconds for UmaDB to initialize..."
    sleep 5

    echo "=== UmaDB Startup Log ==="
    cat /opt/benchmark/umadb.log || true
    echo "========================="
    ;;
esac


# 4. Run Workload
export ESB_SESSION_ID=$SESSION_ID
export ESB_WORKLOAD_STORES=$STORE
make run-scaling-dcb