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
  aws s3 cp /var/log/benchmark.log "$S3_BUCKET/$SESSION_ID/$STORE/benchmark.log" || true

  # Sync results folder if it exists
  if [ -d "results" ]; then
    aws s3 sync results/ "$S3_BUCKET/$SESSION_ID/" || true
  fi

  shutdown -h now
}
trap cleanup EXIT


echo "Starting benchmark for $STORE in session $SESSION_ID"

# 1. System Setup
apt-get update
apt-get install -y protobuf-compiler make build-essential git curl unzip

# Install AWS CLI v2 directly
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
./aws/install
rm -rf awscliv2.zip aws/

# Verify AWS CLI installation
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
    apt-get install -y postgresql postgresql-contrib

    # 1. Stop PostgreSQL to safely move data to NVMe
    systemctl stop postgresql

    # 2. Move data directory to the local NVMe SSD on /opt
    mkdir -p /opt/postgresql
    rsync -av /var/lib/postgresql/ /opt/postgresql/
    rm -rf /var/lib/postgresql
    ln -s /opt/postgresql /var/lib/postgresql

    # 3. Restart PostgreSQL on NVMe storage
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
    apt-get install -y openjdk-21-jdk
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
    curl -sSL https://github.com/umadb-io/umadb/releases/download/v0.6.13/umadb-x86_64-unknown-linux-gnu.tar.gz -o umadb.tar.gz
    tar -xzf umadb.tar.gz && chmod +x umadb && mv umadb /usr/local/bin/
    UMADB_READ_METHOD=fileio UMADB_PAGE_CACHE_MAX_MB=2000 nohup umadb > /opt/benchmark/umadb.log 2>&1 &
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
#make ./configs/quick-scaling-dcb.yaml
