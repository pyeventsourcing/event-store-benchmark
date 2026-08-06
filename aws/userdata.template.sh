#!/bin/bash
exec > >(tee /var/log/benchmark.log|logger -t user-data -s 2>/dev/console) 2>&1
set -e

# Disable the AWS CLI pager so the script runs non-interactively
export AWS_PAGER=""

# Explicitly set HOME for cloud-init background execution
export HOME="/root"

STORE="{{STORE}}"
SESSION_ID="{{SESSION_ID}}"
REPO_URL="{{REPO_URL}}"
BRANCH="{{BRANCH}}"
ARCH="{{ARCH}}"
GIT_HASH="{{GIT_HASH}}"
ESB_MAX_CONCURRENT_WORKERS="{{ESB_MAX_CONCURRENT_WORKERS}}"
S3_BUCKET="s3://esb-benchmark-results"

STORE_LOG="/var/log/$STORE-server.log"
RESULTS_STORE_LOG="/opt/benchmark/results/esb-$SESSION_ID/$STORE-server.log"


# Get IMDSv2 Token & Instance Info
TOKEN=$(curl -s -S -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_TYPE=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
AZ=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
REGION="${AZ%?}" # Trim last char to get region (e.g. us-east-1a -> us-east-1)

echo "=========================================="
echo "Benchmark Host Metadata"
echo "Instance ID:   $INSTANCE_ID"
echo "Instance Type: $INSTANCE_TYPE"
echo "Region / AZ:   $AZ"
echo "Git Commit:    $GIT_HASH"
echo "Architecture:  $ARCH"
echo "=========================================="
echo "Local Storage Topology (lsblk):"
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL
echo "=========================================="

# Query AWS API for exact EBS Volume configuration (IOPS, Throughput, VolumeType)
echo "EBS Volume Provisioning Specs:"
aws ec2 describe-volumes \
    --region "$REGION" \
    --filters Name=attachment.instance-id,Values="$INSTANCE_ID" \
    --query 'Volumes[*].{VolumeId:VolumeId, VolumeType:VolumeType, Size:Size, Iops:Iops, Throughput:Throughput}' \
    --output table || echo "[WARN] Failed to query AWS EC2 API for EBS details (Ensure IAM role has ec2:DescribeVolumes permission)."
echo "=========================================="

# --- MOUNT DEDICATED STORAGE TO /opt ---
echo "Searching for dedicated benchmark storage..."

# 1. Look for local NVMe Instance Storage first (e.g. c7gd instances)
DATA_DEV=$(lsblk -dno NAME,MODEL | grep -i "Instance Storage" | awk '{print "/dev/"$1}' | head -n 1)

# 2. If no Instance Storage, look for secondary attached disk (e.g. /dev/sdb or /dev/nvme1n1)
if [ -z "$DATA_DEV" ]; then
  DATA_DEV=$(lsblk -dno NAME,TYPE | grep "disk" | awk '{print "/dev/"$1}' | grep -vE "nvme0n1|xvda" | head -n 1)
fi

if [ -n "$DATA_DEV" ]; then
  echo "Found dedicated data disk at $DATA_DEV. Formatting with ext4 (journaling enabled)..."
  mkfs.ext4 -F "$DATA_DEV"
  mkdir -p /opt
  mount -o noatime "$DATA_DEV" /opt
  echo "Data volume mounted successfully to /opt:"
  df -h /opt
else
  echo "Warning: No secondary data volume found, falling back to root volume."
fi
# ---------------------------------------

# ALWAYS SHUTDOWN ON EXIT (Even if an error triggers set -e)
cleanup() {
  EXIT_CODE=$?
  echo "Script exiting with code $EXIT_CODE. Syncing logs and shutting down..."

  # 1. Check for OOMs and append to the main execution log BEFORE we copy it
  if dmesg -T | grep -q -iE 'oom-killer|killed process'; then
    echo -e "\n=========================================="
    echo "🚨 KERNEL OOM KILLER DETECTED 🚨"
    dmesg -T | grep -iE 'oom-killer|killed process'
    echo "=========================================="
  fi

  # 2. Ensure target directory exists before copying
  mkdir -p /opt/benchmark/results/esb-$SESSION_ID

  # Flush pending disk writes from RAM
  sync

  # 3. Capture store-specific background server logs if present
  if [[ $STORE =~ postgres ]]; then

    echo "Dumping PostgreSQL logs from systemd journal..."
    journalctl -u postgresql --no-pager > "$RESULTS_STORE_LOG" 2>/dev/null

    echo "Appending physical PostgreSQL log files..."
    # NB: /opt/postgresql/data/log/ is probably where they actually are...
    ls -l "/opt/postgresql/data/log/" 2>/dev/null || echo "Failed to list logs in /opt/postgresql/data/log/"
    cat /var/log/postgresql/*.log /var/lib/pgsql/data/log/*.log /opt/postgresql/data/log/*.log >> "$RESULTS_STORE_LOG" 2>/dev/null || true

  else
    echo "Listing $STORE log files:"
    ls -l "$STORE_LOG" 2>/dev/null || echo "File $STORE_LOG not found."
    if [ -f "$STORE_LOG" ]; then
      cp -v "$STORE_LOG" "$RESULTS_STORE_LOG" || true
    fi
  fi

  # 4. Ensure working directory is /opt/benchmark before syncing
  cd /opt/benchmark

  # 5. Copy the benchmark log file
  cp /var/log/benchmark.log "/opt/benchmark/results/esb-$SESSION_ID/$STORE-benchmark.log" || true

  # 6. Sync results folder if it exists
  if [ -d "results" ]; then
    aws s3 sync results/ "$S3_BUCKET/$SESSION_ID/" || true
  fi

  shutdown -h now
}
trap cleanup EXIT


# Force max CPU performance
sudo cpupower frequency-set -g performance || true

# Maximize max map count and file descriptors
sudo sysctl -w vm.max_map_count=262144
sudo sysctl -w fs.file-max=2097152


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

# AL2023 System Package Installation (No compiler tools required)
dnf update -y
dnf install -y git unzip make

# Verify AWS CLI installation (Pre-installed on AL2023)
aws --version

# 2. Clone repository & fetch pre-compiled binary from S3
git clone -b $BRANCH $REPO_URL /opt/benchmark
cd /opt/benchmark
mkdir -p /opt/benchmark/target/release

BINARY_S3_PATH="$S3_BUCKET/binaries/$GIT_HASH/es-bench-$STORE-$ARCH"
SHA_S3_PATH="$S3_BUCKET/binaries/$GIT_HASH/es-bench-$STORE-$ARCH.sha256"

echo "Fetching pre-compiled binary for $STORE ($ARCH) at commit $GIT_HASH..."
aws s3 cp "$BINARY_S3_PATH" /opt/benchmark/target/release/es-bench
aws s3 cp "$SHA_S3_PATH" /opt/benchmark/target/release/es-bench.sha256

chmod +x /opt/benchmark/target/release/es-bench
ln -sf /opt/benchmark/target/release/es-bench /opt/benchmark/es-bench

# Verify binary integrity
echo "Verifying binary checksum..."
EXPECTED_SHA=$(cat /opt/benchmark/target/release/es-bench.sha256)
ACTUAL_SHA=$(sha256sum /opt/benchmark/target/release/es-bench | awk '{print $1}')

if [ "$EXPECTED_SHA" != "$ACTUAL_SHA" ]; then
    echo "[FATAL ERROR] SHA256 Mismatch!"
    echo "Expected: $EXPECTED_SHA"
    echo "Got:      $ACTUAL_SHA"
    exit 1
fi

echo "[SUCCESS] Binary checksum verified! ($ACTUAL_SHA)"


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

    # --- DYNAMIC JVM MEMORY ALLOCATION ---
    # Fetch total memory in MB
    TOTAL_MEM_MB=$(($(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024))

    # Allocate 40% of system RAM to the JVM Heap (leave rest for OS, native buffers, and es-bench)
    # Minimum floor of 512m so it doesn't crash on very small instances
    HEAP_MB=$((TOTAL_MEM_MB * 40 / 100))
    if [ "$HEAP_MB" -lt 512 ]; then
      HEAP_MB=512
    fi

    echo "Detected ${TOTAL_MEM_MB} MB RAM. Setting AxonServer JVM heap (-Xms/-Xmx) to ${HEAP_MB}m."
    # -------------------------------------

    AXONIQ_AXONSERVER_STANDALONE_DCB=true nohup java \
      -Xms${HEAP_MB}m \
      -Xmx${HEAP_MB}m \
      -XX:+UseG1GC \
      -jar axonserver.jar > $STORE_LOG 2>&1 &

    echo $! > /opt/benchmark/axonserver.pid
    cd /opt/benchmark

    echo "Waiting for AxonServer gRPC port (8124) to accept connections..."
    for i in {1..60}; do
      if timeout 1 bash -c '</dev/tcp/127.0.0.1/8124' 2>/dev/null; then
        echo "Port 8124 is open! AxonServer is ready."
        break
      fi
      echo "Port 8124 not ready yet, waiting 2s... ($i/60)"
      sleep 2
    done

    # Give an extra 3 seconds for gRPC context initialization
    sleep 3

    echo "=== AxonServer Startup Log ==="
    cat $STORE_LOG || true
    echo "=============================="
    ;;

  umadb)
    ARCH="{{ARCH}}"
    VERSION="v0.7.3"

    if [ "$ARCH" = "aarch64" ]; then
      BINARY_URL="https://github.com/umadb-io/umadb/releases/download/${VERSION}/umadb-aarch64-unknown-linux-gnu.tar.gz"
    else
      BINARY_URL="https://github.com/umadb-io/umadb/releases/download/${VERSION}/umadb-x86_64-unknown-linux-gnu-v3.tar.gz"
    fi

    echo "Downloading UmaDB binary for $ARCH from $BINARY_URL..."
    curl -sSL "$BINARY_URL" -o umadb.tar.gz
    tar -xzf umadb.tar.gz && chmod +x umadb && mv umadb /usr/local/bin/

    TOTAL_MEM_MB=$(($(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024))

    # Reserve 1000 MB for OS/es-bench, split the remainder 50/50
    # between UmaDB deserialized cache and other things
    RAW_CALC=$(( (TOTAL_MEM_MB - 1000) / 2 ))

    # Enforce a 128 MB minimum floor for low-memory testing (t4g.micro / t4g.small)
    if [ "$RAW_CALC" -lt 128 ]; then
        UMADB_PAGE_CACHE_MAX_MB=128
    else
        UMADB_PAGE_CACHE_MAX_MB=$RAW_CALC
    fi

    echo "Total RAM: ${TOTAL_MEM_MB}MB | UmaDB Cache: ${UMADB_PAGE_CACHE_MAX_MB}MB | Free for OS & Kernel Cache: $((TOTAL_MEM_MB - UMADB_PAGE_CACHE_MAX_MB))MB"
    # ---------------------------------

    UMADB_READ_METHOD=fileio UMADB_PAGE_CACHE_MAX_MB=$UMADB_PAGE_CACHE_MAX_MB nohup umadb > $STORE_LOG 2>&1 &
    echo $! > /opt/benchmark/umadb.pid

    echo "Waiting 5 seconds for UmaDB to initialize..."
    sleep 5

    echo "=== UmaDB Startup Log ==="
    cat $STORE_LOG || true
    echo "========================="
    ;;
esac


# 4. Run Workload
export ESB_SESSION_ID=$SESSION_ID
export ESB_WORKLOAD_STORES=$STORE
export ESB_MAX_CONCURRENT_WORKERS="{{ESB_MAX_CONCURRENT_WORKERS}}"
make run-scaling-dcb