#!/bin/bash
exec > >(tee /var/log/benchmark.log|logger -t user-data -s 2>/dev/console) 2>&1
set -e

# Disable the AWS CLI pager so the script runs non-interactively
export AWS_PAGER=""
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
RESULTS_DIR="/opt/benchmark/results/esb-$SESSION_ID"
RESULTS_STORE_LOG="$RESULTS_DIR/$STORE-server.log"
PG_SERVICE="postgresql" # Default service name, updated dynamically if on Ubuntu

# ==========================================
# 1. REGISTER CLEANUP TRAP FIRST
# ==========================================
# Trap will fire on script completion OR any fatal exit/error
cleanup() {
  EXIT_CODE=$?
  echo "Script exiting with code $EXIT_CODE. Syncing logs and shutting down..."

  # 1. Check for OOMs in kernel ring buffer
  if dmesg -T 2>/dev/null | grep -q -iE 'oom-killer|killed process'; then
    echo -e "\n=========================================="
    echo "🚨 KERNEL OOM KILLER DETECTED 🚨"
    dmesg -T 2>/dev/null | grep -iE 'oom-killer|killed process'
    echo "=========================================="
  fi

  # 2. Ensure target results directory exists
  mkdir -p "$RESULTS_DIR"

  # Flush pending disk writes from RAM
  sync

  # 3. Capture store-specific background server logs
  if [[ "$STORE" == *postgres* ]]; then
    echo "Dumping PostgreSQL logs from systemd journal ($PG_SERVICE)..."
    journalctl -u "$PG_SERVICE" --no-pager > "$RESULTS_STORE_LOG" 2>/dev/null || true

    echo "Appending physical PostgreSQL log files if present..."
    ls -l "/opt/postgresql/data/log/" 2>/dev/null || echo "No logs in /opt/postgresql/data/log/"
    cat /var/log/postgresql/*.log /var/lib/pgsql/data/log/*.log /opt/postgresql/data/log/*.log >> "$RESULTS_STORE_LOG" 2>/dev/null || true
  else
    echo "Listing $STORE log file ($STORE_LOG):"
    ls -l "$STORE_LOG" 2>/dev/null || echo "File $STORE_LOG not found."
    if [ -f "$STORE_LOG" ]; then
      cp -v "$STORE_LOG" "$RESULTS_STORE_LOG" || true
    fi
  fi

  # 4. Copy the main user-data execution log
  cp /var/log/benchmark.log "$RESULTS_DIR/$STORE-benchmark.log" 2>/dev/null || true

  # 5. Sync results directory to S3
  if [ -d "/opt/benchmark/results" ]; then
    cd /opt/benchmark
    aws s3 sync results/ "$S3_BUCKET/$SESSION_ID/" || true
  fi

  shutdown -h now
}
trap cleanup EXIT

# ==========================================
# 2. OS DETECTION
# ==========================================
if grep -qi ubuntu /etc/os-release; then
  OS_FAMILY="ubuntu"
elif grep -qi amzn /etc/os-release; then
  OS_FAMILY="al"
else
  echo "[FATAL] Unsupported OS. This script requires Ubuntu or Amazon Linux."
  exit 1
fi

# ==========================================
# 3. HOST METADATA
# ==========================================
# Get IMDSv2 Token & Instance Info
TOKEN=$(curl -s -S -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_TYPE=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
AZ=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
REGION="${AZ%?}" # Trim last char to get region (e.g. us-east-1a -> us-east-1)

echo "=========================================="
echo "Benchmark Host Metadata"
echo "OS Family:     $OS_FAMILY"
echo "Instance ID:   $INSTANCE_ID"
echo "Instance Type: $INSTANCE_TYPE"
echo "Region / AZ:   $AZ"
echo "Git Commit:    $GIT_HASH"
echo "Architecture:  $ARCH"


# ==========================================
# 4. SYSTEM SETUP & PACKAGE INSTALLATION
# ==========================================
# Maximize max map count and file descriptors
sudo sysctl -w vm.max_map_count=262144
sudo sysctl -w fs.file-max=2097152

DESIRED_FD=65535
HARD_FD=$(ulimit -Hn)

if [ "$HARD_FD" != "unlimited" ] && [ "$HARD_FD" -lt "$DESIRED_FD" ]; then
    echo "[WARN] Operating system hard limit ($HARD_FD) is lower than recommended ($DESIRED_FD)."
    SET_FD=$HARD_FD
else
    SET_FD=$DESIRED_FD
fi

ulimit -n "$SET_FD"
echo "[INFO] Starting benchmark with soft file descriptor limit set to $(ulimit -n)..."

# Install packages dynamically based on OS family
if [ "$OS_FAMILY" = "ubuntu" ]; then
    export DEBIAN_FRONTEND=noninteractive

    # --- SPEED UP APT DOWNLOADS ---
    # 1. Disable apt periodic timer updates that block dpkg locks on boot
    systemctl stop apt-daily.service apt-daily-upgrade.service || true
    systemctl disable apt-daily.service apt-daily-upgrade.service || true

#    # 2. Swap standard mirrors to fast AWS EC2 Regional Mirrors
#    if [ "$ARCH" = "x86_64" ]; then
#        sed -i "s|http://archive.ubuntu.com|http://${REGION}.ec2.archive.ubuntu.com|g" /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
#        sed -i "s|http://security.ubuntu.com|http://${REGION}.ec2.archive.ubuntu.com|g" /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
#    fi

#    # 3. Disable apt bandwidth throttling & ipv6 timeouts
#    echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4
#    echo 'Acquire::Queue-Mode "host";' >> /etc/apt/apt.conf.d/99parallel
#    # ------------------------------

    echo "Updating Ubuntu packages..."
    apt-get update -yq
    apt-get install -yq git unzip make linux-tools-common linux-tools-generic

    echo "Installing AWS CLI v2 for Ubuntu ($ARCH)..."
    # Official AWS CLI v2 Installation (Architecture Aware)
    if [ "$ARCH" = "aarch64" ]; then
        AWS_CLI_URL="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"
    else
        AWS_CLI_URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
    fi

    curl -sSL "$AWS_CLI_URL" -o "/tmp/awscliv2.zip"
    unzip -q /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install
    rm -rf /tmp/awscliv2.zip /tmp/aws
else
    dnf update -y
    dnf install -y git unzip make kernel-tools
fi

# ==========================================
# 5. STORAGE DETECTION AND MOUNTING
# ==========================================

echo "=========================================="
echo "Local Storage Topology (lsblk):"
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL
echo "=========================================="

# Query AWS API for exact EBS Volume configuration
echo "EBS Volume Provisioning Specs:"
aws ec2 describe-volumes \
    --region "$REGION" \
    --filters Name=attachment.instance-id,Values="$INSTANCE_ID" \
    --query 'Volumes[*].{VolumeId:VolumeId, VolumeType:VolumeType, Size:Size, Iops:Iops, Throughput:Throughput}' \
    --output table || echo "[WARN] Failed to query AWS EC2 API for EBS details."
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


# Force max CPU performance if cpupower is installed
if command -v cpupower &> /dev/null; then
    sudo cpupower frequency-set -g performance || true
fi

# ==========================================
# 6. FETCH BINARY & CLONE REPO
# ==========================================
echo "Starting benchmark setup for $STORE in session $SESSION_ID"

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

# ==========================================
# 7. STORE-SPECIFIC SETUP
# ==========================================
case $STORE in
  postgres-dcb-ttcte)
    NEW_DIR="/opt/postgresql/data"
    echo "=== Initializing PostgreSQL on NVMe ($NEW_DIR) ==="
    mkdir -p "$NEW_DIR"

    if [ "$OS_FAMILY" = "ubuntu" ]; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get install -yq postgresql postgresql-contrib

        # Stop and disable standard Ubuntu multi-cluster service
        systemctl stop postgresql
        systemctl disable postgresql

        chown -R postgres:postgres /opt/postgresql

        # Dynamically discover installed Postgres version
        PG_VER=$(ls /usr/lib/postgresql/ | grep -E '^[0-9]+$' | sort -V | tail -n 1)
        sudo -u postgres /usr/lib/postgresql/$PG_VER/bin/initdb -D "$NEW_DIR"

        # Force physical log creation
        echo "logging_collector = on" >> "$NEW_DIR/postgresql.conf"
        echo "log_directory = 'log'" >> "$NEW_DIR/postgresql.conf"

        # Create raw systemd service pointing to custom NVMe path
        cat <<EOF > /etc/systemd/system/postgres-bench.service
[Unit]
Description=PostgreSQL Benchmark Server
[Service]
Type=simple
User=postgres
Environment=PGDATA=$NEW_DIR
ExecStart=/usr/lib/postgresql/$PG_VER/bin/postgres
LimitNOFILE=65535
[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        systemctl start postgres-bench
        PG_SERVICE="postgres-bench"
    else
        # Amazon Linux 2023 Setup
        dnf install -y postgresql15-server postgresql15
        chown -R postgres:postgres /opt/postgresql
        sudo -u postgres initdb -D "$NEW_DIR"

        # Force physical log creation
        echo "logging_collector = on" >> "$NEW_DIR/postgresql.conf"
        echo "log_directory = 'log'" >> "$NEW_DIR/postgresql.conf"

        mkdir -p /etc/systemd/system/postgresql.service.d/
        cat <<EOF > /etc/systemd/system/postgresql.service.d/override.conf
[Service]
Environment=PGDATA=$NEW_DIR
LimitNOFILE=65535
EOF
        systemctl daemon-reload
        systemctl enable postgresql
        systemctl start postgresql
        PG_SERVICE="postgresql"
    fi

    # Database & User setup
    sleep 3
    sudo -u postgres psql -c "CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';" || true
    sudo -u postgres psql -c "CREATE DATABASE eventsourcing OWNER eventsourcing;" || true
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE eventsourcing TO eventsourcing;" || true

    pgrep -o -x postgres > /opt/benchmark/postgres-dcb-ttcte.pid || true

    echo "=== PostgreSQL Status ==="
    systemctl status $PG_SERVICE --no-pager || true

    ./target/release/es-bench create-postgres-dcb-ttcte-tables
    ;;

  axonserver)
    if [ "$OS_FAMILY" = "ubuntu" ]; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get install -yq openjdk-21-jdk-headless
    else
        dnf install -y java-21-amazon-corretto-devel
    fi

    echo "Downloading Axon Server..."
    curl -L https://download.axoniq.io/axonserver/AxonServer-2026.0.5.zip -o axonserver.zip
    unzip -q axonserver.zip
    cd AxonServer-2026.0.5

    # Allocate 40% of system RAM to JVM Heap (floor at 512m)
    TOTAL_MEM_MB=$(($(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024))
    HEAP_MB=$((TOTAL_MEM_MB * 40 / 100))
    if [ "$HEAP_MB" -lt 512 ]; then
      HEAP_MB=512
    fi

    echo "Detected ${TOTAL_MEM_MB} MB RAM. Setting AxonServer JVM heap to ${HEAP_MB}m."

    AXONIQ_AXONSERVER_STANDALONE_DCB=true nohup java \
      -Xms${HEAP_MB}m \
      -Xmx${HEAP_MB}m \
      -XX:+UseG1GC \
      -jar axonserver.jar > "$STORE_LOG" 2>&1 &

    echo $! > /opt/benchmark/axonserver.pid
    cd /opt/benchmark

    echo "Waiting for AxonServer gRPC port (8124)..."
    for i in {1..60}; do
      echo " - attempt $i/60"
      if timeout 1 bash -c '</dev/tcp/127.0.0.1/8124' 2>/dev/null; then
        echo "Port 8124 open!"
        break
      fi
      sleep 2
    done
    sleep 3

    echo "=== AxonServer Startup Log ==="
    cat "$STORE_LOG" || true
    echo "=============================="
    ;;

  umadb)
    VERSION="v0.7.4"

    if [ "$ARCH" = "aarch64" ]; then
      BINARY_URL="https://github.com/umadb-io/umadb/releases/download/${VERSION}/umadb-aarch64-unknown-linux-gnu.tar.gz"
    else
      BINARY_URL="https://github.com/umadb-io/umadb/releases/download/${VERSION}/umadb-x86_64-unknown-linux-gnu-v3.tar.gz"
    fi

    echo "Downloading UmaDB binary for $ARCH..."
    curl -sSL "$BINARY_URL" -o umadb.tar.gz
    tar -xzf umadb.tar.gz && chmod +x umadb && mv umadb /usr/local/bin/

    # Split remaining memory 50/50 after reserving 1000MB for OS/es-bench
    TOTAL_MEM_MB=$(($(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024))
    RAW_CALC=$(( (TOTAL_MEM_MB - 1000) / 2 ))

    if [ "$RAW_CALC" -lt 128 ]; then
        UMADB_PAGE_CACHE_MAX_MB=128
    else
        UMADB_PAGE_CACHE_MAX_MB=$RAW_CALC
    fi

    echo "Total RAM: ${TOTAL_MEM_MB}MB | UmaDB Cache: ${UMADB_PAGE_CACHE_MAX_MB}MB | Free for OS Cache: $((TOTAL_MEM_MB - UMADB_PAGE_CACHE_MAX_MB))MB"

    UMADB_READ_METHOD=fileio UMADB_PAGE_CACHE_MAX_MB=$UMADB_PAGE_CACHE_MAX_MB nohup umadb > "$STORE_LOG" 2>&1 &
    echo $! > /opt/benchmark/umadb.pid

    echo "Waiting 5 seconds for UmaDB initialization..."
    sleep 5

    echo "=== UmaDB Startup Log ==="
    cat "$STORE_LOG" || true
    echo "========================="
    ;;
esac

# ==========================================
# 8. RUN WORKLOAD
# ==========================================
export ESB_SESSION_ID=$SESSION_ID
export ESB_WORKLOAD_STORES=$STORE
export ESB_MAX_CONCURRENT_WORKERS="{{ESB_MAX_CONCURRENT_WORKERS}}"
make run-scaling-dcb