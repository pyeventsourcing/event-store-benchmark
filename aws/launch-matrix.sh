#!/bin/bash
set -e

# Disable the AWS CLI pager so the script runs non-interactively
export AWS_PAGER=""

# Default settings (can be overridden via CLI flags)
INSTANCE_TYPE="${INSTANCE_TYPE:-c6id.2xlarge}"
STORES=("umadb" "axonserver" "postgres-dcb-ttcte")
IAM_PROFILE="${IAM_PROFILE:-BenchmarkRunnerRole}"
EBS_IOPS=""
EBS_THROUGHPUT=""
ESB_MAX_CONCURRENT_WORKERS="1024"
OS_CHOICE="al" # Default OS

# Parse CLI arguments
# Example usage:
#   ./launch.sh --instance c7g.2xlarge --os ubuntu
#   ./launch.sh --instance c7gd.2xlarge --stores umadb --os al
while [[ $# -gt 0 ]]; do
  case $1 in
    -i|--instance) INSTANCE_TYPE="$2"; shift 2 ;;
    -s|--stores) IFS=',' read -r -a STORES <<< "$2"; shift 2 ;;
    -o|--os) OS_CHOICE=$(echo "$2" | tr '[:upper:]' '[:lower:]'); shift 2 ;; # Converts to lowercase automatically
    --iops) EBS_IOPS="$2"; shift 2 ;;
    --throughput) EBS_THROUGHPUT="$2"; shift 2 ;;
    --iam-profile) IAM_PROFILE="$2"; shift 2 ;;
    --max-concurrent-workers) ESB_MAX_CONCURRENT_WORKERS="$2"; shift 2 ;;
    *) echo "Unknown option $1"; exit 1 ;;
  esac
done

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$DIR/.."
REPO_URL=$(git config --get remote.origin.url)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
SESSION_ID=$(date +'%Y-%m-%dT%H-%M-%S')

# Get Git commit hash for binary caching
GIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short HEAD)

echo "$SESSION_ID" > "$REPO_ROOT/.last_session_id"

# Determine architecture based on instance family (e.g., c7g/c7gd = aarch64, c6i/c6id = x86_64)
if [[ "$INSTANCE_TYPE" =~ ^[a-z][0-9]g ]] || [[ "$INSTANCE_TYPE" =~ ^a1 ]]; then
    ARCH="aarch64"
    UBUNTU_ARCH="arm64"
else
    ARCH="x86_64"
    UBUNTU_ARCH="amd64"
fi

# Determine AMI Parameter and Root Device mapping based on OS
if [ "$OS_CHOICE" == "ubuntu" ]; then
    AMI_PARAM="/aws/service/canonical/ubuntu/server/24.04/stable/current/${UBUNTU_ARCH}/hvm/ebs-gp3/ami-id"
    ROOT_DEVICE="/dev/sda1"
else
    OS_CHOICE="al" # Fallback safety
    if [ "$ARCH" == "aarch64" ]; then
        AMI_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
    else
        AMI_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
    fi
    ROOT_DEVICE="/dev/xvda"
fi

echo "Resolving AMI ID for $OS_CHOICE ($ARCH)..."
AMI_ID=$(aws ssm get-parameter --name "$AMI_PARAM" --query "Parameter.Value" --output text)

# Catch I-series (Storage Optimized) OR any instance with a 'd' in its suffix
if [[ "$INSTANCE_TYPE" =~ ^i[a-z0-9]*\. ]] || [[ "$INSTANCE_TYPE" =~ ^[a-z0-9]+d.*\. ]]; then
    echo "Storage Mode: Local NVMe Instance Storage (Direct-attached SSD detected)"
    BLOCK_MAPPINGS="[
      {\"DeviceName\":\"$ROOT_DEVICE\",\"Ebs\":{\"VolumeSize\":20,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}},
      {\"DeviceName\":\"/dev/sdb\",\"VirtualName\":\"ephemeral0\"}
    ]"
else
    echo "Storage Mode: EBS gp3 Network Storage (Dedicated Data Volume)"

    # Build dynamic EBS JSON block for secondary data disk (/dev/sdb)
    EBS_CONFIG='"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true'
    if [ -n "$EBS_IOPS" ]; then
        EBS_CONFIG+=", \"Iops\": $EBS_IOPS"
        echo " -> Custom IOPS: $EBS_IOPS"
    else
        echo " -> IOPS: 3000 (AWS baseline default)"
    fi

    if [ -n "$EBS_THROUGHPUT" ]; then
        EBS_CONFIG+=", \"Throughput\": $EBS_THROUGHPUT"
        echo " -> Custom Throughput: $EBS_THROUGHPUT MB/s"
    else
        echo " -> Throughput: 125 MB/s (AWS baseline default)"
    fi

    # Root volume (20 GB OS) + Secondary volume (60 GB Data)
    BLOCK_MAPPINGS="[
      {\"DeviceName\":\"$ROOT_DEVICE\",\"Ebs\":{\"VolumeSize\":20,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}},
      {\"DeviceName\":\"/dev/sdb\",\"Ebs\":{$EBS_CONFIG}}
    ]"
fi

echo "================================================="
echo " Launching Session: $SESSION_ID"
echo " OS Selection:     $OS_CHOICE"
echo " Stores:           ${STORES[*]}"
echo " Target Repo:      $REPO_URL (Branch: $BRANCH)"
echo " Git Commit Hash:  $GIT_HASH"
echo " Instance Type:    $INSTANCE_TYPE ($ARCH)"
echo " Block mappings:   ${BLOCK_MAPPINGS}"
echo " AMI ID:           $AMI_ID"
echo " IAM Profile:      $IAM_PROFILE"
echo "================================================="

# Pre-build and upload feature-flagged binaries for each store
echo "Checking/Building S3 binary artifacts for target stores..."
for STORE in "${STORES[@]}"; do
    "$DIR/build-and-upload.sh" "$ARCH" "$STORE"
done

TEMPLATE_FILE="$DIR/userdata.template.sh"

for STORE in "${STORES[@]}"; do
    echo "Provisioning instance for $STORE..."

    TMP_USERDATA="/tmp/userdata-$STORE-$SESSION_ID.sh"
    sed -e "s|{{STORE}}|$STORE|g" \
        -e "s|{{SESSION_ID}}|$SESSION_ID|g" \
        -e "s|{{REPO_URL}}|$REPO_URL|g" \
        -e "s|{{BRANCH}}|$BRANCH|g" \
        -e "s|{{ARCH}}|$ARCH|g" \
        -e "s|{{GIT_HASH}}|$GIT_HASH|g" \
        -e "s|{{ESB_MAX_CONCURRENT_WORKERS}}|$ESB_MAX_CONCURRENT_WORKERS|g" \
        "$TEMPLATE_FILE" > "$TMP_USERDATA"

    # Only add CPU credit specification for burstable T-series instances
    CREDIT_SPEC=""
    if [[ "$INSTANCE_TYPE" =~ ^t[0-9] ]]; then
        CREDIT_SPEC="--credit-specification CpuCredits=unlimited"
    fi

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --iam-instance-profile Name="$IAM_PROFILE" \
        $CREDIT_SPEC \
        --block-device-mappings "$BLOCK_MAPPINGS" \
        --user-data file://"$TMP_USERDATA" \
        --instance-initiated-shutdown-behavior terminate \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=Benchmark-$STORE-$SESSION_ID},{Key=Project,Value=event-store-benchmark-suite},{Key=Arch,Value=$ARCH},{Key=OS,Value=$OS_CHOICE}]" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  -> Launched $INSTANCE_ID ($STORE)"
    echo "$INSTANCE_ID" > "$REPO_ROOT/$STORE.aws_instance_id"
    rm "$TMP_USERDATA"
done

echo ""
echo "All instances launched! Monitor with: ./aws/tail-workload.sh <store>"