#!/bin/bash
set -e

# Default settings (can be overridden via CLI flags)
INSTANCE_TYPE="${INSTANCE_TYPE:-c6id.2xlarge}"
STORES=("umadb" "axonserver" "postgres-dcb-ttcte")
IAM_PROFILE="BenchmarkRunnerRole"

# Parse CLI arguments (e.g., ./launch.sh --instance c7g.2xlarge --stores umadb)
while [[ $# -gt 0 ]]; do
  case $1 in
    -i|--instance) INSTANCE_TYPE="$2"; shift 2 ;;
    -s|--stores) IFS=',' read -r -a STORES <<< "$2"; shift 2 ;;
    *) echo "Unknown option $1"; exit 1 ;;
  esac
done

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$DIR/.."
REPO_URL=$(git config --get remote.origin.url)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
SESSION_ID=$(date +'%Y-%m-%dT%H-%M-%S')

echo "$SESSION_ID" > "$REPO_ROOT/.last_session_id"

# Determine architecture based on instance family (e.g., c7g/m7g/a1 = aarch64, c6i/m6a = x86_64)
if [[ "$INSTANCE_TYPE" =~ ^[a-z][0-9]g ]] || [[ "$INSTANCE_TYPE" =~ ^a1 ]]; then
    ARCH="aarch64"
    AMI_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
else
    ARCH="x86_64"
    AMI_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
fi

AMI_ID=$(aws ssm get-parameter --name "$AMI_PARAM" --query "Parameter.Value" --output text)

# Configure block devices (If 'd' is in instance type name, map ephemeral NVMe)
if [[ "$INSTANCE_TYPE" =~ d\. ]]; then
    BLOCK_MAPPINGS='[
      {"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}},
      {"DeviceName":"/dev/sdb","VirtualName":"ephemeral0"}
    ]'
else
    # EBS-only instances (e.g. c6i.2xlarge) - enlarge root volume for database storage
    BLOCK_MAPPINGS='[
      {
        "DeviceName": "/dev/xvda",
        "Ebs": {
          "VolumeSize": 60,
          "VolumeType": "gp3",
          "Iops": 10000,
          "Throughput": 500
        }
      }
    ]'
fi

echo "================================================="
echo " Launching Session: $SESSION_ID"
echo " Instance Type:    $INSTANCE_TYPE ($ARCH)"
echo " AMI ID:           $AMI_ID"
echo " Stores:           ${STORES[*]}"
echo "================================================="

TEMPLATE_FILE="$DIR/userdata.template.sh"

for STORE in "${STORES[@]}"; do
    echo "Provisioning instance for $STORE..."

    TMP_USERDATA="/tmp/userdata-$STORE-$SESSION_ID.sh"
    sed -e "s|{{STORE}}|$STORE|g" \
        -e "s|{{SESSION_ID}}|$SESSION_ID|g" \
        -e "s|{{REPO_URL}}|$REPO_URL|g" \
        -e "s|{{BRANCH}}|$BRANCH|g" \
        -e "s|{{ARCH}}|$ARCH|g" \
        "$TEMPLATE_FILE" > "$TMP_USERDATA"

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --iam-instance-profile Name="$IAM_PROFILE" \
        --block-device-mappings "$BLOCK_MAPPINGS" \
        --user-data file://"$TMP_USERDATA" \
        --instance-initiated-shutdown-behavior terminate \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=Benchmark-$STORE-$SESSION_ID},{Key=Project,Value=event-store-benchmark-suite},{Key=Arch,Value=$ARCH}]" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  -> Launched $INSTANCE_ID ($STORE)"
    echo "$INSTANCE_ID" > "$REPO_ROOT/$STORE.aws_instance_id"
    rm "$TMP_USERDATA"
done

echo ""
echo "All instances launched! Monitor with: ./aws/tail-workload.sh <store>"