#!/bin/bash
set -e

# Find script directory and root repo directory safely
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$DIR/.."

REPO_URL=$(git config --get remote.origin.url)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
SESSION_ID=$(date +'%Y-%m-%dT%H-%M-%S')

# Safely write to root of the repository
echo "$SESSION_ID" > "$REPO_ROOT/.last_session_id"

STORES=("umadb" "axonserver" "postgres-dcb-ttcte")
#STORES=("postgres-dcb-ttcte")

#INSTANCE_TYPE="c6i.2xlarge"
INSTANCE_TYPE="c6id.2xlarge"  # NVMe 8x vCPU
IAM_PROFILE="BenchmarkRunnerRole"

# Fetch the latest official Amazon Linux 2023 AMI for us-east-1 (x86_64)
AMI_ID=$(aws ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query "Parameter.Value" \
  --output text)

echo "Using Ubuntu 24.04 AMI: $AMI_ID"

TEMPLATE_FILE="$DIR/userdata.template.sh"

echo "Launching Session: $SESSION_ID"
echo "Target Repository: $REPO_URL (Branch: $BRANCH)"

for STORE in "${STORES[@]}"; do
    echo "Provisioning instance for $STORE..."

    TMP_USERDATA="/tmp/userdata-$STORE-$SESSION_ID.sh"
    sed -e "s|{{STORE}}|$STORE|g" \
        -e "s|{{SESSION_ID}}|$SESSION_ID|g" \
        -e "s|{{REPO_URL}}|$REPO_URL|g" \
        -e "s|{{BRANCH}}|$BRANCH|g" \
        "$TEMPLATE_FILE" > "$TMP_USERDATA"

#        Used this for c6i.2xlarge:
#        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' \

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id $AMI_ID \
        --instance-type $INSTANCE_TYPE \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[
          {"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}},
          {"DeviceName":"/dev/sdb","VirtualName":"ephemeral0"}
        ]' \
        --user-data file://"$TMP_USERDATA" \
        --instance-initiated-shutdown-behavior terminate \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=Benchmark-$STORE-$SESSION_ID},{Key=Project,Value=event-store-benchmark-suite}]" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  -> Launched $INSTANCE_ID"
    # Save the ID for the tail script
    echo "$INSTANCE_ID" > "$DIR/../$STORE.aws_instance_id"
    rm "$TMP_USERDATA"

    echo "To monitor live, run: ./aws/tail-workload.sh $STORE"
done

echo ""
echo "All instances launched!"
