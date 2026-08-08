#!/bin/bash
set -e

# --- Configuration ---
AZ="us-east-1a"
INSTANCE_TYPE="c7i.2xlarge"  # 🔥 Upgraded to 8 vCPUs / 16GB RAM
KEY_NAME="esb-durability-aws-ssh-key"
KEY_FILE="${KEY_NAME}.pem"

# --- Parse CLI arguments ---
STORE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--store)
      STORE=$(echo "$2" | tr '[:upper:]' '[:lower:]')
      shift 2
      ;;
    *)
      echo "❌ Unknown option: $1"
      echo "Usage: $0 --store <axonserver|umadb>"
      exit 1
      ;;
  esac
done

if [[ -z "$STORE" ]]; then
  echo "❌ Error: The --store argument is mandatory."
  echo "Usage: $0 --store <axonserver|umadb>"
  exit 1
fi

if [[ "$STORE" != "axonserver" && "$STORE" != "umadb" ]]; then
  echo "❌ Error: Invalid store '$STORE'."
  echo "Must be either 'axonserver' or 'umadb'."
  exit 1
fi

cleanup_on_failure() {
    echo -e "\n⚠️ Script interrupted or failed! Cleaning up partial resources..."
    if [ -n "$VOL_ID" ]; then
        echo "Deleting volume $VOL_ID..."
        aws ec2 delete-volume --volume-id "$VOL_ID" 2>/dev/null || true
    fi
    if [ -n "$INST_ID" ]; then
        echo "Terminating instance $INST_ID..."
        aws ec2 terminate-instances --instance-ids "$INST_ID" 2>/dev/null || true
    fi
}

# Trigger cleanup_on_failure on INT (Ctrl+C), TERM, or ERR
trap cleanup_on_failure INT TERM ERR


# Fetch the latest Ubuntu 24.04 AMI
AMI_ID=$(aws ssm get-parameters \
  --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameters[0].Value' \
  --output text)

echo "🚀 Starting Setup for $STORE on a $INSTANCE_TYPE..."

# 1. Create Security Group
SG_ID=$(aws ec2 create-security-group --group-name "db-test-sg" --description "DB Test" --query 'GroupId' --output text 2>/dev/null || aws ec2 describe-security-groups --group-names "db-test-sg" --query 'SecurityGroups[0].GroupId' --output text)
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 8124 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 50051 --cidr 0.0.0.0/0 2>/dev/null || true

# 2. Create EBS Volume (10GB gp3 defaults to 3000 IOPS and 125MB/s - perfect bottleneck)
echo "📦 Creating EBS Volume in $AZ..."
VOL_ID=$(aws ec2 create-volume --availability-zone $AZ --size 10 --volume-type gp3 --query 'VolumeId' --output text)

# 3. Launch Instance
echo "🖥️ Launching Instance 1..."
INST_ID=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --count 1 \
  --instance-type $INSTANCE_TYPE \
  --key-name $KEY_NAME \
  --security-group-ids $SG_ID \
  --placement AvailabilityZone=$AZ \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=event-store-benchmark-suite}]" \
  --query 'Instances[0].InstanceId' \
  --output text)


echo "⏳ Waiting for Instance 1 to be running..."
aws ec2 wait instance-running --instance-ids $INST_ID
IP=$(aws ec2 describe-instances --instance-ids $INST_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

# 4. Attach Volume
echo "🔗 Attaching Volume $VOL_ID to $INST_ID..."
aws ec2 attach-volume --volume-id $VOL_ID --instance-id $INST_ID --device /dev/sdf > /dev/null
aws ec2 wait volume-in-use --volume-ids $VOL_ID

# Save state
echo "VOL_ID=$VOL_ID" > .test-state
echo "INST1_ID=$INST_ID" >> .test-state
echo "SG_ID=$SG_ID" >> .test-state
echo "INST1_IP=$IP" >> .test-state
echo "STORE=$STORE" >> .test-state

# 5. Setup Server via SSH
echo "⏳ Waiting for SSH to become available on $IP..."
while ! ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=2 ubuntu@$IP 'echo OK' > /dev/null 2>&1; do sleep 2; done

echo "🛠️ Formatting volume, installing Docker, and starting server..."
ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
    sudo mkfs.ext4 /dev/nvme1n1
    sudo mkdir -p /mnt/data
    sudo mount /dev/nvme1n1 /mnt/data
    sudo apt-get update -y && sudo apt-get install -y docker.io
EOF

if [ "$STORE" == "axonserver" ]; then
    echo "🐳 Starting Axon Server container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        sudo docker pull axoniq/axonserver:2026.0.5-jdk-21-nonroot
        sudo docker run -d --rm \
          --name my-axon-server-dcb \
          -p 8024:8024 \
          -p 8124:8124 \
          -v /mnt/data/eventdata:/eventdata \
          -e AXONIQ_AXONSERVER_NAME=my-axon-dcb-server \
          -e AXONIQ_AXONSERVER_HOSTNAME=my-axon-dcb-server \
          -e AXONIQ_AXONSERVER_STANDALONE_DCB="true" \
          axoniq/axonserver:2026.0.5-jdk-21-nonroot

        echo "⏳ Waiting 30 seconds for Axon Server to boot..."
        sleep 30
EOF
    echo -e "\n✅ INSTANCE 1 READY! Run your write workload:"
    echo "AXON_SERVER_URI=http://$IP:8124 ESB_WORKLOAD_STORES=axonserver make run-write-unconditional"
else
    echo "🐳 Starting UmaDB container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        # NOTE: Replace 'your/umadb-image' with your actual image path/pull command
        sudo docker run -d -p 50051:50051 -v /mnt/data/umadb:/data umadb/umadb:latest
        echo "⏳ Waiting 3 seconds for UmaDB to boot..."
        sleep 3
EOF
    echo -e "\n✅ INSTANCE 1 READY! Run your write workload:"
    echo "UMADB_URI=http://$IP:50051 ESB_WORKLOAD_STORES=umadb make run-write-unconditional"
fi