#!/bin/bash
set -e

rm -v .test-state || true

# --- Configuration ---
AZ="us-east-1a"
INSTANCE_TYPE="c7i.2xlarge"  # 🔥 Upgraded to 8 vCPUs / 16GB RAM
KEY_NAME="esb-durability-aws-ssh-key"
KEY_FILE="${KEY_NAME}.pem"

# Save state
echo "AZ=$AZ" > .test-state
echo "INSTANCE_TYPE=$INSTANCE_TYPE" >> .test-state
echo "KEY_NAME=$KEY_NAME" >> .test-state
echo "KEY_FILE=$KEY_FILE" >> .test-state


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
      echo "Usage: $0 --store <axonserver|umadb|tephra>"
      exit 1
      ;;
  esac
done

if [[ -z "$STORE" ]]; then
  echo "❌ Error: The --store argument is mandatory."
  echo "Usage: $0 --store <axonserver|umadb|tephra>"
  exit 1
fi

if [[ "$STORE" != "axonserver" && "$STORE" != "umadb" && "$STORE" != "tephra" ]]; then
  echo "❌ Error: Invalid store '$STORE'."
  echo "Must be either 'axonserver' or 'umadb' or 'tephra'."
  exit 1
fi

echo "STORE=$STORE" >> .test-state

cleanup_on_failure() {
    echo -e "\n⚠️ Script interrupted or failed! Cleaning up partial resources..."
    if [ -n "$VOL_ID" ]; then
        echo "Deleting volume $VOL_ID..."
        aws ec2 delete-volume --volume-id "$VOL_ID" 2>/dev/null || true
    fi
    if [ -n "$INST_ID" ]; then
        echo "💥 Terminating instance $INST_ID..."
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

echo "AMI_ID=$AMI_ID" >> .test-state

echo "🚀 Starting Setup for $STORE on a $INSTANCE_TYPE..."

# 1. Create Security Group
SG_ID=$(aws ec2 create-security-group --group-name "db-test-sg" --description "DB Test" --query 'GroupId' --output text 2>/dev/null || aws ec2 describe-security-groups --group-names "db-test-sg" --query 'SecurityGroups[0].GroupId' --output text)

echo "SG_ID=$SG_ID" >> .test-state

aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 8124 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 50051 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 9000 --cidr 0.0.0.0/0 2>/dev/null || true

# 2. Create EBS Volume (10GB gp3 defaults to 3000 IOPS and 125MB/s - perfect bottleneck)
echo "📦 Creating EBS Volume in $AZ..."
VOL_ID=$(aws ec2 create-volume --availability-zone $AZ --size 50 --volume-type gp3 --query 'VolumeId' --output text)

echo "VOL_ID=$VOL_ID" >> .test-state

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

echo "INST1_ID=$INST_ID" >> .test-state

echo "⏳ Waiting for Instance 1 to be running..."
aws ec2 wait instance-running --instance-ids $INST_ID
IP=$(aws ec2 describe-instances --instance-ids $INST_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo "INST1_IP=$IP" >> .test-state

# 4. Attach Volume
echo "🔗 Attaching Volume $VOL_ID to $INST_ID..."
aws ec2 attach-volume --volume-id $VOL_ID --instance-id $INST_ID --device /dev/sdf > /dev/null
aws ec2 wait volume-in-use --volume-ids $VOL_ID


# 5. Setup Server via SSH
echo "⏳ Waiting for SSH to become available on $IP..."
while ! ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=2 ubuntu@$IP 'echo OK' > /dev/null 2>&1; do sleep 2; done

echo "🛠️ Formatting volume, installing Docker, and starting server..."
ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
    sudo mkfs.ext4 /dev/nvme1n1
    sudo mkdir -p /mnt/data
    sudo mount /dev/nvme1n1 /mnt/data

#    # --- THE LINUX KERNEL MEMORY TRAP (...which didn't seem to cause any problems) ---
#    # Don't start background flushing until 80% of RAM is dirty
#    sudo sysctl -w vm.dirty_background_ratio=80
#    # Block processes only if 90% of RAM is dirty
#    sudo sysctl -w vm.dirty_ratio=90
#    # Wait 10 minutes (60000 centisecs) before considering data "old" enough to flush
#    sudo sysctl -w vm.dirty_expire_centisecs=60000
#    # Wake up the background flush daemon only every 10 minutes
#    sudo sysctl -w vm.dirty_writeback_centisecs=60000
#    # ------------------------------------

    sudo apt-get update -y && sudo apt-get install -y docker.io
EOF

if [ "$STORE" == "axonserver" ]; then
    echo "🐳 Starting Axon Server container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        #  For Axon Server nonroot Docker image
        sudo mkdir -p /mnt/data/axonserver-events /mnt/data/axonserver-data /mnt/data/axonserver-log
        sudo chmod -R 777 /mnt/data

        sudo docker pull axoniq/axonserver:2026.0.5-jdk-21-nonroot
        sudo docker run -d --rm \
          --name my-axon-server-dcb \
          -p 8024:8024 \
          -p 8124:8124 \
          -v /mnt/data/axonserver-events:/axonserver/events \
          -v /mnt/data/axonserver-data:/axonserver/data \
          -v /mnt/data/axonserver-log:/axonserver/log \
          -e AXONIQ_AXONSERVER_NAME=my-axon-dcb-server \
          -e AXONIQ_AXONSERVER_HOSTNAME=my-axon-dcb-server \
          -e AXONIQ_AXONSERVER_STANDALONE_DCB="true" \
          axoniq/axonserver:2026.0.5-jdk-21-nonroot

        echo "⏳ Waiting 30 seconds for Axon Server to boot..."
        sleep 30
EOF
elif [ "$STORE" == "umadb" ]; then
    echo "🐳 Starting UmaDB container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        sudo docker run -d --rm -p 50051:50051 -v /mnt/data/umadb:/data umadb/umadb:latest
        echo "⏳ Waiting 3 seconds for UmaDB to boot..."
        sleep 3
EOF
elif [ "$STORE" == "tephra" ]; then
    echo "🐳 Starting Tephra container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        sudo mkdir -p /mnt/data/tephra-data
        sudo chmod -R 777 /mnt/data

        sudo docker run -d --rm -p 9000:9000 -v /mnt/data/tephra-data:/data ghcr.io/tqwewe/tephra:latest
        echo "⏳ Waiting 3 seconds for Tephra to boot..."
        sleep 3
EOF
else
    echo "Error: Store $STORE not supported!"
    exit 1
fi

if [ "$STORE" == "axonserver" ]; then
    echo "⏱️ Getting max timestamp for Axon Server (should be none)..."
    AXON_SERVER_URI=http://$IP:8124 ./target/release/es-bench read-max-timestamp axonserver
    echo -e "\n✅ INSTANCE 1 READY! Run your write workload:"
    echo "AXON_SERVER_URI=http://$IP:8124 ESB_WORKLOAD_STORES=axonserver make run-write-unconditional"
elif [ "$STORE" == "umadb" ]; then
    echo "⏱️ Getting max timestamp for UmaDB (should be none)..."
    UMADB_URI=http://$IP:50051 ./target/release/es-bench read-max-timestamp umadb
    echo -e "\n✅ INSTANCE 1 READY! Run your write workload:"
    echo "UMADB_URI=http://$IP:50051 ESB_WORKLOAD_STORES=umadb make run-write-unconditional"
elif [ "$STORE" == "tephra" ]; then
    echo "⏱️ Getting max timestamp for Tephra (should be none)..."
    TEPHRA_URI=$IP:9000 ./target/release/es-bench read-max-timestamp tephra
    echo -e "\n✅ INSTANCE 1 READY! Run your write workload:"
    echo "TEPHRA_URI=$IP:9000 ESB_WORKLOAD_STORES=tephra make run-write-unconditional"
else
    echo "Error: Store $STORE not supported!"
    exit 1
fi