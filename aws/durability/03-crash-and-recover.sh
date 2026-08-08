#!/bin/bash
set -e

# --- Configuration ---
AZ="us-east-1a"
INSTANCE_TYPE="c7i.2xlarge"
KEY_NAME="esb-durability-aws-ssh-key"
KEY_FILE="${KEY_NAME}.pem"

AMI_ID=$(aws ssm get-parameters --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query 'Parameters[0].Value' --output text)

source .test-state

echo "💥 TERMINATING INSTANCE 1 ($INST1_ID)..."
aws ec2 terminate-instances --instance-ids $INST1_ID > /dev/null

echo "Waiting for Instance 1 to be terminated ($INST1_ID)..."
aws ec2 wait instance-terminated --instance-ids $INST1_ID 2>/dev/null || true

echo "⏳ Waiting for Volume $VOL_ID to become available..."
aws ec2 wait volume-available --volume-ids $VOL_ID

if [ -n "$WORKLOAD_PID" ]; then
    echo "⏳ Waiting for client workload (PID $WORKLOAD_PID) to finish its run..."

    # 'kill -0' exits with success as long as the process is still running
    while kill -0 $WORKLOAD_PID 2>/dev/null; do
        sleep 2
    done

    echo "✅ Workload finished."
fi

echo "🖥️ Launching Instance 2..."
INST2_ID=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --count 1 \
  --instance-type $INSTANCE_TYPE \
  --key-name $KEY_NAME \
  --security-group-ids $SG_ID \
  --placement AvailabilityZone=$AZ \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=event-store-benchmark-suite}]" \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "INST2_ID=$INST2_ID" >> .test-state

echo "⏳ Waiting for Instance 2 to be running..."
aws ec2 wait instance-running --instance-ids $INST2_ID
IP=$(aws ec2 describe-instances --instance-ids $INST2_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo "INST2_IP=$IP" >> .test-state

echo "🔗 Attaching existing Volume $VOL_ID to Instance 2..."
aws ec2 attach-volume --volume-id $VOL_ID --instance-id $INST2_ID --device /dev/sdf > /dev/null
aws ec2 wait volume-in-use --volume-ids $VOL_ID

echo "⏳ Waiting for SSH to become available on $IP..."
while ! ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=2 ubuntu@$IP 'echo OK' > /dev/null 2>&1; do sleep 2; done

echo "🛠️ Mounting existing volume (NO format) and starting server..."
ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << EOF
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
    echo -e "\n✅ RECOVERY COMPLETE! Run your read verification:"
    echo "AXON_SERVER_URI=http://$IP:8124 ./target/release/es-bench read-max-timestamp axonserver"
else
    echo "🐳 Starting UmaDB container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        # NOTE: Replace 'your/umadb-image' with your actual image path/pull command
        sudo docker run -d -p 50051:50051 -v /mnt/data/umadb:/data umadb/umadb:latest
        echo "⏳ Waiting 3 seconds for UmaDB to boot..."
        sleep 3
EOF
    echo -e "\n✅ RECOVERY COMPLETE! Run your read verification:"
    echo "UMADB_URI=http://$IP:50051 ./target/release/es-bench read-max-timestamp umadb"
fi