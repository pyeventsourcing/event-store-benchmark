#!/bin/bash
set -e

# --- Configuration ---
INTERACTIVE_RECOVERY="no"   # "yes" or anything else for "no"

source .test-state

echo "🪓 FORCE DETACHING VOLUME ($VOL_ID) FROM RUNNING INSTANCE..."
aws ec2 detach-volume --volume-id $VOL_ID --force > /dev/null

echo "⏳ Waiting for Volume $VOL_ID to become fully detached and available..."
aws ec2 wait volume-available --volume-ids $VOL_ID

echo "💥 TERMINATING INSTANCE 1 ($INST1_ID)..."
aws ec2 terminate-instances --instance-ids $INST1_ID > /dev/null

echo "⏳ Waiting for Instance 1 to be terminated ($INST1_ID)..."
aws ec2 wait instance-terminated --instance-ids $INST1_ID 2>/dev/null || true


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

    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
#        echo "Deleting corrupted Axon control data and logs"
#        sudo rm -rfv /mnt/data/axonserver-data/*
#        sudo rm -rfv /mnt/data/axonserver-log/*
#
#        echo "Deleting Axon Server RocksDB index"
#        sudo find /mnt/data/axonserver-events -type d -name "index" -exec rm -rfv {} +
#
#        echo "Deleting Rainbow Last Sequence Index and Headstores"
#        sudo find /mnt/data/axonserver-events -type f -name "rainbow.lsi" -exec rm -v {} \;
#        sudo find /mnt/data/axonserver-events -type f -name "*headstore.bin" -exec rm -v {} \;

        echo "--- FILES IN AXON SERVER LOG DIR ---"
        sudo ls -laR /mnt/data/axonserver-log

        echo "--- FILES IN AXON SERVER EVENTS DIR ---"
        sudo ls -laR /mnt/data/axonserver-events

        echo "--- FILES IN AXON SERVER DATA DIR ---"
        sudo ls -laR /mnt/data/axonserver-data
        echo "-------------------------------------"
EOF

    echo "🐳 Starting Axon Server container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
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

EOF

    if [ "$INTERACTIVE_RECOVERY" == "yes" ]; then

        echo "🔍 Dropping you into the instance to monitor Axon Server."
        echo "👉 To watch the logs, run: sudo docker logs -f my-axon-server-dcb"
        echo "👉 To inspect the events file: ls -lh /mnt/data/axonserver-events/default"
        echo "⚠️  Press Ctrl-D (or type 'exit') to close the connection and finish the script."

        # This will open an interactive shell and pause the script until you exit, and ignore non-zero exit codes
        ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o ServerAliveInterval=60 ubuntu@$IP || true

    else

      echo "⏳ Waiting 30 seconds for Axon Server to boot..."
      sleep 30

      echo "📄 --- AXON SERVER LOGS ---"
      ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@$IP "sudo docker logs my-axon-server-dcb"
      echo "--------------------------------"

      echo "🔍 Inspecting the raw bytes of the .events file..."
      ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@$IP << 'EOF'
          EVENTS_FILE="/mnt/data/axonserver-events/default/00000000000000000000.events"
          if [ -f "$EVENTS_FILE" ]; then
              echo "File size and details:"
              sudo ls -lh $EVENTS_FILE

              echo -e "\nFirst 256 bytes of the file (Hex Dump):"
              sudo hexdump -C -n 256 $EVENTS_FILE

              echo -e "\nScanning entire 256MB file for non-zero data (compressed view, first 30 lines):"
              # hexdump -C folds identical lines into a '*'.
              # If the file is just zeroes, you'll only see a few lines of output.
              sudo hexdump -C $EVENTS_FILE | head -n 30

          else
              echo "No events file found at $EVENTS_FILE"
          fi
EOF

      echo "📦 Staging the .events files into a folder for local download..."
          ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@$IP << 'EOF'
              # Create the staging folder
              mkdir -p /home/ubuntu/recovered-events

              # Find all .events files and copy them to the folder
              sudo find /mnt/data/axonserver-events/default/ -name "*.events" -exec cp {} /home/ubuntu/recovered-events/ \;

              # Fix permissions so the ubuntu user can download the folder
              sudo chown -R ubuntu:ubuntu /home/ubuntu/recovered-events

              echo "✅ Files successfully staged!"
              ls -lh /home/ubuntu/recovered-events/
EOF

    fi

    echo -e "\n📥 Downloading the recovered events files..."
    rm -r ./axon-recovered-events || true
    scp -r -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@$IP:/home/ubuntu/recovered-events ./axon-recovered-events

    echo -e "\n✅ RECOVERY COMPLETE! Run your read verification:"
    echo "AXON_SERVER_URI=http://$IP:8124 ./target/release/es-bench read-max-timestamp axonserver"

elif [ "$STORE" == "umadb" ]; then
    echo "🐳 Starting UmaDB container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        sudo docker run -d --rm -p 50051:50051 -v /mnt/data/umadb:/data umadb/umadb:latest
        echo "⏳ Waiting 3 seconds for UmaDB to boot..."
        sleep 3
EOF
    echo -e "\n✅ RECOVERY COMPLETE! Run your read verification:"
    echo "UMADB_URI=http://$IP:50051 ./target/release/es-bench read-max-timestamp umadb"

elif [ "$STORE" == "tephra" ]; then
    echo "🐳 Starting Tephra container..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$IP << 'EOF'
        sudo docker run -d --rm -p 9000:9000 -v /mnt/data/tephra-data:/data ghcr.io/tqwewe/tephra:latest
        echo "⏳ Waiting 3 seconds for Tephra to boot..."
        sleep 3
EOF
    echo -e "\n✅ RECOVERY COMPLETE! Run your read verification:"
    echo "TEPHRA_URI=$IP:9000 ./target/release/es-bench read-max-timestamp tephra"

else
    echo "Error: Store $STORE not supported!"
    exit 1
fi
