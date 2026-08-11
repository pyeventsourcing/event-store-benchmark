#!/bin/bash
set -e

source .test-state

if [ "$STORE" == "axonserver" ]; then
    echo "⏱️ Getting max timestamp for Axon Server..."

    # Run es-bench and capture its stdout & stderr without blocking execution
    ES_BENCH_OUTPUT=$(AXON_SERVER_URI=http://$INST2_IP:8124 ./target/release/es-bench read-max-timestamp axonserver 2>&1 || true)

    echo "📄 --- AXON SERVER LOGS (LAST 50 LINES) ----------"
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@$INST2_IP "sudo docker logs my-axon-server-dcb --tail 50"  || echo "Failed to get last 50 lines (INST2_IP: $INST2_IP)"
    echo "--------------------------------------------------"

    echo -e "\n📊 --- TIMESTAMPS RECOVERED FROM AXON SERVER ---"
    echo "$ES_BENCH_OUTPUT"

    echo -e "\n📊 --- TIMESTAMPS EXTRACTED FROM EVENTS FILE ---"
    python3 ./aws/durability/extract-max-timestamp-from-axon-events-file.py || true
    echo ""

elif [ "$STORE" == "umadb" ]; then
    echo -e "\n📊 --- TIMESTAMPS RECOVERED FROM UMADB ---"
    UMADB_URI=http://$INST2_IP:50051 ./target/release/es-bench read-max-timestamp umadb

elif [ "$STORE" == "tephra" ]; then
    echo -e "\n📊 --- TIMESTAMPS RECOVERED FROM TEPHRA ---"
    TEPHRA_URI=$INST2_IP:9000 ./target/release/es-bench read-max-timestamp tephra
fi
