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

    echo -e "\n📊 --- ES-BENCH READ MAX TIMESTAMP RESULT ---"
    echo "$ES_BENCH_OUTPUT"

    echo -e "\n📊 --- PYTHON TIMESTAMP EXTRACTION ----------"
    python3 ./aws/durability/extract-max-timestamp-from-axon-events-file.py || true
    echo ""

else
    echo -e "\n📊 --- ES-BENCH READ MAX TIMESTAMP RESULT ---"
    UMADB_URI=http://$INST2_IP:50051 ./target/release/es-bench read-max-timestamp umadb
fi
