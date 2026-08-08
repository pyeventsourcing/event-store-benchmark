#!/bin/bash
set -e

source .test-state

if [ "$STORE" == "axonserver" ]; then
    echo "⏱️ Getting max timestamp for Axon Server..."
    AXON_SERVER_URI=http://$INST2_IP:8124 ./target/release/es-bench read-max-timestamp axonserver
else
    echo "⏱️ Getting max timestamp for UmaDB..."
    UMADB_URI=http://$INST2_IP:50051 ./target/release/es-bench read-max-timestamp umadb
fi