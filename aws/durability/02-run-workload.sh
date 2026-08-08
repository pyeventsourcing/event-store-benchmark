#!/bin/bash
set -e

source .test-state

if [ "$STORE" == "axonserver" ]; then
    echo "🚀 Starting Axon Server workload..."
    AXON_SERVER_URI=http://$INST1_IP:8124 ESB_WORKLOAD_STORES=axonserver make run-write-unconditional
else
    echo "🚀 Starting UmaDB workload..."
    UMADB_URI=http://$INST1_IP:50051 ESB_WORKLOAD_STORES=umadb make run-write-unconditional
fi