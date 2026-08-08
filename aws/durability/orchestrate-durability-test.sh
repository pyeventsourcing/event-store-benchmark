#!/bin/bash
set -e

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
  exit 1
fi

if [[ "$STORE" != "axonserver" && "$STORE" != "umadb" ]]; then
  echo "❌ Error: Invalid store '$STORE'."
  exit 1
fi

# 🚨 SAFETY TRAP: If you press Ctrl+C, forcefully kill the workload and run cleanup!
cleanup() {
  if [ -n "$WORKLOAD_PID" ]; then
      # We only kill on an unexpected exit (Ctrl+C). Normal execution bypasses this.
      kill -9 $WORKLOAD_PID 2>/dev/null || true
  fi
  ./aws/durability/05-clean-up.sh
}
trap cleanup EXIT INT TERM ERR


echo "=================================================="
echo "🚀 STARTING DURABILITY TEST FOR: $STORE"
echo "=================================================="

# 1. Launch Instance 1 & Server
./aws/durability/01-launch-first-node.sh --store "$STORE"

# 2. Run Workload in Background & Redirect Logs
echo "⚡ Starting workload in background (logging to workload.log)..."
./aws/durability/02-run-workload.sh > workload.log 2>&1 &
WORKLOAD_PID=$!
echo "WORKLOAD_PID=$WORKLOAD_PID" >> .test-state

# Let the workload hammer the server for 60 seconds
# - this needs to match the workload duration config
echo "⏳ Hammering server for 60 seconds..."
sleep 60

# 3. Crash Instance 1 & Recover on Instance 2
# Note: The client will now start throwing timeouts in the background, which is exactly what we want!
echo "💥 Terminating node to test durability..."
./aws/durability/03-crash-and-recover.sh

# 4. Wait for the workload to finish naturally
# The recovery step above takes ~1-2 minutes, so es-bench might already be done.
# This just ensures we don't proceed until es-bench has printed its final summary.
echo "⏳ Waiting for client workload to gracefully conclude its 120s run..."
wait $WORKLOAD_PID || true
# Clear the PID so the trap doesn't try to kill it again
WORKLOAD_PID=""

# 5. Fetch Max Timestamp from Surviving Disk
echo "--------------------------------------------------"
echo "📊 Querying surviving event store on Instance 2..."
./aws/durability/04-get-max-timestamp.sh

# 6. Print Client ACKed Max Timestamp for Comparison
echo ""
echo "📋 Client Workload Summary (from workload.log):"
# Grabbing the last 25 lines so you can see the final stats even if there are a few timeout logs at the end
tail -n 25 workload.log | grep "Total count:" || true
tail -n 25 workload.log | grep "Max acknowledged timestamp:" || true
echo "=================================================="
echo "✅ Test complete! (Cleanup will run automatically)"