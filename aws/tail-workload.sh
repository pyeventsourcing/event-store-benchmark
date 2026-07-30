#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./aws/tail-workload.sh "
    echo "Example: ./aws/tail-workload.sh umadb"
    exit 1
fi

STORE="$1"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ID_FILE="$DIR/../$STORE.aws_instance_id"

if [ ! -f "$ID_FILE" ]; then
    echo "Error: Instance ID file not found at $ID_FILE"
    echo "Has the workload been launched?"
    exit 1
fi

INSTANCE_ID=$(cat "$ID_FILE")
echo "Connecting to $STORE (Instance: $INSTANCE_ID)..."
echo "Press Ctrl+C to exit (this will not stop the workload)."
echo "--------------------------------------------------------"

# Use AWS-StartInteractiveCommand to run bash and tail the log
# We must pass parameters as a properly escaped JSON string to avoid CLI parsing errors
aws ssm start-session \
    --target "$INSTANCE_ID" \
    --document-name AWS-StartInteractiveCommand \
    --parameters '{"command":["bash -l -c \"tail -n +1 -f /var/log/benchmark.log\""]}'