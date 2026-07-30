#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$DIR/.."
S3_BUCKET="s3://esb-benchmark-results" # Update to match your bucket name

# Use argument if passed, otherwise fall back to .last_session_id
SESSION_ID="${1}"
if [ -z "$SESSION_ID" ]; then
    if [ -f "$REPO_ROOT/.last_session_id" ]; then
        SESSION_ID=$(cat "$REPO_ROOT/.last_session_id")
    else
        echo "Error: No SESSION_ID provided and no .last_session_id file found."
        echo "Usage: ./aws/fetch-report.sh [SESSION_ID]"
        exit 1
    fi
fi

echo "Syncing benchmark results for session: $SESSION_ID..."

# 1. Create results directory if it doesn't exist
mkdir -p "$REPO_ROOT/results"

# 2. Sync all store result subdirectories from S3 into local results/
aws s3 sync "$S3_BUCKET/$SESSION_ID/" "$REPO_ROOT/results/"

echo "Results downloaded to ./results/"

# 3. Generate the report locally
make report

echo "Report generated successfully!"