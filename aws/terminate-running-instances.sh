#!/bin/bash
set -e

# Fetch running instance IDs as a space-separated string
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
            "Name=tag:Project,Values=event-store-benchmark-suite" \
  --query "Reservations[*].Instances[*].InstanceId" \
  --output text)

# Check if any instances were found
if [ -z "$INSTANCE_IDS" ] || [ "$INSTANCE_IDS" == "None" ]; then
  echo "No running instances found."
  exit 0
fi

echo "💥 Terminating instances: $INSTANCE_IDS"
aws ec2 terminate-instances --instance-ids $INSTANCE_IDS