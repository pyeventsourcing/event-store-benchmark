#!/bin/bash
set -e

aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
            "Name=tag:Project,Values=event-store-benchmark-suite" \
  --query "Reservations[*].Instances[*].InstanceId" \
  --output text