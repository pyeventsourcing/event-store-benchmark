#!/bin/bash
source .test-state

echo "🧹 Cleaning up..."

if [ -n "$INST1_ID" ]; then
    echo "Terminating Instance 1 ($INST1_ID)..."
    aws ec2 terminate-instances --instance-ids $INST1_ID > /dev/null 2>&1 || true
    echo "Waiting for Instance 1 to be terminated ($INST1_ID)..."
    aws ec2 wait instance-terminated --instance-ids $INST1_ID 2>/dev/null || true
fi

if [ -n "$INST2_ID" ]; then
    echo "Terminating Instance 2 ($INST2_ID)..."
    aws ec2 terminate-instances --instance-ids $INST2_ID > /dev/null 2>&1 || true
    echo "Waiting for Instance 2 to be terminated ($INST2_ID)..."
    aws ec2 wait instance-terminated --instance-ids $INST2_ID 2>/dev/null || true
fi

if [ -n "$VOL_ID" ]; then
    echo "⏳ Waiting for Volume $VOL_ID to become available..."
    aws ec2 wait volume-available --volume-ids $VOL_ID
    echo "Deleting Volume ($VOL_ID)..."
    aws ec2 delete-volume --volume-id $VOL_ID > /dev/null 2>&1 || true
fi

if [ -n "$SG_ID" ]; then
    echo "Deleting Security Group ($SG_ID)..."
    aws ec2 delete-security-group --group-id $SG_ID > /dev/null 2>&1 || true
fi

rm -f .test-state
echo "✅ Cleanup complete!"