#!/bin/bash
source .test-state

echo "🧹 Cleaning up..."
echo "Terminating Instance 2 ($INST2_ID)..."
aws ec2 terminate-instances --instance-ids $INST2_ID > /dev/null
aws ec2 wait instance-terminated --instance-ids $INST2_ID

echo "Deleting Volume ($VOL_ID)..."
aws ec2 delete-volume --volume-id $VOL_ID > /dev/null

echo "Deleting Security Group ($SG_ID)..."
aws ec2 delete-security-group --group-id $SG_ID > /dev/null

rm .test-state
echo "✅ Cleanup complete!"