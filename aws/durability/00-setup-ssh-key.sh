#!/bin/bash
set -e

KEY_NAME="esb-durability-aws-ssh-key"
KEY_FILE="${KEY_NAME}.pem"

echo "🔑 Setting up SSH key pair: $KEY_NAME"

# Check if the key already exists in AWS and delete it to ensure a fresh start
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" > /dev/null 2>&1; then
    echo "⚠️ Key '$KEY_NAME' already exists in AWS. Deleting it..."
    aws ec2 delete-key-pair --key-name "$KEY_NAME"
fi

# Remove the local file if it exists
if [ -f "$KEY_FILE" ]; then
    echo "🗑️ Removing old local key file '$KEY_FILE'..."
    rm "$KEY_FILE"
fi

echo "✨ Creating new key pair in AWS..."
aws ec2 create-key-pair \
    --key-name "$KEY_NAME" \
    --query 'KeyMaterial' \
    --output text > "$KEY_FILE"

echo "🔒 Securing local key file (chmod 400)..."
chmod 400 "$KEY_FILE"

echo "✅ Key setup complete! Your key is saved as '$KEY_FILE'."
echo "You can now run: ./01-launch-first-node.sh umadb"