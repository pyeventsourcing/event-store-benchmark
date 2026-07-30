#!/bin/bash
set -e

ROLE_NAME="BenchmarkRunnerRole"
S3_BUCKET="esb-benchmark-results"

echo "Setting up IAM Role: $ROLE_NAME"

# 1. Create the trust policy
cat <<EOF > /tmp/trust-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role if it doesn't exist
if ! aws iam get-role --role-name $ROLE_NAME 2>/dev/null; then
    aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document file:///tmp/trust-policy.json
else
    echo "Role $ROLE_NAME already exists."
fi

# 2. Create and attach S3 permissions
cat <<EOF > /tmp/s3-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::$S3_BUCKET/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::$S3_BUCKET"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $ROLE_NAME \
  --policy-name BenchmarkS3BucketAccess \
  --policy-document file:///tmp/s3-policy.json

# 3. Create Instance Profile and attach Role
if ! aws iam get-instance-profile --instance-profile-name $ROLE_NAME 2>/dev/null; then
    aws iam create-instance-profile --instance-profile-name $ROLE_NAME
    aws iam add-role-to-instance-profile --instance-profile-name $ROLE_NAME --role-name $ROLE_NAME
else
    echo "Instance profile $ROLE_NAME already exists."
fi

# 4. Attach SSM policy for live log tailing
aws iam attach-role-policy \
  --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

rm /tmp/trust-policy.json /tmp/s3-policy.json
echo "IAM setup complete!"