#!/bin/bash
set -e

ROLE_NAME="BenchmarkRunnerRole"

aws iam get-role-policy \
  --role-name $ROLE_NAME \
  --policy-name BenchmarkRunnerAccess