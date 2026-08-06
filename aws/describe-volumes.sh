#!/bin/bash
set -e

aws ec2 describe-volumes \
    --query "Volumes[*].{ID:VolumeId, State:State, Size:Size, Type:VolumeType, AttachedTo:Attachments[0].InstanceId}" \
    --output table