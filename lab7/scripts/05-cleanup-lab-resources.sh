#!/usr/bin/env bash
# Cleanup lab resources: builder EC2s tagged for this lab + custom AMIs named emr-extra-jars*
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
DRY_RUN="${DRY_RUN:-0}"

echo "Region=$AWS_REGION  DRY_RUN=$DRY_RUN"

# --- EC2 builders ---
IDS=$(aws ec2 describe-instances --region "$AWS_REGION" \
  --filters \
    "Name=tag:Project,Values=lab7" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text)

# Also match Name=emr-ami-builder*
IDS2=$(aws ec2 describe-instances --region "$AWS_REGION" \
  --filters \
    "Name=tag:Name,Values=emr-ami-builder*" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text)

ALL_IDS=$(echo "$IDS $IDS2" | tr ' ' '\n' | sort -u | tr '\n' ' ' | xargs || true)

if [[ -n "${ALL_IDS:-}" ]]; then
  echo "Terminating instances: $ALL_IDS"
  if [[ "$DRY_RUN" != "1" ]]; then
    aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids $ALL_IDS \
      --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output table
  fi
else
  echo "No builder instances found."
fi

# --- Custom AMIs ---
AMIS=$(aws ec2 describe-images --region "$AWS_REGION" --owners self \
  --filters "Name=name,Values=emr-extra-jars*" \
  --query 'Images[].ImageId' --output text)

if [[ -z "${AMIS:-}" || "$AMIS" == "None" ]]; then
  echo "No custom AMIs matching emr-extra-jars* found."
else
  for ami in $AMIS; do
    echo "Deregistering $ami ..."
    SNAPS=$(aws ec2 describe-images --region "$AWS_REGION" --image-ids "$ami" \
      --query 'Images[0].BlockDeviceMappings[].Ebs.SnapshotId' --output text)
    if [[ "$DRY_RUN" != "1" ]]; then
      aws ec2 deregister-image --region "$AWS_REGION" --image-id "$ami"
      for snap in $SNAPS; do
        [[ -z "$snap" || "$snap" == "None" ]] && continue
        echo "  deleting snapshot $snap"
        aws ec2 delete-snapshot --region "$AWS_REGION" --snapshot-id "$snap" || true
      done
    else
      echo "  (dry-run) would deregister + delete snaps: $SNAPS"
    fi
  done
fi

echo "Cleanup done."
