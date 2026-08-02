#!/usr/bin/env bash
# Create a custom AMI from the builder EC2 that has /opt/emr-extra-jars/ installed.
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
BUILDER_INSTANCE_ID="${BUILDER_INSTANCE_ID:?Set BUILDER_INSTANCE_ID=i-xxxxxxxx}"
AMI_NAME="${AMI_NAME:-emr-extra-jars-$(date +%Y%m%d-%H%M%S)}"
AMI_DESC="${AMI_DESC:-EMR custom AMI with pre-baked JARs under /opt/emr-extra-jars}"
NO_REBOOT="${NO_REBOOT:-0}"

echo "Creating AMI from $BUILDER_INSTANCE_ID (region=$AWS_REGION) ..."
echo "  name: $AMI_NAME"

ARGS=(
  --region "$AWS_REGION"
  --instance-id "$BUILDER_INSTANCE_ID"
  --name "$AMI_NAME"
  --description "$AMI_DESC"
)

if [[ "$NO_REBOOT" == "1" ]]; then
  ARGS+=(--no-reboot)
  echo "  (no-reboot — prefer stopping the instance first for a consistent filesystem)"
fi

AMI_ID="$(aws ec2 create-image "${ARGS[@]}" --query 'ImageId' --output text)"
echo "AMI_ID=$AMI_ID"

echo "Waiting until available (can take several minutes) ..."
aws ec2 wait image-available --region "$AWS_REGION" --image-ids "$AMI_ID"

aws ec2 describe-images \
  --region "$AWS_REGION" \
  --image-ids "$AMI_ID" \
  --query 'Images[0].[ImageId,Name,State,Architecture]' \
  --output text

echo ""
echo "Export: export CUSTOM_AMI_ID=$AMI_ID"
echo "You can terminate the builder instance once the AMI is available."
