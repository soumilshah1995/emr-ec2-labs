#!/bin/bash

# ---------------------------
# Configuration
# ---------------------------
CLUSTER_NAME="My_Spark_Cluster"
RELEASE_LABEL="emr-7.5.0"
REGION="us-east-1"
ACCOUNT="867098943567"
SERVICE_ROLE="arn:aws:iam::${ACCOUNT}:role/EMRServiceRole"
EC2_PROFILE="EMR_EC2_DefaultRole"
SUBNET_ID="XXX"
KEY_NAME="XX"
BUCKET_NAME="soumil-dev-bucket-1995"
LOG_URI="s3://${BUCKET_NAME}/logs/"
BOOTSTRAP_SCRIPT_S3_PATH="XXXX"

INSTANCE_FLEETS_CONFIG='[
  {
    "InstanceFleetType": "MASTER",
    "TargetOnDemandCapacity": 1,
    "InstanceTypeConfigs": [
      {
        "InstanceType": "m5.xlarge"
      }
    ]
  }
]'




# ---------------------------
# Define Bootstrap Actions
# ---------------------------
BOOTSTRAP_ACTIONS='[
  {
    "Name": "Install dependencies",
    "Path": "'"$BOOTSTRAP_SCRIPT_S3_PATH"'",
    "Args": []
  }
]'

# ---------------------------
# Create EMR Cluster with Step
# ---------------------------
echo "Creating EMR cluster and submitting step..."

CLUSTER_ID=$(aws emr create-cluster \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --region "$REGION" \
  --name "$CLUSTER_NAME" \
  --log-uri "$LOG_URI" \
  --instance-fleets "$INSTANCE_FLEETS_CONFIG" \
  --service-role "$SERVICE_ROLE" \
  --ec2-attributes InstanceProfile="$EC2_PROFILE",SubnetId="$SUBNET_ID",KeyName="$KEY_NAME" \
  --bootstrap-actions "$BOOTSTRAP_ACTIONS" \
  --query 'ClusterId' --output text)

if [[ $? -ne 0 || -z "$CLUSTER_ID" ]]; then
  echo "Error: Failed to create EMR cluster."
  exit 1
fi

echo "Cluster created successfully with ID: $CLUSTER_ID"
echo "Cluster details:"
echo "  Cluster Name: $CLUSTER_NAME"
echo "  Release Label: $RELEASE_LABEL"
echo "  Region: $REGION"
echo "  Log URI: $LOG_URI"
echo "  Service Role: $SERVICE_ROLE"
echo "  EC2 Profile: $EC2_PROFILE"
echo "  Subnet ID: $SUBNET_ID"
echo "  Key Name: $KEY_NAME"
