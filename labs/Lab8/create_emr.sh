#!/usr/bin/env bash
# Create a session-enabled EMR on EC2 cluster for Spark Connect (emr-spark-8.0.0+).
# Replace placeholders with values from your AWS account before running.

set -euo pipefail

PROFILE="${AWS_PROFILE:-dev}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${CLUSTER_NAME:-spark-connect-cluster}"

# --- replace these ---
ACCOUNT_ID="XXXXXXXXXXXX"
SUBNET_ID="subnet-XXXXXXXX"
MASTER_SG="sg-XXXXXXXX"   # ElasticMapReduce-master (or your EMR primary SG)
CORE_SG="sg-XXXXXXXX"     # ElasticMapReduce-slave (or your EMR core SG)
KEY_NAME="YOUR_EC2_KEY_PAIR"
SERVICE_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/service-role/AmazonEMR-ServiceRole-XXXXXXXX"
INSTANCE_PROFILE="AmazonEMR-InstanceProfile-XXXXXXXX"
# ---------------------

aws emr create-cluster \
  --profile="$PROFILE" \
  --region="$REGION" \
  --name "$CLUSTER_NAME" \
  --release-label emr-spark-8.0.0 \
  --applications Name=Spark \
  --service-role "$SERVICE_ROLE" \
  --ec2-attributes "{
    \"KeyName\": \"${KEY_NAME}\",
    \"InstanceProfile\": \"${INSTANCE_PROFILE}\",
    \"SubnetId\": \"${SUBNET_ID}\",
    \"EmrManagedMasterSecurityGroup\": \"${MASTER_SG}\",
    \"EmrManagedSlaveSecurityGroup\": \"${CORE_SG}\"
  }" \
  --instance-groups '[
    {"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"r8g.xlarge"},
    {"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"r8g.xlarge"}
  ]' \
  --session-enabled \
  --auto-termination-policy IdleTimeout=3600 \
  --tags for-use-with-amazon-emr-managed-policies=true
