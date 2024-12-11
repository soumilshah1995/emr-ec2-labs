#!/bin/bash

# ---------------------------
# Configuration
# ---------------------------
CLUSTER_NAME="My_Spark_Cluster"
RELEASE_LABEL="emr-7.5.0"
REGION="us-east-1"
ACCOUNT="XXX"
SERVICE_ROLE="arn:aws:iam::${ACCOUNT}:role/EMRServiceRole"
EC2_PROFILE="EMR_EC2_DefaultRole"
SUBNET_ID="subnet-XXXXX"
KEY_NAME="MacBookEMR"
JOB_PATH="./iceberg_job.py"
JOB_FILENAME=$(basename "$JOB_PATH")
BUCKET_NAME="XXXXX"
S3_JOB_PATH="s3://${BUCKET_NAME}/jobs/${JOB_FILENAME}"
LOG_URI="s3://${BUCKET_NAME}/logs/"
STEP_NAME="PySpark Job"

# ---------------------------
# Upload PySpark Job to S3
# ---------------------------
echo "Uploading PySpark job to S3..."
aws s3 cp "$JOB_PATH" "$S3_JOB_PATH"

if [[ $? -ne 0 ]]; then
  echo "Error: Failed to upload PySpark job to S3."
  exit 1
fi

echo "PySpark job uploaded to $S3_JOB_PATH."

# ---------------------------
# Define Step and Instance Fleets Config
# ---------------------------
STEP_CONFIG='[
  {
    "Name": "'"$STEP_NAME"'",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "--conf", "spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog",
      "--conf", "spark.sql.catalog.dev.warehouse=s3://'"${BUCKET_NAME}"'/warehouse/",
      "--conf", "spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "--conf", "spark.sql.catalog.dev.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "'"$S3_JOB_PATH"'"
    ]
  }
]'

ICEBERG_CONFIG='[
  {
    "Classification": "iceberg-defaults",
    "Properties": {
      "iceberg.enabled": "true"
    }
  }
]'

INSTANCE_FLEETS_CONFIG='[
  {
    "InstanceFleetType": "MASTER",
    "TargetSpotCapacity": 1,
    "InstanceTypeConfigs": [
      {
        "InstanceType": "m5.xlarge",
        "BidPrice": "0.1"
      }
    ],
    "LaunchSpecifications": {
      "SpotSpecification": {
        "TimeoutDurationMinutes": 60,
        "TimeoutAction": "TERMINATE_CLUSTER"
      }
    }
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
  --auto-terminate \
  --steps "$STEP_CONFIG" \
  --configurations "$ICEBERG_CONFIG" \
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
echo "  Job Path: $S3_JOB_PATH"
echo "  Step Name: $STEP_NAME"
