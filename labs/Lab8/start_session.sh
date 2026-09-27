#!/usr/bin/env bash
# Start (or inspect) a Spark Connect session on a session-enabled EMR cluster.
# Prefer hello_spark_connect.py for create-or-reuse by session name.

set -euo pipefail

PROFILE="${AWS_PROFILE:-dev}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-j-XXXXXXXXXXXXX}"
SESSION_NAME="${SESSION_NAME:-my-session}"

aws emr start-session \
  --cluster-id "$CLUSTER_ID" \
  --name "$SESSION_NAME" \
  --profile="$PROFILE" \
  --region="$REGION"

# After start-session returns, set SESSION_ID from the output, then:
# SESSION_ID="is-XXXXXXXXXXXXX"
# aws emr get-session \
#   --cluster-id "$CLUSTER_ID" \
#   --session-id "$SESSION_ID" \
#   --profile="$PROFILE" \
#   --region="$REGION"
#
# aws emr get-session-endpoint \
#   --cluster-id "$CLUSTER_ID" \
#   --session-id "$SESSION_ID" \
#   --profile="$PROFILE" \
#   --region="$REGION"
