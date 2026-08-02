#!/usr/bin/env bash
# Run ON the builder EC2 instance (Amazon Linux 2 / AL2023).
# Downloads JARs into /opt/emr-extra-jars/ for baking into a custom EMR AMI.
#
# Modes:
#   (default)            curl jars from Maven Central
#   S3_JAR_PREFIX=s3://bucket/jars/   sync from S3 instead (or in addition)
#
# Env:
#   DEST                 install path (default /opt/emr-extra-jars)
#   INCLUDE_AWS_HADOOP   set to 1 to also install hadoop-aws + aws-java-sdk-bundle
#                        (can conflict with EMR-bundled libs — off by default for safety)
set -euo pipefail

DEST="${DEST:-/opt/emr-extra-jars}"
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
INCLUDE_AWS_HADOOP="${INCLUDE_AWS_HADOOP:-0}"
S3_JAR_PREFIX="${S3_JAR_PREFIX:-}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/jars"

download() {
  local url="$1"
  local out="$2"
  echo "Downloading $(basename "$out") ..."
  curl -fL --retry 3 --retry-delay 2 "$url" -o "$out"
}

echo "=== Installing extra JARs → $DEST ==="

# Extra / app JARs (safe default set for this lab)
download "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.38/bundle-2.29.38.jar" \
  "$TMP/jars/awssdk-bundle-2.29.38.jar"
download "https://repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar" \
  "$TMP/jars/caffeine-3.1.8.jar"
download "https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.11.0/commons-configuration2-2.11.0.jar" \
  "$TMP/jars/commons-configuration2-2.11.0.jar"
download "https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg/0.1.3/s3-tables-catalog-for-iceberg-0.1.3.jar" \
  "$TMP/jars/s3-tables-catalog-for-iceberg-0.1.3.jar"
download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/1.6.1/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-1.6.1.jar" \
  "$TMP/jars/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-1.6.1.jar"
download "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.24.2/snowflake-jdbc-3.24.2.jar" \
  "$TMP/jars/snowflake-jdbc-3.24.2.jar"
download "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.1.3/spark-snowflake_2.12-3.1.3.jar" \
  "$TMP/jars/spark-snowflake_2.12-3.1.3.jar"

if [[ "$INCLUDE_AWS_HADOOP" == "1" ]]; then
  echo "INCLUDE_AWS_HADOOP=1 — adding hadoop-aws + aws-java-sdk-bundle (watch for EMR conflicts)"
  download "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar" \
    "$TMP/jars/aws-java-sdk-bundle-1.12.661.jar"
  download "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    "$TMP/jars/hadoop-aws-3.3.4.jar"
fi

if [[ -n "$S3_JAR_PREFIX" ]]; then
  echo "Also syncing from $S3_JAR_PREFIX ..."
  aws s3 sync "$S3_JAR_PREFIX" "$TMP/jars/"
fi

sudo mkdir -p "$DEST"
sudo cp -f "$TMP/jars/"*.jar "$DEST/"
sudo chmod 755 "$DEST"
sudo chmod 644 "$DEST"/*.jar

echo ""
echo "Installed $(ls -1 "$DEST"/*.jar | wc -l | tr -d ' ') JARs:"
ls -lh "$DEST"
echo ""
echo "Done. Create an AMI from this instance next (scripts/03-create-ami.sh)."
