# Lab 8: Interactive PySpark Anywhere with Spark Connect on EMR on EC2

Use [Spark Connect on Amazon EMR on EC2](https://aws.amazon.com/blogs/big-data/announcing-spark-connect-on-amazon-emr-on-ec2-interactive-pyspark-anywhere/) to develop PySpark from your laptop while Spark runs on a shared EMR cluster.

## What you will learn

- Create an EMR cluster with **Spark Connect sessions enabled** (`emr-spark-8.0.0+`)
- Start or reuse a named interactive session
- Fetch the session endpoint + auth token
- Connect a local `SparkSession` over Spark Connect and run SQL

## Prerequisites

- AWS CLI configured (`--profile` / region)
- IAM permissions for `elasticmapreduce:StartSession`, `GetSession`, `GetSessionEndpoint`, `ListSessions`, `TerminateSession`, plus `iam:PassRole`
- EMR **service role** with `AmazonEMRServicePolicyForSessions` (and `AmazonEMRServicePolicy_v2`)
- Python 3.9+ and packages from `requirements.txt`
- Local PySpark version must match the cluster (`4.0.2` for `emr-spark-8.0.0`)

## Files

| File | Purpose |
|------|---------|
| `create_emr.sh` | Create a session-enabled EMR cluster (placeholders only — fill your subnet/SG/roles) |
| `start_session.sh` | CLI helper to start a session by cluster id |
| `hello_spark_connect.py` | Resolve cluster **by name**, create-or-reuse session, connect, run hello-world |
| `requirements.txt` | `pyspark[connect]==4.0.2`, gRPC deps, `boto3` |

## Steps

### 1. Create the cluster

Edit placeholders in `create_emr.sh` (`ACCOUNT_ID`, `SUBNET_ID`, security groups, key pair, IAM roles), then:

```bash
chmod +x create_emr.sh
./create_emr.sh
```

Wait until the cluster is in `WAITING`. Confirm **Spark Connect Endpoint: Enabled** in the EMR console.

### 2. Install local client

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Connect and run hello-world

Edit the cluster / session names at the bottom of `hello_spark_connect.py` if needed, then:

```bash
python3 hello_spark_connect.py
```

The script will:

1. Resolve cluster id from cluster name  
2. Reuse an existing session with that name, or create one  
3. Wait until the session is ready  
4. Print endpoint / host / session id (token redacted)  
5. Run `SELECT 'Hello from EMR on EC2'`

### 4. Cleanup

```bash
aws emr terminate-session --cluster-id j-XXXX --session-id is-XXXX --profile dev
aws emr terminate-clusters --cluster-ids j-XXXX --profile dev
```

## Notes

- Do **not** commit `.pem` key files, real subnet/VPC/SG IDs, or account-specific ARNs.
- Auth tokens expire (~1 hour). Re-run `hello_spark_connect.py` to refresh.
- `spark.stop()` only closes the local client; terminate the session (or idle timeout) to free cluster resources.
