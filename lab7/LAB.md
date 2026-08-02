# Lab 7: Learn How to Speed Up EMR Bootstrapping with a Custom AMI 

When working with **Amazon EMR** in enterprise environments, cluster startup time can become a bottleneck.

A common pattern is installing the same dependencies every time a new cluster is created:

- JAR files required by your Spark applications
- Python packages and libraries
- Internal artifacts from repositories such as **JFrog**
- Other runtime dependencies and configurations

Every EMR cluster has to download and install these components during the **bootstrap** phase, which increases cluster provisioning time and delays job execution.

In this lab, you will bake those dependencies into a **custom AMI** once, then launch EMR clusters that already have everything on disk.

| Approach | What happens at cluster start |
|----------|-------------------------------|
| **Bootstrap (slow)** | Every node downloads hundreds of MB of JARs / packages |
| **Custom AMI (fast)** | Dependencies are already on disk (e.g. `/opt/emr-extra-jars/`) |

You will learn:

- Why EMR bootstrap actions can slow down cluster creation
- How to find the correct **base Amazon Linux AMI** for your EMR release
- How to build a custom AMI with your enterprise dependencies (click-ops friendly)
- How to launch an EMR cluster using your custom AMI

---

## Lab flow (big picture)

```text
1. Find the base AMI          ← scripts/01-find-base-ami.sh
2. Launch a builder EC2       ← AWS Console click-ops (or CLI)
3. Install JARs on the EC2    ← scripts/02-install-jars-on-instance.sh
4. Create a custom AMI        ← Console "Create image" or scripts/03-create-ami.sh
5. Launch EMR with custom AMI ← EMR Console / CLI (--custom-ami-id)
```

```text
lab7/
  LAB.md                              ← you are here
  configs/
    spark-defaults-extra-jars.json    ← optional Spark config for EMR
  scripts/
    01-find-base-ami.sh               ← Step 1
    02-install-jars-on-instance.sh    ← Step 3 (run ON the builder EC2)
    03-create-ami.sh                  ← Step 4 (optional CLI)
    05-cleanup-lab-resources.sh       ← cleanup when done
```

---

## Prerequisites

- AWS account + Console access (EC2, EMR, IAM)
- AWS CLI configured (`aws configure` or `AWS_PROFILE`)
- A VPC subnet + security group you can use for EC2 / EMR
- Know which **EMR release** and **instance type** you plan to use

### Match Linux + CPU to your EMR cluster

| Your EMR | Instance examples | Base OS | Architecture |
|----------|-------------------|---------|--------------|
| **6.x** | `m5`, `m6i`, `r5` | Amazon Linux **2** | x86_64 |
| **6.x** | `m6g`, `r6g`, `c6g` | Amazon Linux **2** | **arm64** |
| **7.x** | `m5`, `m6i`, `r5` | Amazon Linux **2023** (kernel **6.1**) | x86_64 |
| **7.x** | `m7g`, `r8g`, `c7g` | Amazon Linux **2023** (kernel **6.1**) | **arm64** |

### Hard rules (read once)

1. **Never** create a custom AMI from an EMR node / EMR AMI. Always start from a plain Amazon Linux AMI.
2. Bake JARs to **`/opt/emr-extra-jars/`** — not `/usr/lib/spark/jars/` (Spark is installed later by EMR).
3. AMI architecture must match your EMR instance types (`m5` = x86_64, `r8g` / `m7g` = arm64).

Docs: [AWS EMR custom AMI](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-custom-ami.html)

---

## Step 0 — Set your environment

From your laptop:

```bash
cd labs/lab7/scripts

export AWS_REGION="${AWS_REGION:-us-east-1}"
# export AWS_PROFILE=your-profile

# Fill these in for your account (needed for EMR later)
export SUBNET_ID=subnet-xxxxxxxx
export SG_ID=sg-xxxxxxxx
export LOG_URI=s3://YOUR_BUCKET/emr-logs/
```

Pick the EMR release + instance type you will use later (example: EMR 7.13 on Graviton):

```bash
export EMR_RELEASE=emr-7.13.0
export INSTANCE_TYPE=r8g.xlarge
```

---

## Step 1 — Find the base AMI (do this first)

This is the most important step. EMR custom AMIs must be built from a **supported Amazon Linux base image** — not from an EMR AMI.

Run:

```bash
EMR_RELEASE=emr-7.13.0 INSTANCE_TYPE=r8g.xlarge ./01-find-base-ami.sh
```

Or infer from an existing / past EMR cluster:

```bash
CLUSTER_ID=j-XXXXXXXXXXXXX ./01-find-base-ami.sh
```

The script prints something like:

```text
╔══════════════════════════════════════════════════════════════╗
║  EMR custom AMI — base image resolution                     ║
║  EMR release     : emr-7.13.0
║  Linux family    : Amazon Linux 2023
║  Architecture    : arm64
║  Builder type    : m7g.xlarge
║  Base AMI        : ami-0abcdef1234567890
╚══════════════════════════════════════════════════════════════╝
```

Copy the values it suggests:

```bash
export BASE_AMI_ID=ami-0abcdef1234567890   # ← from script output
export BUILDER_INSTANCE_TYPE=m7g.xlarge    # ← same arch as your EMR fleet
export ARCH=arm64
export EMR_RELEASE=emr-7.13.0
```

You now have the **base AMI ID**. Next you launch an EC2 from it.

---

## Step 2 — Create a builder EC2 (click-ops)

You can do this entirely in the **AWS Console** — no CLI required.

1. Open **EC2 → Instances → Launch instance**
2. **Name:** `emr-ami-builder`
3. **Application and OS Images (AMI):**
   - Choose **Browse more AMIs** → **My AMIs / Community / Amazon** search, **or**
   - Paste your `BASE_AMI_ID` from Step 1 into the AMI search box
4. **Instance type:** use the builder type from Step 1 (e.g. `m7g.xlarge` for arm64, `m5.xlarge` for x86_64)
5. **Key pair:** optional if you will use **Session Manager (SSM)**; otherwise pick an SSH key
6. **Network:**
   - Same VPC/subnet you will use for EMR (`SUBNET_ID`)
   - Security group that allows outbound internet (to download JARs) and SSM/SSH as needed
7. **IAM instance profile (recommended):** attach a role with `AmazonSSMManagedInstanceCore` so you can connect without SSH
8. **Storage:** 20–30 GB gp3 is enough for this lab
9. Click **Launch instance**

Wait until the instance is **Running** (and SSM shows **Online** if you use Session Manager).

Note the **Instance ID** (`i-xxxxxxxx`) — you need it in Steps 3–4.

<details>
<summary>Optional: launch the builder with CLI instead</summary>

```bash
BUILDER_INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$BASE_AMI_ID" \
  --instance-type "$BUILDER_INSTANCE_TYPE" \
  --subnet-id "$SUBNET_ID" \
  --security-group-ids "$SG_ID" \
  --iam-instance-profile "Name=YOUR_INSTANCE_PROFILE" \
  --associate-public-ip-address \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=emr-ami-builder},{Key=Project,Value=lab7}]' \
  --query 'Instances[0].InstanceId' --output text)

echo "$BUILDER_INSTANCE_ID"
aws ec2 wait instance-running --instance-ids "$BUILDER_INSTANCE_ID"
```

</details>

---

## Step 3 — Install JARs on the builder EC2

Connect to the instance:

- **Console:** EC2 → select instance → **Connect** → **Session Manager**, or
- **SSH:** `ssh -i your-key.pem ec2-user@<public-ip>`

On the builder, copy the install script up (or paste it), then run:

```bash
# From your laptop: copy the script onto the instance (example with SSM / scp)
# Or paste scripts/02-install-jars-on-instance.sh onto the instance, then:

sudo bash 02-install-jars-on-instance.sh
```

Optional: sync JARs from your own S3 / JFrog mirror instead of Maven:

```bash
S3_JAR_PREFIX=s3://YOUR_BUCKET/jars/ sudo bash 02-install-jars-on-instance.sh
```

Verify:

```bash
ls -lh /opt/emr-extra-jars/
```

You should see Iceberg, Snowflake, S3 Tables, and other lab JARs listed.

> **Enterprise tip:** This is where you would also `pip install` Python packages, pull internal artifacts from JFrog, drop config files, etc. Anything baked here will already be present when EMR nodes boot.

---

## Step 4 — Create the custom AMI (click-ops)

Still in the **EC2 Console**:

1. Select your **builder** instance (`emr-ami-builder`)
2. **Actions → Image and templates → Create image**
3. **Image name:** `emr-extra-jars-YYYYMMDD` (example: `emr-extra-jars-20260802`)
4. **Description:** `EMR custom AMI with pre-baked JARs under /opt/emr-extra-jars`
5. Leave **No reboot** unchecked for a consistent filesystem (instance will reboot briefly)
6. Click **Create image**

Wait until the AMI status is **Available** (AMI → Images → AMIs). Copy the new **AMI ID** (`ami-yyyyyyyy`).

You can **terminate** the builder instance once the AMI is available — you no longer need it.

<details>
<summary>Optional: create the AMI with CLI</summary>

```bash
export BUILDER_INSTANCE_ID=i-xxxxxxxx
./03-create-ami.sh
# wait until available, then:
export CUSTOM_AMI_ID=ami-yyyyyyyy
```

</details>

```bash
export CUSTOM_AMI_ID=ami-yyyyyyyy   # ← your new AMI from the console
```

---

## Step 5 — Create an EMR cluster with your custom AMI

### Console (click-ops)

1. Open **Amazon EMR → Create cluster**
2. **EMR release:** must match what you baked for (e.g. `emr-7.13.0`)
3. **Application:** Spark (and anything else you need)
4. **Instance type:** must match AMI architecture (e.g. `r8g.xlarge` for arm64)
5. Under **Advanced** / **Software** / **Hardware** settings, find **Custom AMI ID** and paste `$CUSTOM_AMI_ID`
6. (Optional) Add configuration JSON from `configs/spark-defaults-extra-jars.json` so Spark loads jars from `/opt/emr-extra-jars/`
7. Set subnet, roles, log URI as usual
8. **Create cluster**

No jar-download bootstrap action is required for these JARs — they are already on the AMI.

### CLI sketch

```bash
aws emr create-cluster \
  --name "lab7-custom-ami" \
  --release-label "$EMR_RELEASE" \
  --custom-ami-id "$CUSTOM_AMI_ID" \
  --applications Name=Spark \
  --use-default-roles \
  --ec2-attributes "SubnetId=${SUBNET_ID}" \
  --instance-type "$INSTANCE_TYPE" \
  --instance-count 3 \
  --configurations file://../configs/spark-defaults-extra-jars.json \
  --log-uri "$LOG_URI"
```

Once the cluster is **Waiting**, SSH / EMR Notebook / Spark shell and confirm jars exist on a node:

```bash
ls -lh /opt/emr-extra-jars/
```

---

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Cluster never reaches READY | Wrong base AMI / arch; check EC2 console system log |
| `AMI architecture must be arm64` | Cluster is Graviton; your custom AMI is x86_64 — rebuild with matching arch |
| EMR 7.x fails with AL2 custom AMI | Wrong OS — EMR 7 needs **AL2023 kernel 6.1** |
| Classpath / `NoClassDefFoundError` | Conflicting JARs vs EMR-bundled libs — drop AWS/Hadoop overrides first |
| Works on `m5`, fails on `r8g` | Architecture mismatch — bake an **arm64** AMI |

---

## Cleanup

```bash
cd labs/lab7/scripts
DRY_RUN=1 ./05-cleanup-lab-resources.sh   # preview
./05-cleanup-lab-resources.sh             # terminate builders + deregister lab AMIs
```

Also terminate any EMR cluster you created for the lab when finished.

---

## What you should walk away with

1. **Find base AMI first** — Linux family + CPU arch must match your EMR release and instance types.
2. **Builder EC2 (click-ops)** — launch from that base AMI and install dependencies once.
3. **Create AMI** — snapshot the builder.
4. **Launch EMR with `--custom-ami-id`** — clusters start faster because bootstrap no longer downloads the same JARs every time.

Bootstrap = install at **runtime** (slow, every cluster).  
Custom AMI = install at **build time** (fast, every cluster reuses the bake).
