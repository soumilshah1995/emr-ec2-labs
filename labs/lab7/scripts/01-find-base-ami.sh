#!/usr/bin/env bash
# Resolve the correct *base* Amazon Linux AMI for an EMR custom AMI bake.
#
# Inputs (any combo):
#   EMR_RELEASE=emr-7.13.0          required unless CLUSTER_ID is set
#   INSTANCE_TYPE=r8g.xlarge        preferred — derives arch automatically
#   ARCH=arm64|x86_64               override / fallback
#   CLUSTER_ID=j-XXXX               optional — read release + first instance type from an existing/past EMR cluster
#   AWS_REGION=us-east-1
#
# Rules (AWS-supported):
#   EMR 5.30–6.x  → Amazon Linux 2
#   EMR 7.x       → Amazon Linux 2023, kernel 6.1 (NOT 6.12)
#   NEVER use an EMR AMI as the base
#   AMI arch MUST match every instance type on the cluster (Graviton = arm64)
#
# Output: prints BASE_AMI_ID and export hints on stderr/stdout.
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-}"
EMR_RELEASE="${EMR_RELEASE:-}"
INSTANCE_TYPE="${INSTANCE_TYPE:-}"
ARCH="${ARCH:-}"

usage() {
  cat <<'EOF'
Usage:
  # From EMR release + instance type (recommended)
  EMR_RELEASE=emr-7.13.0 INSTANCE_TYPE=r8g.xlarge ./01-find-base-ami.sh

  # Infer release + instance type from a past/current cluster
  CLUSTER_ID=j-08609969V54I17N71V8 ./01-find-base-ami.sh

  # Explicit arch override
  EMR_RELEASE=emr-6.15.0 ARCH=x86_64 ./01-find-base-ami.sh

Graviton families (arm64): *g* after generation — m6g, m7g, r8g, c7g, ...
Intel/AMD (x86_64): m5, m6i, r5, r6i, ...
EOF
}

for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
  esac
done

# --- Optional: learn from an EMR cluster ---
# Only consult EMR when something is still missing, so an exported CLUSTER_ID
# from a previous run never overrides explicit inputs.
if [[ -n "$CLUSTER_ID" ]] && { [[ -z "$EMR_RELEASE" ]] || { [[ -z "$INSTANCE_TYPE" ]] && [[ -z "$ARCH" ]]; }; }; then
  echo "Reading EMR cluster $CLUSTER_ID ..." >&2
  if [[ -z "$EMR_RELEASE" ]]; then
    EMR_RELEASE=$(aws emr describe-cluster --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" \
      --query 'Cluster.ReleaseLabel' --output text)
  fi
  if [[ -z "$INSTANCE_TYPE" && -z "$ARCH" ]]; then
    # Prefer fleet specs; fall back to instance groups / listed instances
    INSTANCE_TYPE=$(aws emr list-instance-fleets --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" \
      --query 'InstanceFleets[0].InstanceTypeSpecifications[0].InstanceType' --output text 2>/dev/null || true)
    if [[ -z "$INSTANCE_TYPE" || "$INSTANCE_TYPE" == "None" ]]; then
      INSTANCE_TYPE=$(aws emr list-instances --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" \
        --query 'Instances[0].InstanceType' --output text 2>/dev/null || true)
    fi
    if [[ -z "$INSTANCE_TYPE" || "$INSTANCE_TYPE" == "None" ]]; then
      INSTANCE_TYPE=$(aws emr describe-cluster --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" \
        --query 'Cluster.InstanceGroups[0].InstanceType' --output text 2>/dev/null || true)
    fi
  fi
fi

if [[ -z "$EMR_RELEASE" ]]; then
  echo "Error: set EMR_RELEASE=emr-X.Y.Z or CLUSTER_ID=j-..." >&2
  usage
  exit 1
fi

# Normalize release label
if [[ "$EMR_RELEASE" != emr-* ]]; then
  EMR_RELEASE="emr-$EMR_RELEASE"
fi

major=$(echo "$EMR_RELEASE" | sed -E 's/^emr-([0-9]+).*/\1/')
if ! [[ "$major" =~ ^[0-9]+$ ]]; then
  echo "Error: cannot parse major version from EMR_RELEASE=$EMR_RELEASE" >&2
  exit 1
fi

# --- Resolve architecture ---
arch_from_instance_type() {
  local itype="$1"
  # Authoritative: ask EC2
  local arches
  arches=$(aws ec2 describe-instance-types --region "$AWS_REGION" \
    --instance-types "$itype" \
    --query 'InstanceTypes[0].ProcessorInfo.SupportedArchitectures' \
    --output text 2>/dev/null || true)
  if [[ "$arches" == *arm64* && "$arches" != *x86_64* ]]; then
    echo arm64
  elif [[ "$arches" == *x86_64* && "$arches" != *arm64* ]]; then
    echo x86_64
  elif [[ "$arches" == *arm64* ]]; then
    # rare dual — prefer arm64 if user picked a g-family name
    if [[ "$itype" =~ ^[a-z]+[0-9]+g ]]; then echo arm64; else echo x86_64; fi
  else
    # Heuristic fallback: m6g / r8g / c7g / a1 → arm64
    if [[ "$itype" =~ ^[a-z]+[0-9]+g ]] || [[ "$itype" =~ ^a1\. ]]; then
      echo arm64
    else
      echo x86_64
    fi
  fi
}

if [[ -n "$INSTANCE_TYPE" && "$INSTANCE_TYPE" != "None" ]]; then
  DERIVED_ARCH=$(arch_from_instance_type "$INSTANCE_TYPE")
  if [[ -n "$ARCH" && "$ARCH" != "$DERIVED_ARCH" ]]; then
    echo "Warning: ARCH=$ARCH conflicts with INSTANCE_TYPE=$INSTANCE_TYPE (→ $DERIVED_ARCH). Using $DERIVED_ARCH." >&2
  fi
  ARCH="$DERIVED_ARCH"
elif [[ -z "$ARCH" ]]; then
  ARCH=x86_64
  echo "Warning: no INSTANCE_TYPE/ARCH set — defaulting to x86_64. Set INSTANCE_TYPE for Graviton fleets." >&2
fi

# Suggested builder size (same arch, small)
if [[ "$ARCH" == "arm64" ]]; then
  SUGGESTED_BUILDER="${SUGGESTED_BUILDER:-m7g.xlarge}"
else
  SUGGESTED_BUILDER="${SUGGESTED_BUILDER:-m5.xlarge}"
fi

# --- Pick Linux family + AMI ---
if [[ "$major" -ge 7 ]]; then
  LINUX_FAMILY="Amazon Linux 2023"
  KERNEL_NOTE="kernel-6.1 (required for EMR 7.x; do NOT use 6.12)"
  if [[ "$ARCH" == "arm64" ]]; then
    SSM_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-arm64"
    NAME_FILTER="al2023-ami-2023.*-kernel-6.1-arm64"
  else
    SSM_PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-x86_64"
    NAME_FILTER="al2023-ami-2023.*-kernel-6.1-x86_64"
  fi
elif [[ "$major" -ge 5 ]]; then
  LINUX_FAMILY="Amazon Linux 2"
  KERNEL_NOTE="AL2 HVM gp2"
  SSM_PARAM=""
  if [[ "$ARCH" == "arm64" ]]; then
    NAME_FILTER="amzn2-ami-hvm-2.0.*-arm64-gp2"
  else
    NAME_FILTER="amzn2-ami-hvm-2.0.*-x86_64-gp2"
  fi
else
  echo "Error: EMR $EMR_RELEASE is too old for this lab (need 5.7+ custom AMI support)." >&2
  exit 1
fi

BASE_AMI_ID=""
BASE_AMI_NAME=""

if [[ -n "$SSM_PARAM" ]]; then
  BASE_AMI_ID=$(aws ssm get-parameter --region "$AWS_REGION" --name "$SSM_PARAM" \
    --query 'Parameter.Value' --output text 2>/dev/null || true)
  if [[ -n "$BASE_AMI_ID" && "$BASE_AMI_ID" != "None" ]]; then
    BASE_AMI_NAME=$(aws ec2 describe-images --region "$AWS_REGION" --image-ids "$BASE_AMI_ID" \
      --query 'Images[0].Name' --output text)
  fi
fi

if [[ -z "$BASE_AMI_ID" || "$BASE_AMI_ID" == "None" ]]; then
  read -r BASE_AMI_ID BASE_AMI_NAME _ <<<"$(
    aws ec2 describe-images --region "$AWS_REGION" --owners amazon \
      --filters \
        "Name=name,Values=${NAME_FILTER}" \
        "Name=state,Values=available" \
        "Name=architecture,Values=${ARCH}" \
      --query 'sort_by(Images,&CreationDate)[-1].[ImageId,Name,CreationDate]' \
      --output text
  )"
fi

if [[ -z "$BASE_AMI_ID" || "$BASE_AMI_ID" == "None" ]]; then
  echo "Error: could not find a base AMI for EMR=$EMR_RELEASE ARCH=$ARCH" >&2
  exit 1
fi

# Confirm arch on the AMI
AMI_ARCH=$(aws ec2 describe-images --region "$AWS_REGION" --image-ids "$BASE_AMI_ID" \
  --query 'Images[0].Architecture' --output text)

cat >&2 <<EOF

╔══════════════════════════════════════════════════════════════╗
║  EMR custom AMI — base image resolution                     ║
╠══════════════════════════════════════════════════════════════╣
║  EMR release     : $EMR_RELEASE
║  Linux family    : $LINUX_FAMILY
║  Kernel note     : $KERNEL_NOTE
║  Instance type   : ${INSTANCE_TYPE:-'(not set)'}
║  Architecture    : $ARCH  (AMI reports: $AMI_ARCH)
║  Builder type    : $SUGGESTED_BUILDER
║  Base AMI        : $BASE_AMI_ID
║  Base AMI name   : $BASE_AMI_NAME
╚══════════════════════════════════════════════════════════════╝

Hard rules:
  • NEVER snapshot an EMR node / EMR AMI
  • AMI arch MUST match fleet (r8g/m7g/... → arm64; m5/m6i/... → x86_64)
  • Mixed x86+Graviton fleets need TWO custom AMIs

EOF

if [[ "$AMI_ARCH" != "$ARCH" ]]; then
  echo "Error: AMI architecture $AMI_ARCH != required $ARCH" >&2
  exit 1
fi

# Machine-readable primary output (for scripting)
echo "$BASE_AMI_ID"

cat >&2 <<EOF
Export:
  export EMR_RELEASE=$EMR_RELEASE
  export ARCH=$ARCH
  export BASE_AMI_ID=$BASE_AMI_ID
  export BUILDER_INSTANCE_TYPE=$SUGGESTED_BUILDER
$([ -n "${INSTANCE_TYPE:-}" ] && [ "$INSTANCE_TYPE" != "None" ] && echo "  export INSTANCE_TYPE=$INSTANCE_TYPE")

Next (Lab 7): launch a *${SUGGESTED_BUILDER}* builder from \$BASE_AMI_ID (same arch), install jars, create AMI, then launch EMR with --custom-ami-id.
EOF
