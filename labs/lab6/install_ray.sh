#!/bin/bash

# Check if we're on the master node
if grep isMaster /mnt/var/lib/info/instance.json | grep false; then
    # Worker node setup
    sudo python3 -m pip install numpy==1.23.5  # Install a compatible NumPy version
    sudo python3 -m pip install -U ray[all]==2.0.0


    RAY_HEAD_IP=$(grep "\"masterHost\":" /emr/instance-controller/lib/info/extraInstanceData.json | cut -f2 -d: | cut -f2 -d\")

    sudo mkdir -p /tmp/ray/
    sudo chmod a+rwx -R /tmp/ray/

    cat >/tmp/start_ray.sh <<EOF
#!/bin/bash
echo -n "Waiting for Ray leader node..."
while ( ! nc -z -v $RAY_HEAD_IP 6379); do echo -n "."; sleep 5; done
echo -e "\nRay available...starting!"
ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076 --disable-usage-stats
EOF

    chmod +x /tmp/start_ray.sh
    nohup /tmp/start_ray.sh &
    exit 0
fi

# Master node setup
cat >/tmp/install_ray.sh <<EOF
#!/bin/bash
NODEPROVISIONSTATE="waiting"
echo -n "Waiting for EMR to provision..."
while [ ! "\$NODEPROVISIONSTATE" == "SUCCESSFUL" ]; do
    echo -n "."
    sleep 10
    NODEPROVISIONSTATE=\$(sed -n '/localInstance [{]/,/[}]/{
    /nodeProvisionCheckinRecord [{]/,/[}]/ {
    /status: / { p }
    /[}]/a
    }
    /[}]/a
    }' /emr/instance-controller/lib/info/job-flow-state.txt | awk ' { print \$2 }')
done

echo "EMR provisioned! Continuing with installation..."

# Update Python and install dependencies
sudo yum update -y
sudo yum install -y python3-devel

# Install compatible versions of NumPy and other dependencies
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install numpy==1.23.5
sudo python3 -m pip install pandas==1.5.3
sudo python3 -m pip install pyarrow==11.0.0
sudo python3 -m pip install  pydantic==1.10.8

# Install Ray
sudo python3 -m pip install -U ray[all]==2.0.0

sudo mkdir -p /tmp/ray/
sudo chmod a+rwx -R /tmp/ray/

ray start --head --port=6379 --object-manager-port=8076 --disable-usage-stats
EOF

chmod +x /tmp/install_ray.sh
nohup /tmp/install_ray.sh &
