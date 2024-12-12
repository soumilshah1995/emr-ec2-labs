#!/bin/bash

# Print a message to indicate the start of the bootstrap script
echo "Starting bootstrap script..."

# Install Faker and pyarrow using pip3
echo "Installing Faker and pyarrow..."
sudo pip3 install Faker pyarrow

# Check if the installation was successful
if [ $? -eq 0 ]; then
  echo "Faker and pyarrow installed successfully."
else
  echo "Error: Failed to install Faker and pyarrow."
  exit 1
fi

# Verify the installation by checking the package versions
echo "Verifying installation..."
FAKER_VERSION=$(pip3 show Faker | grep Version | cut -d ' ' -f2)
PYARROW_VERSION=$(pip3 show pyarrow | grep Version | cut -d ' ' -f2)

echo "Faker version: $FAKER_VERSION"
echo "pyarrow version: $PYARROW_VERSION"

echo "Bootstrap script completed successfully."
