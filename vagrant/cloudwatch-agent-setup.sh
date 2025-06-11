#!/bin/bash

# This shell script installs Amazon CloudWatch Agent on all EC2 instances that
# are provisioned and spinned-off as worker-nodes for system-test runs.
#
# This helps in collecting and monitoring useful system-level metrics from these EC2
# instances aka worker-nodes to identify performance patterns and anomalies,
# and devise methods to address them.
#
# Example : Disk Space Utilization and Memory Utilization Metrics.

set -ex

architecture=amd64

if [ "$JDK_ARCH" == "x64" ]; then
    architecture=amd64
fi

echo "JDK_ARCH value: $JDK_ARCH, Architecture value: $architecture JDK_MAJOR: $JDK_MAJOR, JDK_FULL => $JDK_FULL"

downloadUrl="https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/$architecture/latest/amazon-cloudwatch-agent.deb"

echo "Installing amazon-cloudwatch-agent...."
wget -nv $downloadUrl

dpkg -i -E ./amazon-cloudwatch-agent.deb
apt-get -y update && apt-get -y install collectd

#apt-get -y update && apt-get -y install jq
echo "current_directory_start"
pwd
pushd /tmp

# Get the latest stable jq release URL from GitHub releases page (check for the latest version)
# As of now, jq 1.7.1 is the latest stable.
# Find the correct download URL for your architecture (x64 for Linux)
JQ_URL="https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64" # Use jq-linux-amd64 for x64

# Download the binary
wget "$JQ_URL" -O jq

# Make it executable
chmod +x jq

# Copy it to a directory in your PATH (e.g., /usr/local/bin)
sudo cp jq /usr/local/bin/

# Verify the version
jq --version

###
echo "current_directory_end"
popd
pwd

CONFIG_FILE="/tmp/cloudwatch-agent-configuration.json"
TEMP_FILE="cloudwatch-agent-configuration.json.tmp"

# traverse the JSON file and update the SemaphoreJobId value
jq \
  --arg job_id "$SEMAPHORE_JOB_ID" \
  '
  walk(
    # Check if the current element is an object and has 'append_dimensions'
    # and if 'append_dimensions' has 'SemaphoreJobId'
    if type == "object" and has("append_dimensions") and (.append_dimensions | has("SemaphoreJobId")) then
      # If true, update the SemaphoreJobId value
      .append_dimensions.SemaphoreJobId = $job_id
    else
      # Otherwise, return the element unchanged
      .
    end
  )
  ' \
   "$CONFIG_FILE" > "$TEMP_FILE"

# todo remove
echo "SemaphoreJobId is ", $SEMAPHORE_JOB_ID

# Check if the jq command was successful
if [ $? -eq 0 ]; then
  cp "$TEMP_FILE" /opt/aws/amazon-cloudwatch-agent/bin/config.json
  echo "Updated file with SEMAPHORE_JOB_ID"
  cat /opt/aws/amazon-cloudwatch-agent/bin/config.json
else
  echo "Error: Failed to update '$CONFIG_FILE' using jq."
  rm -f "$TEMP_FILE"
  exit 1
fi

/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -m ec2 -a status
echo "Installation completed for amazon-cloudwatch-agent !!!"