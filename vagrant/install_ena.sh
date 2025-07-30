#!/usr/bin/env bash

set -ex

echo 'Starting ENA driver installation and system updates for x64 architecture...'

# --- System Update and Build Tools Installation ---
# This script assumes an Ubuntu-based AMI (using apt-get).
# If your source_ami is RHEL/CentOS/Amazon Linux, you MUST adjust the package manager
# commands (e.g., 'yum' or 'dnf') and the initramfs command ('dracut' instead of 'update-initramfs').

echo 'Updating system packages and installing build tools...'
sudo apt-get -y update
sudo apt-get install -y build-essential dkms git linux-headers-$(uname -r)

# Check if build-essential/dkms/git/headers installed successfully
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to install essential build tools. Exiting."
    exit 1
fi

# --- ENA Driver Installation using DKMS ---
echo 'Cloning AWS ENA drivers repository...'
cd /tmp
# Use --depth 1 to only clone the latest commit, saving time and space
git clone --depth 1 https://github.com/amzn/amzn-drivers.git || { echo "ERROR: Failed to clone amzn-drivers. Exiting."; exit 1; }

# Determine the driver version from the cloned repository
# This ensures DKMS registration uses the correct version
DRIVER_VERSION=$(grep ^VERSION amzn-drivers/kernel/linux/rpm/Makefile | cut -d' ' -f2)
if [ -z "$DRIVER_VERSION" ]; then
    echo "ERROR: Could not determine ENA driver version. Exiting."
    exit 1
fi

echo "Detected ENA driver version: $DRIVER_VERSION"

# Move the driver source to /usr/src for DKMS
echo "Moving ENA driver source to /usr/src/amzn-drivers-$DRIVER_VERSION..."
sudo mv amzn-drivers /usr/src/amzn-drivers-$DRIVER_VERSION || { echo "ERROR: Failed to move amzn-drivers to /usr/src. Exiting."; exit 1; }

# Create DKMS configuration file
echo "Creating DKMS configuration file..."
sudo tee /usr/src/amzn-drivers-$DRIVER_VERSION/dkms.conf > /dev/null <<EOM
PACKAGE_NAME="ena"
PACKAGE_VERSION="$DRIVER_VERSION"
CLEAN="make -C kernel/linux/ena clean"
MAKE="make -C kernel/linux/ena/ BUILD_KERNEL=\${kernelver}"
BUILT_MODULE_NAME[0]="ena"
BUILT_MODULE_LOCATION="kernel/linux/ena"
DEST_MODULE_LOCATION[0]="/updates"
DEST_MODULE_NAME[0]="ena"
AUTOINSTALL="yes"
EOM

# Add, build, and install the ENA module with DKMS
echo "Adding ENA module to DKMS..."
sudo dkms add -m amzn-drivers -v $DRIVER_VERSION || { echo "ERROR: Failed to add ENA module to DKMS. Exiting."; exit 1; }

echo "Building ENA module with DKMS..."
sudo dkms build -m amzn-drivers -v $DRIVER_VERSION || { echo "ERROR: Failed to build ENA module with DKMS. Exiting."; exit 1; }

echo "Installing ENA module with DKMS..."
sudo dkms install -m amzn-drivers -v $DRIVER_VERSION || { echo "ERROR: Failed to install ENA module with DKMS. Exiting."; exit 1; }

# --- Kernel Module and Initramfs Configuration ---
echo 'Updating kernel module dependencies...'
sudo depmod

echo 'Rebuilding initramfs to include ENA driver for boot...'
sudo update-initramfs -u -k all || { echo "ERROR: Failed to rebuild initramfs. Exiting."; exit 1; }

# --- Optional: Disable Predictable Network Interface Names (Recommended for consistency) ---
# This helps ensure 'eth0' is used instead of 'ensX' names, which can prevent boot issues
# or network configuration problems on some systems.
echo 'Disabling predictable network interface names (if applicable)...'
# Check if GRUB_CMDLINE_LINUX exists and modify it
if grep -q "^GRUB_CMDLINE_LINUX=" /etc/default/grub; then
    sudo sed -i '/^GRUB_CMDLINE_LINUX=/s/"$/ net.ifnames=0 biosdevname=0"/' /etc/default/grub
else
    # If the line doesn't exist, add it
    echo 'GRUB_CMDLINE_LINUX="net.ifnames=0 biosdevname=0"' | sudo tee -a /etc/default/grub > /dev/null
fi
sudo update-grub || { echo "ERROR: Failed to update GRUB. Exiting."; exit 1; }

echo 'ENA driver installation and configuration complete. Rebooting instance...'
sudo reboot


# previous commands
## --- System Update and Build Tools Installation ---
## This assumes an Ubuntu-based AMI. Adjust commands for other distros (e.g., yum for RHEL/CentOS).
## These commands are generally architecture-agnostic and will install x64 packages on an x64 system.
#sudo apt-get -y update
#sudo apt-get install -y build-essential dkms git linux-headers-$(uname -r)
#
## --- ENA Driver Installation from AWS GitHub ---
#echo 'Cloning AWS ENA drivers repository...'
#cd /tmp
#git clone https://github.com/amzn/amzn-drivers.git
#cd amzn-drivers/kernel/linux/ena
#
#echo 'Compiling and installing ENA driver...'
#make
#sudo make install
#
## --- Kernel Module and Initramfs Configuration ---
#echo 'Updating kernel module dependencies...'
#sudo depmod
#
#echo 'Rebuilding initramfs to include ENA driver...'
#sudo update-initramfs -u -k all
#
## --- Optional: Disable Predictable Network Interface Names (Recommended for consistency) ---
## This helps ensure 'eth0' is used instead of 'ensX' names, which can prevent boot issues
## or network configuration problems on some systems.
#echo 'Disabling predictable network interface names...'
#sudo sed -i '/^GRUB_CMDLINE_LINUX/s/"$/ net.ifnames=0 biosdevname=0"/' /etc/default/grub
#sudo update-grub
#
#echo 'ENA driver installation and configuration complete. Rebooting instance...'
#sudo reboot