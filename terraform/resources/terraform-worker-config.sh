# Check AMI_ID present or not as env variable if not then set
# Ubuntu 22.04 (jammy) — systemd base required by the Uptycs EDR bake (DPA-3337)
AMI_ID() {
  echo "ami-021c7f7dcbd49b417"
}

if [ -z "${AMI_ID}" ]; then
  echo "AMI_ID is not set. Setting it now."
  # Set the AMI_ID variable
  export AMI_ID=$(AMI_ID)
else
  echo "AMI_ID is already set to: $AMI_ID"
fi

export IPV6_SUPPORT="False"