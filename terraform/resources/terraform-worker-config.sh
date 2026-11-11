# Check AMI_ID present or not as env variable if not then set
AMI_ID() {
  # Ubuntu 22.04 jammy (Nitro-compatible) for c6a. DPA-3043
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