#!/usr/bin/env bash
set -exuo pipefail

# Default configuration
export UPSTREAM_PROJECTS='confluentinc/kafka'
export DOCKER_REGISTRY='519856050701.dkr.ecr.us-west-2.amazonaws.com'
export DOCKER_IMAGE_REPO='docker/prod/confluentinc'
export DOCKER_IMAGE_NAME='kafka'
export DISABLE_CONCURRENT_BUILDS=true
export DOWNSTREAM_REPOS='common'
export NANO_VERSION=true

# Dynamic configuration based on branch name
export TARGET_BRANCH=${SEMAPHORE_GIT_BRANCH}
export RELEASE_TAG="v1.${TARGET_BRANCH}"
export IS_CC_JOB=false

# Ensure a specific directory exists in the user's home directory for storing files
HOME_OUTPUT_DIR="${HOME}/.build_output"
mkdir -p "${HOME_OUTPUT_DIR}"
export HOME_OUTPUT_DIR

# CC release configs
BRANCH_NAME=${SEMAPHORE_GIT_BRANCH} # Use Semaphore environment variable to derive the branch name
matcher=$(echo $BRANCH_NAME | grep -oP 'v([0-9]+\.[0-9]+)\.((x|[0-9]+))-([1-9]+\.[0-9]+\.[0-9]+)-[0-9]+(.*)' || true) 

if [[ $matcher && "$SEMAPHORE_GIT_REF_TYPE" != "pull-request" ]]; then
    export IS_CC_JOB=true

    # Check if the artifact exists for the current branch
    if artifact_output=$(artifact pull project "${BRANCH_NAME}_build_number.txt" 2>&1); then
        # If artifact pull succeeds, this block executes.
        echo "Artifact pulled successfully."
        # Read the current version from the artifact
        build_number=$(cat "${BRANCH_NAME}_build_number.txt")

        # Increment the version
        new_build_number=$((build_number + 1))

        echo $new_build_number > "${BRANCH_NAME}_build_number.txt"

    elif echo "$artifact_output" | grep -q '404 status code'; then
        # If the artifact doesn't exist, use the Semaphore workflow number
        new_build_number=$SEMAPHORE_WORKFLOW_NUMBER
    else
        # If artifact pull fails for any other reason, this block executes.
        echo "Artifact pull failed for a reason other than 404."
        exit 1  # Exit the script with an error status.
    fi

    # Replace 'x' with build number and 'v0' with 'v1'
    export RELEASE_TAG=$(echo $matcher | sed -E 's/x/'"$new_build_number"'/; s/v0/v1/')
    echo "The release tag for the current build is: $RELEASE_TAG"
    echo $RELEASE_TAG > $HOME_OUTPUT_DIR/.release_tag
    # Extract CP release tag, assuming it's the 6th group in the match
    export CP_RELEASE_TAG=$(echo $matcher | awk '{print $6}')
fi

if echo "$SEMAPHORE_GIT_BRANCH" | grep -E '(-rc|-beta|-cp|-post$)' &>/dev/null; then
  export IS_RELEASE_JOB="true"
else
  export IS_RELEASE_JOB="false"
fi

export OIDC_TOKEN=${SEMAPHORE_OIDC_TOKEN}

