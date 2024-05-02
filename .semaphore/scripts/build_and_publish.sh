#!/bin/bash

set -exuo pipefail

BRANCH=${TARGET_BRANCH} make clone/kafka
make clone

if [ "${IS_CC_JOB}" == "true" ]; then
  components=$(make show-projects)
  for component in $components; do
    if [ "$component" != "kafka" ]; then
      cd ".src/${component}"
      if [[ ! `git checkout ${TARGET_BRANCH}` ]]; then
        git fetch --tags
        git checkout -b ${TARGET_BRANCH} `git tag -l 'v${CP_RELEASE_TAG}-[0-9]*' --sort=-v:refname | head -1`
      fi
      cd -
    fi
  done
elif [ "${SEMAPHORE_GIT_REF_TYPE}" == "pull-request" ]; then
  BRANCH=${TARGET_BRANCH} make checkout
fi

export SKIP_TESTS=true
export MAVEN_OPTS=-XX:MaxPermSize=128M

#Compile projects
if [ "${NANO_VERSION}" == "true" ] && [ ! "${IS_RELEASE_JOB}" == "true" ]; then
  CI_TOOLS_CMD=". ci-tools ci-update-version"
  if [ "${IS_CC_JOB}" == "true" ]; then
    CI_TOOLS_CMD+=" --version=${RELEASE_TAG}"
  fi
bash -c "$CI_TOOLS_CMD"
fi
make compile/kafka
make SKIP_TESTS=false compile

unset SKIP_TESTS
unset MAVEN_OPTS

export BRANCH=${TARGET_BRANCH}
export DOCKER_IMAGE_VERSION=${RELEASE_TAG}

#Set upstream tracking
if [ "${IS_CC_JOB}" != "true" ]; then
  for i in $(make show-projects); do if [ "${i}" != "kafka" ]; then make track/"${i}"; fi; done
fi

#Package docker image
make clean/docker package/docker
console_url=https://twistlock.tools.confluent-internal.io
make print-branch-info
localTag=$(make describe/docker)
#Twistlock scan
docker tag ${localTag}-amd64 dev-${localTag}-amd64
bash -c "twistcli images scan  --address \"$console_url\" \
      --details --output-file scan.json \"dev-${localTag}-amd64\""
docker tag ${localTag}-arm64 dev-${localTag}-arm64
bash -c "twistcli images scan  --address \"$console_url\" \
      --details --output-file scan.json \"dev-${localTag}-arm64\""

#Deploy: docker image
if [ "${SEMAPHORE_GIT_REF_TYPE}" != "pull-request" ]; then
  if [ "${NANO_VERSION}" == "true" ] && [ ! "${IS_RELEASE_JOB}" == "true" ] && { [ -z "${IS_CC_JOB}" ] || [ "${IS_CC_JOB}" == "false" ]; }; then
    bash -c ". ci-tools ci-push-tag"
  fi
  make deploy/docker sign/docker
fi
