#!/bin/bash

DOCKER_BUILDER_INSTANCE=$1
shift

echo "Install arm64 emulation"
docker run --privileged --rm tonistiigi/binfmt --install arm64 > /dev/null

(docker buildx use "$DOCKER_BUILDER_INSTANCE" >& /dev/null \
  && echo "Using existing docker builder instance: $DOCKER_BUILDER_INSTANCE") \
  || (echo "Creating docker builder instance: $DOCKER_BUILDER_INSTANCE" \
      && docker buildx create --use --name "$DOCKER_BUILDER_INSTANCE")

# in some cases we have noticed it takes a few seconds for all supported
# architectures to show up in the builder instance inspect output
MAX=60;r=0
while [ $r -lt $MAX ] ; do
  docker buildx inspect --bootstrap "$DOCKER_BUILDER_INSTANCE" | grep linux/arm64 > /dev/null && break
  r=$(( r + 1 ))
  echo "Waiting for builder instance platforms to populate... [$r/$MAX]"
  sleep 1
done

[ $r -lt $MAX ] || (echo "Failed to detect linux/arm64 support in docker builder instance" && exit 1)
