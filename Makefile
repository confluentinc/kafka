#!/usr/bin/make -f

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Below targets are used during kafka packaging for debian.

.SECONDEXPANSION:

SHELL := /bin/bash -e

DEPENDENTS := kafka-rest confluent-cloud-plugins
ALL := kafka kafka-rest kafka-http-server $(DEPENDENTS)

BRANCH ?= $$(git rev-parse --abbrev-ref HEAD)
SRC := $(CURDIR)/.src

GITHUB_REPO := confluentinc

QEMU_INIT_SCRIPT=$(CURDIR)/scripts/init-docker-builder.sh
DOCKER_BUILDER = buildx-builder

# Set to default registry/repo for development/cpd. Override for other envs.
DOCKER_IMAGE_REGISTRY ?= 519856050701.dkr.ecr.us-west-2.amazonaws.com
DOCKER_IMAGE_REPO ?= docker/dev/confluentinc
DOCKER_IMAGE_NAME := kafka
DOCKER_IMAGE_VERSION ?= $(subst v0,v1,$(subst master,latest,$(BRANCH)))
DOCKER_IMAGE_TAG := $(DOCKER_IMAGE_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
DOCKER_UPSTREAM_IMAGE_TAG ?= $(DOCKER_IMAGE_REGISTRY)/$(DOCKER_IMAGE_TAG)

TARGET_CHECKOUT := $(addprefix checkout/, $(ALL))
TARGET_TRACK := $(addprefix track/, $(ALL))
TARGET_TEST := $(addprefix test/, $(ALL))
TARGET_CLEAN :=  $(addprefix clean/, $(ALL))
TARGET_CLONE :=  $(addprefix clone/, $(ALL))
TARGET_COMPILE := $(addprefix compile/, $(ALL))
TARGET_DEPENDENT := $(addsuffix /target,$(addprefix $(SRC)/,$(DEPENDENTS)))
TARGET_DESCRIBE :=  $(addprefix describe/, $(ALL))
TARGET_DOCKER := $(SRC)/.docker
TARGET_KAFKA := $(SRC)/kafka/core/build/distributions
TARGET_KAFKA_HTTP := $(addsuffix /target,$(addprefix $(SRC)/,kafka-rest kafka-http-server))
TARGET_PULL := $(addprefix pull/, $(ALL))
TARGET_SRC := $(addprefix $(SRC)/,$(ALL))

IMAGE_SIGNING_URL ?= 'https://imagesigning.prod.cire.aws.internal.confluent.cloud/v1/oidc/sign' 

#### Macro Options ####
SKIP_TESTS ?= true
ifeq ($(SKIP_TESTS),true)
	MVN_OPTS += -DskipTests=true -Ddependency.check.skip=true
	GRADLE_OPTS += -x test
else
	MVN_OPTS += -DKAFKA_REST_CLOUD_SMOKE_TESTS=true
endif
# Populate $1 with additional maven options in compile recipes
MVN_OPTS += $(1)

CREATE_BRANCH_IF_NOT_FOUND ?= false
ifeq ($(CREATE_BRANCH_IF_NOT_FOUND),true)
	CHECKOUT = git fetch --all >/dev/null; git checkout $(BRANCH) >/dev/null || git checkout -b $(BRANCH);  > /dev/null
else
	CHECKOUT = git fetch --all >/dev/null; git checkout $(BRANCH) &> /dev/null
endif

#### MACROS ####
# Project build command
MVN = mvn install $(MVN_OPTS) -Pjenkins -Dmaven.test.failure.ignore=true
GRADLE = ./gradlew install releaseTarGz $(GRADLE_OPTS) -PpackageMetricsReporter=true -PscalaOptimizerMode=inline-scala -x javadoc -x scaladoc

# Invoke appropriate build tool for project
COMPILE = rm -f $(TARGET_DOCKER); test -f "pom.xml" && $(MVN) || $(GRADLE)
CLEAN = test -f "pom.xml" && mvn clean || ./gradlew clean

# Silence common commands
PUSHD = pushd $(1) > /dev/null
POPD = popd > /dev/null

#### RECIPES ####
# Clones github recipe into $(SRC) creating a new clone target.
$(TARGET_SRC):
	target=$(subst $(SRC)/,,$@); \
	git clone git@github.com:$(GITHUB_REPO)/$$target $(@); \
	$(PUSHD) $(@); \
		git fetch --all;\
	$(POPD);\
	$(MAKE) checkout/$$target

# User-friendly clone target; example: make clone/kafka
$(TARGET_CLONE): $$(subst clone,$$(SRC),$$@)

# Pseudo-target to invoke all clone recipes.
clone: $(TARGET_SRC)

# Checks out git branch $(BRANCH) for clone target; creates clone target if needed. example: make checkout/kafka
# Set CREATE_BRANCH_IF_NOT_FOUND=true to create the branch when it doesn't exist.
$(TARGET_CHECKOUT): $$(subst checkout, $$(SRC), $$@)
	@ echo "Running ${CHECKOUT}"
	target=$(subst checkout/,,$@); \
	$(MAKE) -s clean/$$target > /dev/null; \
	$(PUSHD) $(SRC)/$$target; \
		$(CHECKOUT); \
    $(POPD);\

# Sets upstream tracking for current state of cloned repository
$(TARGET_TRACK): $$(subst track, $$(SRC), $$@)
	target=$(subst track/,,$@); \
	$(PUSHD) $(SRC)/$$target; \
		git push --set-upstream origin $$(git branch --show-current);\
    $(POPD);\
	$(MAKE) -s describe/$$target

# Sets upstream tracking for current state of cloned repository
$(TARGET_TEST): $$(subst test, $$(SRC), $$@)
	target=$(subst test/,,$@); \
	$(PUSHD) $(SRC)/$$target; \
		if [ -f "pom.xml" ]; then \
			mvn -Dmaven.test.failure.ignore=true test; \
		else \
			./gradlew unitTest integrationTest \
				--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed \
				-PmaxParallelForks=4 -PignoreFailures=true;\
		fi; \
    $(POPD); \

# Pulls latest revisions on $(BRANCH) for clone target;
# Recompiles code to avoid packaging stale artifacts.
$(TARGET_PULL): $$(subst pull, $$(SRC), $$@)
	target=$(subst pull/,,$@); \
	version=$$($(MAKE) -s kafka-cp-version); \
	$(PUSHD) $(SRC)/$$target; \
		$(CHECKOUT); \
		$(call COMPILE, $${version}); \
    $(POPD);\
	$(MAKE) -s describe/$$target

# Pseudo-target to invoke all checkout recipes.
# Set CREATE_BRANCH_IF_NOT_FOUND=true to create the branch when it doesn't exist.
checkout:
	for target in $$($(MAKE) -s show-projects); do \
		$(MAKE) checkout/$$target; \
    done

# Displays TAG "nearest" HEAD; example: make describe/kafka
# see https://git-scm.com/docs/git-describe#_search_strategy for an explanation of nearest.
$(TARGET_DESCRIBE): $$(subst describe,$$(SRC),$$@)
	@ target=$(subst describe/,,$@); \
	cd $(SRC)/$$target; \
	echo "$$target=$$(git describe --tags --always --dirty --match 'v[0-9]*.[0-9]*.[0-9]*')"

# Pseudo-target to invoke all known describe recipes; does not create targets.
describe:
	@ test -d $(SRC) && \
		for target in $$(ls -d $(SRC)/*); do \
				$(MAKE) -s describe/"$${target#$(SRC)/}"; \
		done \
	|| echo "make: Nothing to be done for $@."

# Pseudo-target to print local image tag.
describe/docker:
	@ echo $(DOCKER_IMAGE_TAG)

kafka-cp-version:
	@version=$$($(MAKE) -s describe/kafka); \
		echo "-Dce.kafka.version=$$(echo $${version} | sed 's/.*=v[^-]*-//' | sed 's/ce-.*/ce/')"

# Cleans compile target; example: make clean/kafka.
# Note: Source code remains intact
$(TARGET_CLEAN):
	export target=$(subst clean,$(SRC),$@); \
	if [[ -d "$$target" ]]; then \
		cd $(subst clean,$(SRC),$@); $(CLEAN); \
	else \
		echo "make: Nothing to be done for $@."; \
	fi

# Pseudo-target clean target for docker image.
clean/docker :
	 rm -f $(TARGET_DOCKER) && docker rmi -f $$($(MAKE) -s describe/docker)

clean:
	#mvn dependency:purge-local-repository
	$(MAKE) -s clean/docker
	rm -rf $(SRC)

# Invokes all compile recipes.
$(TARGET_COMPILE):
	export target=$(subst compile/,,$@); \
	if [[ "$$target" = "kafka" ]]; then \
		$(MAKE) $(TARGET_KAFKA); \
	else \
		$(MAKE) $(SRC)/$$target/target; \
	fi

# Creates kafka compile target.
# Ignores update to source code to avoid spurious rebuilds.
$(TARGET_KAFKA): | $(SRC)/kafka
	cd $(SRC)/kafka && $(COMPILE)

# Creates second-tier compile targets.
# Ignores update to source code to avoid spurious rebuilds.
# Pins to locally built kafka
$(TARGET_KAFKA_HTTP): $(TARGET_KAFKA) | $(SRC)/kafka-rest $(SRC)/kafka-http-server
	version=$$($(MAKE) -s kafka-cp-version); \
		echo "Building against kafka version $${version}"; \
		cd $(subst /target,,$@); \
		$(call COMPILE, $${version})

# Creates bottom-tier compile targets
# Ignores update to source code to avoid spurious rebuilds.
# Pins to locally built kafka
$(TARGET_DEPENDENT): $(TARGET_KAFKA) $(TARGET_KAFKA_HTTP) | $(TARGET_SRC)
	version=$$($(MAKE) -s kafka-cp-version); \
		echo "Building against kafka version $${version}"; \
		cd $(subst /target,,$@); \
		$(call COMPILE, $${version})

# Pseudo compile target to invoke all compile recipes.
compile: $(TARGET_DEPENDENT)

# Create docker package target.
# Adds an empty file to avoid spurious rebuilds.
$(TARGET_DOCKER): install-qemu-static $(TARGET_DEPENDENT)
	CC_RELEASE="$$(cd $(SRC)/kafka && git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*')"; \
	for version in $$($(MAKE) -s describe); do label_args+=(--label "$$version"); done; \
	cd $(CURDIR); \
	docker buildx build \
		--platform linux/arm64 \
		--build-arg PREFIX=$(subst $(CURDIR),,$(SRC)) \
		--build-arg RELEASE=$${CC_RELEASE#*=} \
		--load \
		$${label_args[@]} \
		-t "$$($(MAKE) -s describe/docker)-arm64" . ; \
	docker buildx build \
		--platform linux/amd64 \
		--build-arg PREFIX=$(subst $(CURDIR),,$(SRC)) \
		--build-arg RELEASE=$${CC_RELEASE#*=} \
		--load \
		$${label_args[@]} \
		-t "$$($(MAKE) -s describe/docker)-amd64" .
	touch $(TARGET_DOCKER)

install-qemu-static:
	$(QEMU_INIT_SCRIPT) $(DOCKER_BUILDER)

show-projects:
	@echo $(ALL)

#  https://www.cmcrossroads.com/article/dumping-every-makefile-variable
debug:
	$(foreach V,$(sort $(.VARIABLES)),\
	  $(if $(filter-out environment% default automatic, \
	  	$(origin $V)),$(warning $V=$($V) ($(value $V)))))

show-branch:
	test -d $(SRC) && \
		for target in $$(ls -d $(SRC)/*); do \
			$(PUSHD) $$target; \
				echo "$${target#$(SRC)/}:$$(git rev-parse --abbrev-ref HEAD)"; \
			$(POPD); \
		done \
	|| echo "make: Nothing to be done for $@."

package/docker: | $(TARGET_DOCKER)

clean/test: test/shared/results
	rm -rf test/shared/results/*

test: package/docker
	 RESULTS_DIR=/test/shared/results/$$(date +%s); \
	 BRANCH=$(BRANCH) docker-compose -f ./test/docker-compose.yml up -d --build; \
	 echo "Test results can be found in $${RESULTS_DIR}"

# tag and push the arch-specific images
# then create the manifest which contains a reference to each arch-specific image
deploy/docker: $(TARGET_DOCKER)
	# `docker manifest create` doesn't replace an existing manifest the way tagging an image does, so try and delete any existing manifest if it exists locally
	IMAGE_TAG=$$($(MAKE) -s describe/docker); \
	echo "upstream image tag ${DOCKER_UPSTREAM_IMAGE_TAG}"; \
	docker tag $${IMAGE_TAG}-arm64 ${DOCKER_UPSTREAM_IMAGE_TAG}-arm64; \
	docker push ${DOCKER_UPSTREAM_IMAGE_TAG}-arm64; \
	echo "pushed ${DOCKER_UPSTREAM_IMAGE_TAG}-arm64"; \
	docker tag $${IMAGE_TAG}-amd64 ${DOCKER_UPSTREAM_IMAGE_TAG}-amd64; \
	docker push ${DOCKER_UPSTREAM_IMAGE_TAG}-amd64; \
	echo "pushed ${DOCKER_UPSTREAM_IMAGE_TAG}-amd64"; \
	docker manifest rm ${DOCKER_UPSTREAM_IMAGE_TAG} 2&> /dev/null || true; \
	docker manifest create ${DOCKER_UPSTREAM_IMAGE_TAG} ${DOCKER_UPSTREAM_IMAGE_TAG}-arm64 ${DOCKER_UPSTREAM_IMAGE_TAG}-amd64; \
	echo "pushed ${DOCKER_UPSTREAM_IMAGE_TAG}"; \
	docker manifest push ${DOCKER_UPSTREAM_IMAGE_TAG}

.PHONY: sign/docker
sign/docker:
	$(eval IMAGE_DIGEST_ARM := $(shell docker inspect --format="{{index .RepoDigests 0}}" "${DOCKER_UPSTREAM_IMAGE_TAG}-arm64" | cut -d'@' -f2))
	$(eval IMAGE_DIGEST_AMD := $(shell docker inspect --format="{{index .RepoDigests 0}}" "${DOCKER_UPSTREAM_IMAGE_TAG}-amd64" | cut -d'@' -f2))
	@curl -X POST \
		-w "%{http_code}\n" \
		$(IMAGE_SIGNING_URL) \
		-H "Authorization: Bearer ${OIDC_TOKEN}" \
		-H "Content-Type: application/json" \
		-d '{"images": [{"uri": "$(DOCKER_IMAGE_REGISTRY)/$(DOCKER_IMAGE_REPO)/$(DOCKER_IMAGE_NAME)@$(IMAGE_DIGEST_AMD)"}]}' \
		| cat | sed '/^2/q ; /^\([0,1,3,4,5,6,7,8,9]\)/{s//Error, please link this log in slack channel image-signing-service-help\n\1/ ; q1}'
	@curl -X POST \
		-w "%{http_code}\n" \
		$(IMAGE_SIGNING_URL) \
		-H "Authorization: Bearer ${OIDC_TOKEN}" \
		-H "Content-Type: application/json" \
		-d '{"images": [{"uri": "$(DOCKER_IMAGE_REGISTRY)/$(DOCKER_IMAGE_REPO)/$(DOCKER_IMAGE_NAME)@$(IMAGE_DIGEST_ARM)"}]}' \
		| cat | sed '/^2/q ; /^\([0,1,3,4,5,6,7,8,9]\)/{s//Error, please link this log in slack channel image-signing-service-help\n\1/ ; q1}'
	@curl -X POST \
		-w "%{http_code}\n" \
		$(IMAGE_SIGNING_URL) \
		-H "Authorization: Bearer ${OIDC_TOKEN}" \
		-H "Content-Type: application/json" \
		-d '{"images": [{"uri": "$(DOCKER_IMAGE_REGISTRY)/$(DOCKER_IMAGE_REPO)/$(DOCKER_IMAGE_NAME)@$(shell docker manifest push $(DOCKER_UPSTREAM_IMAGE_TAG))"}]}' \
		| cat | sed '/^2/q ; /^\([0,1,3,4,5,6,7,8,9]\)/{s//Error, please link this log in slack channel image-signing-service-help\n\1/ ; q1}'

.PHONY: print-branch-info
print-branch-info:
	@echo "Branch variable: $(BRANCH)"
	@echo "Current git branch: $$(git rev-parse --abbrev-ref HEAD)"
	@echo "Docker image version: $(DOCKER_IMAGE_VERSION)"
	@echo "Docker image tag: $(DOCKER_IMAGE_TAG)"
	@echo "Modified branch name: $(subst v0,v1,$(subst master,latest,$(BRANCH)))"


.PHONY: clean
clean:

.PHONY: distclean
distclean:

