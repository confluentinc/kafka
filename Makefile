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

### BEGIN MK-INCLUDE UPDATE ###
CURL ?= curl
FIND ?= find
TAR ?= tar

# Mount netrc so curl can work from inside a container
DOCKER_NETRC_MOUNT ?= 1

GITHUB_API = api.github.com
GITHUB_MK_INCLUDE_OWNER := confluentinc
GITHUB_MK_INCLUDE_REPO := cc-mk-include
GITHUB_API_CC_MK_INCLUDE := https://$(GITHUB_API)/repos/$(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO)
GITHUB_API_CC_MK_INCLUDE_TARBALL := $(GITHUB_API_CC_MK_INCLUDE)/tarball
GITHUB_API_CC_MK_INCLUDE_VERSION ?= $(GITHUB_API_CC_MK_INCLUDE_TARBALL)/$(MK_INCLUDE_VERSION)

MK_INCLUDE_DIR := mk-include
MK_INCLUDE_LOCKFILE := .mk-include-lockfile
MK_INCLUDE_TIMESTAMP_FILE := .mk-include-timestamp
# For optimum performance, you should override MK_INCLUDE_TIMEOUT_MINS above the managed section headers to be
# a little longer than the worst case cold build time for this repo.
MK_INCLUDE_TIMEOUT_MINS ?= 240
# If this latest validated release is breaking you, please file a ticket with DevProd describing the issue, and
# if necessary you can temporarily override MK_INCLUDE_VERSION above the managed section headers until the bad
# release is yanked.
MK_INCLUDE_VERSION ?= v0.955.0

# Make sure we always have a copy of the latest cc-mk-include release less than $(MK_INCLUDE_TIMEOUT_MINS) old:
./$(MK_INCLUDE_DIR)/%.mk: .mk-include-check-FORCE
	@trap "rm -f $(MK_INCLUDE_LOCKFILE); exit" 0 2 3 15; \
	waitlock=0; while ! ( set -o noclobber; echo > $(MK_INCLUDE_LOCKFILE) ); do \
	   sleep $$waitlock; waitlock=`expr $$waitlock + 1`; \
	   test 14 -lt $$waitlock && { \
	      echo 'stealing stale lock after 105s' >&2; \
	      break; \
	   } \
	done; \
	test -s $(MK_INCLUDE_TIMESTAMP_FILE) || rm -f $(MK_INCLUDE_TIMESTAMP_FILE); \
	test -z "`$(FIND) $(MK_INCLUDE_TIMESTAMP_FILE) -mmin +$(MK_INCLUDE_TIMEOUT_MINS) 2>&1`" || { \
	   grep -q 'machine $(GITHUB_API)' ~/.netrc 2>/dev/null || { \
	      echo 'error: follow https://confluentinc.atlassian.net/l/cp/0WXXRLDh to fix your ~/.netrc'; \
	      exit 1; \
	   }; \
	   $(CURL) --fail --silent --netrc --location "$(GITHUB_API_CC_MK_INCLUDE_VERSION)" --output $(MK_INCLUDE_TIMESTAMP_FILE)T --write-out '$(GITHUB_API_CC_MK_INCLUDE_VERSION): %{errormsg}\n' >&2 \
	   && $(TAR) zxf $(MK_INCLUDE_TIMESTAMP_FILE)T \
	   && rm -rf $(MK_INCLUDE_DIR) \
	   && mv $(GITHUB_MK_INCLUDE_OWNER)-$(GITHUB_MK_INCLUDE_REPO)-* $(MK_INCLUDE_DIR) \
	   && mv -f $(MK_INCLUDE_TIMESTAMP_FILE)T $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && echo 'installed cc-mk-include-$(MK_INCLUDE_VERSION) from $(GITHUB_MK_INCLUDE_REPO)' \
	   ; \
	} || { \
	   rm -f $(MK_INCLUDE_TIMESTAMP_FILE)T; \
	   if test -f $(MK_INCLUDE_TIMESTAMP_FILE); then \
	      touch $(MK_INCLUDE_TIMESTAMP_FILE); \
	      echo 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to check for latest release; next try in $(MK_INCLUDE_TIMEOUT_MINS) minutes' >&2; \
	   else \
	      echo 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to bootstrap mk-include subdirectory' >&2 && false; \
	   fi; \
	}

.PHONY: .mk-include-check-FORCE
.mk-include-check-FORCE:
	@test -z "`git ls-files $(MK_INCLUDE_DIR)`" || { \
		echo 'fatal: checked in $(MK_INCLUDE_DIR)/ directory is preventing make from fetching recent cc-mk-include releases for CI' >&2; \
		exit 1; \
	}
### END MK-INCLUDE UPDATE ###
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

IMAGE_NAME := ce-kafka
MASTER_BRANCH := master
KAFKA_VERSION := $(shell awk 'sub(/.*version=/,""){print $1}' ./gradle.properties)
VERSION_POST := -$(KAFKA_VERSION)
DOCKER_BUILD_PRE  += copy-gradle-properties
DOCKER_BUILD_POST += clean-gradle-properties
UPDATE_MK_INCLUDE := false
UPDATE_MK_INCLUDE_AUTO_MERGE := false

DOCKER_BUILD_MULTIARCH := true
# enable intermediate build stage unless on CI
# since we pass --no-cache in CI which prevents
# use of intermediate docker build stage
ifneq ($(CI),true)
DOCKER_SHARED_TARGET := kafka-builder
endif

RELEASE_POSTCOMMIT += push-docker sox-log-docker-sha-ce-kafka
VERSION_REFS := 'v0.*'

ifeq ($(CONFLUENT_PLATFORM_PACKAGING),)
include ./mk-include/cc-begin.mk
include ./mk-include/cc-testbreak.mk
include ./mk-include/cc-vault.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk
else
.PHONY: clean
clean:

.PHONY: distclean
distclean:

%:
	$(MAKE) -f debian/Makefile $@
endif

# Custom docker targets
.PHONY: show-docker-all
show-docker-all:
	@echo
	@echo ========================
	@echo "Docker info for ce-kafka:"
	@make VERSION=$(VERSION) show-docker
	@echo
	@echo ========================
	@echo "Docker info for cc-zookeeper"
	@make VERSION=$(VERSION) -C cc-zookeeper show-docker
	@echo
	@echo ========================
	@echo "Docker info for soak_cluster"
	@make VERSION=$(VERSION) -C cc-services/soak_cluster show-docker
	@echo
	@echo ========================
	@echo "Docker info for trogdor"
	@make VERSION=$(VERSION) -C cc-services/trogdor show-docker
	@echo
	@echo ========================
	@echo "Docker info for tier-validator-services"
	@make VERSION=$(VERSION) -C cc-services/tier_validator show-docker

.PHONY: build-docker-cc-services
build-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster build-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor build-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/tier_validator build-docker

.PHONY: push-docker-cc-services
push-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster push-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor push-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/tier_validator push-docker

.PHONY: build-docker-cc-zookeeper
build-docker-cc-zookeeper:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-zookeeper build-docker

.PHONY: push-docker-cc-zookeeper
push-docker-cc-zookeeper:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-zookeeper push-docker sign-image

GRADLE_TEMP = ./tmp/gradle/
.PHONY: copy-gradle-properties
copy-gradle-properties:
	mkdir -p $(GRADLE_TEMP)
	cp ~/.gradle/gradle.properties $(GRADLE_TEMP)

.PHONY: clean-gradle-properties
clean-gradle-properties:
	rm -rf $(GRADLE_TEMP)

# alias to prevent breaking developer and automated workflows
# we previously used build-docker-developer to generate S3 credentials
# required for apt to fetch packages
build-docker-developer: build-docker

.PHONY: push-docker-pr
ifeq ($(CI),true)
ifneq (,$(findstring pr-image, $(SEMAPHORE_GIT_PR_NAME)))
push-docker-pr: push-docker-version
endif
endif

# Log for the master image. Requires images to be pushed before this step.
.PHONY: sox-log-docker-sha-ce-kafka
sox-log-docker-sha-ce-kafka:
ifeq ($(CI),true)
ifneq ($(RELEASE_BRANCH),$(_empty))
	pip3 install confluent-ci-tools
	# amd64 image
	$(eval IMAGE_SHA := $(shell docker inspect --format="{{index .RepoDigests 0}}" "$(DOCKER_REPO)/confluentinc/ce-kafka:$(BUMPED_VERSION)-amd64"))
	@echo "Reporting docker image information event for $(DOCKER_REPO)/confluentinc/ce-kafka:$(BUMPED_VERSION)-amd64, image sha: $(IMAGE_SHA)"
	# pushing the $(BUMPED_VERSION)-amd64 version as canonical version since that's what we're running in prod until arm64 lands
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_VERSION) --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_VERSION)-amd64 --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
	# arm64 image
	$(eval IMAGE_SHA := $(shell docker inspect --format="{{index .RepoDigests 0}}" "$(DOCKER_REPO)/confluentinc/ce-kafka:$(BUMPED_VERSION)-arm64"))
	@echo "Reporting docker image information event for $(DOCKER_REPO)/confluentinc/ce-kafka:$(BUMPED_VERSION)-arm64, image sha: $(IMAGE_SHA)"
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_VERSION)-arm64 --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
endif
endif

# Log for cc-zookeeper image. Requires images to be pushed before this step.
.PHONY: sox-log-docker-sha-cc-zookeeper
sox-log-docker-sha-cc-zookeeper:
ifeq ($(CI),true)
ifneq ($(RELEASE_BRANCH),$(_empty))
	pip3 install confluent-ci-tools
	$(eval IMAGE_SHA := $(shell docker inspect --format="{{index .RepoDigests 0}}" "$(DOCKER_REPO)/confluentinc/cc-zookeeper:$(BUMPED_BASE_VERSION)-amd64"))
	@echo "Reporting docker image information event for $(DOCKER_REPO)/confluentinc/cc-zookeeper:$(BUMPED_BASE_VERSION)-amd64, image sha: $(IMAGE_SHA)"
	# pushing the $(BUMPED_VERSION)-amd64 version as canonical version since that's what we're running in prod until arm64 lands
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_BASE_VERSION) --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_BASE_VERSION)-amd64 --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
	# arm64
	$(eval IMAGE_SHA := $(shell docker inspect --format="{{index .RepoDigests 0}}" "$(DOCKER_REPO)/confluentinc/cc-zookeeper:$(BUMPED_BASE_VERSION)-arm64"))
	@echo "Reporting docker image information event for $(DOCKER_REPO)/confluentinc/cc-zookeeper:$(BUMPED_BASE_VERSION)-arm64, image sha: $(IMAGE_SHA)"
	# pushing the $(BUMPED_VERSION)-arm64 version as latest since that's what we're running in prod until arm64 lands
	ci-docker-image-semaphore-event --topic 'sox-sdlc-audit-automation' --version-tag $(BUMPED_BASE_VERSION)-arm64 --sha256 $(IMAGE_SHA) --config-file $(HOME)/.sox-semaphore-build-info.ini
endif
endif

#Runs the compile and checkstyle error check
.PHONY: compile-validate
compile-validate:
	./retry_zinc ./gradlew --build-cache clean assemble publishToMavenLocal check -x test -PkeepAliveMode=session --stacktrace 2>&1 | tee build.log
	@error_count=$$(grep -c -E "(ERROR|error:|\[Error\])" build.log); \
	echo "Compile, checkstyle or spotbugs error found"; \
	grep -E "(ERROR|error:|\[Error\])" build.log | while read -r line; do \
		echo "$$line"; \
	done; \
	echo "Number of compile, checkstyle and spotbug errors: $$error_count"; \
	exit $$error_count


#Check compilation compatibility with Scala 2.12
.PHONY: check-scala-compatibility
check-scala-compatibility:
	./retry_zinc ./gradlew clean assemble -PscalaVersion=2.12 -PkeepAliveMode=session --stacktrace 2>&1 | tee build.log
	@error_count=$$(grep -c -E "(ERROR|error:|\[Error\])" build.log); \
  	grep -E "(ERROR|error:|\[Error\])" build.log | while read -r line; do \
		echo "$$line"; \
	done; \
	echo "Number of compile, checkstyle and spotbug errors: $$error_count"; \
	exit $$error_count


###Test task
.PHONY: test-task
test-task:
	$(GIT_ROOT)/scripts/run_all_tests.sh $(partition) $(node) $(NO_OF_TEST_WORKER_NODES) $(MAX_TEST_RETRY_FAILURES_PER_NODE) $(is_semaphore_run) $(destination) "$(GIT_ROOT)" "$(GIT_ROOT)/build" "$(GIT_ROOT)/scripts" $(timeout)

.PHONY: testbreak-after-kafka
testbreak-after-kafka:
	@PREV_BRANCH_NAME=$(BRANCH_NAME); \
	export BRANCH_NAME=$$PREV_BRANCH_NAME-${ARCH}; \
	echo "Current BRANCH_NAME is set to: $$BRANCH_NAME"; \
	TESTBREAK_REPORTING_BRANCHES_WITH_ARCH=$$(echo $(TESTBREAK_REPORTING_BRANCHES) | sed "s/\b\w*\b/&-${ARCH}/g"); \
	if [ "$(TESTBREAK_SKIP)" = "true" ]; then \
		echo "Skipping testbreak after, TESTBREAK_SKIP is set to true."; \
	elif echo "$$TESTBREAK_REPORTING_BRANCHES_WITH_ARCH" | grep -q -w "$$BRANCH_NAME"; then \
		echo "Reporting to testbreak at url: https://testbreak.confluent.io/kiosk/branch/$$BRANCH_NAME/job_result/$(SEMAPHORE_PROJECT_NAME), branch \"$$BRANCH_NAME\" is whitelisted."; \
		ci-kafka-event --start-time $(START_TIME) --duration $(DURATION_IN_MILLIS) --build-log build.log --test-results "$(BUILD_DIR)/**/*TEST*xml" --s3;  # report to testbreak \
	else \
		echo "Not reporting to testbreak, branch \"$$BRANCH_NAME\" not whitelisted in [$$TESTBREAK_REPORTING_BRANCHES_WITH_ARCH]."; \
	fi; \
	export BRANCH_NAME=$$PREV_BRANCH_NAME




# Enable Thread Leak Listener
.PHONY: enable-thread-leak-listener
enable-thread-leak-listener:
	echo "org.apache.kafka.test.ThreadLeakListener" > clients/src/test/resources/META-INF/services/org.junit.platform.launcher.TestExecutionListener
