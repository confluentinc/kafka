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

#Runs the compile and checkstyle error check
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
MK_INCLUDE_VERSION ?= v0.1365.0

# Make sure we always have a copy of the latest cc-mk-include release less than $(MK_INCLUDE_TIMEOUT_MINS) old:
# Note: The simply-expanded make variable makes sure this is run once per make invocation.
UPDATE_MK_INCLUDE := $(shell \
	func_fatal() { echo "$$*" >&2; echo output here triggers error below; exit 1; } ; \
	test -z "`git ls-files $(MK_INCLUDE_DIR)`" || { \
		func_fatal 'fatal: checked in $(MK_INCLUDE_DIR)/ directory is preventing make from fetching recent cc-mk-include releases for CI'; \
	} ; \
	trap "rm -f $(MK_INCLUDE_LOCKFILE); exit" 0 2 3 15; \
	waitlock=0; while ! ( set -o noclobber; echo > $(MK_INCLUDE_LOCKFILE) ); do \
	   sleep $$waitlock; waitlock=`expr $$waitlock + 1`; \
	   test 14 -lt $$waitlock && { \
	      echo 'stealing stale lock after 105s' >&2; \
	      break; \
	   } \
	done; \
	test -s $(MK_INCLUDE_TIMESTAMP_FILE) || rm -f $(MK_INCLUDE_TIMESTAMP_FILE); \
	{ test -d $(MK_INCLUDE_DIR) && test -d /proc && test -z "$(cat /proc/1/sched 2>&1 |head -n 1 |grep init)"; } || \
	test -z "`$(FIND) $(MK_INCLUDE_TIMESTAMP_FILE) -mmin +$(MK_INCLUDE_TIMEOUT_MINS) 2>&1`" || { \
	   GHAUTH=$$(grep -sq 'machine $(GITHUB_API)' ~/.netrc && echo netrc || \
	     ( command -v gh > /dev/null && gh auth status -h github.com > /dev/null && echo gh )); \
	   test -n "$$GHAUTH" || \
	     func_fatal 'error: no GitHub token available via "~/.netrc" or "gh auth status".\nFollow https://confluentinc.atlassian.net/l/cp/0WXXRLDh to setup GitHub authentication.\n'; \
	   echo "downloading $(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO) $(MK_INCLUDE_VERSION) using $$GHAUTH" >&2; \
	   if [ "netrc" = "$$GHAUTH" ]; then \
	     $(CURL) --fail --silent --netrc --location "$(GITHUB_API_CC_MK_INCLUDE_VERSION)" --output $(MK_INCLUDE_TIMESTAMP_FILE)T --write-out '$(GITHUB_API_CC_MK_INCLUDE_VERSION): %{errormsg}\n' >&2; \
	   else \
	     gh release download --clobber --repo=$(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO) \
	        --archive=tar.gz --output $(MK_INCLUDE_TIMESTAMP_FILE)T $(MK_INCLUDE_VERSION) >&2; \
	   fi \
	   && TMP_MK_INCLUDE_DIR=$$(mktemp -d -t cc-mk-include.XXXXXXXXXX) \
	   && $(TAR) -C $$TMP_MK_INCLUDE_DIR --strip-components=1 -zxf $(MK_INCLUDE_TIMESTAMP_FILE)T \
	   && rm -rf $$TMP_MK_INCLUDE_DIR/tests \
	   && rm -rf $(MK_INCLUDE_DIR) \
	   && mv $$TMP_MK_INCLUDE_DIR $(MK_INCLUDE_DIR) \
	   && mv -f $(MK_INCLUDE_TIMESTAMP_FILE)T $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && echo 'installed cc-mk-include $(MK_INCLUDE_VERSION) from $(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO)' >&2 \
	   || func_fatal unable to install cc-mk-include $(MK_INCLUDE_VERSION) from $(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO) \
	   ; \
	} || { \
	   rm -f $(MK_INCLUDE_TIMESTAMP_FILE)T; \
	   if test -f $(MK_INCLUDE_TIMESTAMP_FILE); then \
	      touch $(MK_INCLUDE_TIMESTAMP_FILE); \
	      func_fatal 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to check for latest release; next try in $(MK_INCLUDE_TIMEOUT_MINS) minutes'; \
	   else \
	      func_fatal 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to bootstrap mk-include subdirectory'; \
	   fi; \
	} \
)
ifneq ($(UPDATE_MK_INCLUDE),)
    $(error mk-include update failed)
endif

# Export the (empty) .mk-include-check-FORCE target to allow users to trigger the mk-include
# download code above via make but without having to run any of the other targets, e.g. build.
.PHONY: .mk-include-check-FORCE
.mk-include-check-FORCE:
	@echo -n ""
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

ifeq ($(CONFLUENT_PLATFORM_PACKAGING),)
include ./mk-include/cc-begin.mk
include ./mk-include/cc-testbreak.mk
include ./mk-include/cc-end.mk
else
.PHONY: clean
clean:

.PHONY: distclean
distclean:

%:
	$(MAKE) -f debian/Makefile $@
endif

.PHONY: compile-validate
compile-validate:
	./retry_zinc ./gradlew clean -PskipSigning=true publishToMavenLocal build -x test --no-daemon --stacktrace -PxmlSpotBugsReport=true 2>&1 | tee build.log
	@error_count=$$(grep -c -E "(ERROR|error:|\[Error\]|FAILED)" build.log); \
	if [ $$error_count -ne 0 ]; then \
		echo "Compile, checkstyle or spotbugs error found"; \
		grep -E "(ERROR|error:|\[Error\]|FAILED)" build.log | while read -r line; do \
			echo "$$line"; \
		done; \
		echo "Number of compile, checkstyle and spotbug errors: $$error_count"; \
		exit $$error_count; \
	else \
		echo "No errors found"; \
	fi

#Check compilation compatibility with Scala 2.12
.PHONY: check-scala-compatibility
check-scala-compatibility:
	./retry_zinc ./gradlew clean build -x test --no-daemon --stacktrace -PxmlSpotBugsReport=true -PscalaVersion=2.12 2>&1 | tee build.log
	@error_count=$$(grep -c -E "(ERROR|error:|\[Error\]|FAILED)" build.log); \
	if [ $$error_count -ne 0 ]; then \
		grep -E "(ERROR|error:|\[Error\]|FAILED)" build.log | while read -r line; do \
			echo "$$line"; \
		done; \
		echo "Number of compile, checkstyle and spotbug errors: $$error_count"; \
		exit $$error_count; \
	else \
		echo "No errors found"; \
	fi
# Below targets are used during kafka packaging for debian.

.PHONY: clean
clean:

.PHONY: distclean
distclean:

%:
	$(MAKE) -f debian/Makefile $@
