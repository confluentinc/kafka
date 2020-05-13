#!/usr/bin/python

import os
import logging
import re
import subprocess
import sys


logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)


class CI:

    def __init__(self, new_version, repo_path):
        """Initialize class variables"""
        # List of all the files that were modified by this script so the parent
        # script that runs this update can commit them.
        self.updated_files = []
        # The new version
        self.new_version = new_version
        # The path the root of the repo so we can use full absolute paths
        self.repo_path = repo_path

    def run_update(self):
        """Update all the files with the new version"""
        log.info("Running additional version updates for kafka")
        self.update_kafkatest()
        self.update_quickstart()
        log.info("Finished all kafka additional version updates.")

    def update_kafkatest(self):
        """Update kafka test python scripts"""
        log.info("Updating kafkatest init script.")
        init_file = os.path.join(self.repo_path, "tests/kafkatest/__init__.py")
        dev_version = "{}.dev0".format(self.new_version.split("-ccs")[0])
        self.replace(init_file, "__version__", "__version__ = '{}'".format(dev_version))
        self.updated_files.append(init_file)
        log.info("Updating ducktape version.py")
        ducktape_version_file = os.path.join(self.repo_path, "tests/kafkatest/version.py")
        ducktape_version = self.new_version.split("-ccs")[0]
        self.regexReplace(ducktape_version_file,
                          "^DEV_VERSION = KafkaVersion.*",
                          "DEV_VERSION = KafkaVersion(\"{}\")".format(ducktape_version))
        self.updated_files.append(ducktape_version_file)

    def update_quickstart(self):
        """Uodate the streams quick start pom files."""
        log.info("Updating streams quickstart pom files.")
        quickstart_pom = os.path.join(self.repo_path, "streams/quickstart/pom.xml")
        self.update_project_versions(quickstart_pom, self.new_version)
        self.updated_files.append(quickstart_pom)
        # Do not need to explicitly update this file because the above command updates all pom files in the project.
        # Just need to add it to the list of modified files.
        quickstart_java_pom = os.path.join(self.repo_path, "streams/quickstart/java/pom.xml")
        self.updated_files.append(quickstart_java_pom)
        # The maven plugin has fails to process this pom file because it is an archetype style, so have to use regex.
        log.info("Updating streams quickstart archetype pom")
        archetype_resources_pom = os.path.join(self.repo_path,
                                               "streams/quickstart/java/src/main/resources/archetype-resources/pom.xml")
        self.regexReplace(archetype_resources_pom,
                          "<kafka\.version>.*</kafka\.version>",
                          "<kafka.version>{}</kafka.version>".format(self.new_version))
        self.updated_files.append(archetype_resources_pom)

    def replace(self, path, pattern, replacement):
        updated = []
        with open(path, 'r') as f:
            for line in f:
                updated.append((replacement + '\n') if line.startswith(pattern) else line)

        with open(path, 'w') as f:
            for line in updated:
                f.write(line)

    def regexReplace(self, path, pattern, replacement):
        updated = []
        with open(path, 'r') as f:
            for line in f:
                updated.append(re.sub(pattern, replacement, line))

        with open(path, 'w') as f:
            for line in updated:
                f.write(line)

    def run_cmd(self, cmd):
        """Execute a shell command. Return true if successful, false otherwise."""
        proc = subprocess.Popen(cmd,
                                cwd=self.repo_path,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True)
        stdout = proc.communicate()

        if stdout:
            log.info(stdout)

        if proc.returncode != 0:
            return False
        else:
            return True

    def update_project_versions(self, pom_file, new_version):
        """Set the project version in the pom files to the new project version."""
        cmd = "mvn --batch-mode versions:set -DnewVersion={} ".format(new_version)
        cmd += "-DallowSnapshots=false "
        cmd += "-DgenerateBackupPoms=false "
        cmd += "-DprocessAllModules=true "
        cmd += "-DprocessDependencies=false "
        cmd += "-DprocessPlugins=false "
        cmd += "-f {}".format(pom_file)
        log.info("Updating pom files with new project version.")

        if self.run_cmd(cmd):
            log.info("Finished updating the pom files with new project version.")
        else:
            log.error("Failed to set the new version in the pom files.")
            sys.exit(1)
