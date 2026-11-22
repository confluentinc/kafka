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

import io
import os
import sys
import tempfile
import unittest
from xml.etree import ElementTree

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from check_test_failures import parse_failures_and_rerun_tests, read_suite_totals


def parse(xml):
    return parse_failures_and_rerun_tests(ElementTree.iterparse(io.StringIO(xml), events=("start", "end")))


def suite(*testcases):
    return '<testsuite name="org.example.SomeTest">' + "".join(testcases) + "</testsuite>"


def case(name, *children):
    return f'<testcase name="{name}" classname="org.example.SomeTest" time="1.0">{"".join(children)}</testcase>'


FAILURE = '<failure message="boom">trace</failure>'
RERUN_FAILURE = '<rerunFailure message="boom">trace</rerunFailure>'
FLAKY_FAILURE = '<flakyFailure message="boom">trace</flakyFailure>'


class ParseFailuresTest(unittest.TestCase):
    """Shapes written by Gradle with reports.junitXml.mergeReruns = true and one retry."""

    def test_all_pass(self):
        self.assertEqual((0, 0), parse(suite(case("a()"), case("b()"))))

    def test_failure_without_retry(self):
        self.assertEqual((1, 0), parse(suite(case("a()", FAILURE), case("b()"))))

    def test_passed_on_retry_is_not_a_failure(self):
        self.assertEqual((0, 0), parse(suite(case("a()", FLAKY_FAILURE))))

    def test_failed_on_retry_too_is_a_failure(self):
        self.assertEqual((0, 1), parse(suite(case("a()", FAILURE, RERUN_FAILURE))))

    def test_later_pass_with_same_name_does_not_cancel_a_failure(self):
        self.assertEqual((1, 0), parse(suite(case("[1] x", FAILURE), case("[1] x"))))


class ReadSuiteTotalsTest(unittest.TestCase):
    def totals(self, xml):
        with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False) as f:
            f.write(xml)
        self.addCleanup(os.remove, f.name)
        return read_suite_totals(f.name)

    def test_totals_come_from_testsuite_attributes(self):
        xml = '<testsuite name="org.example.SomeTest" tests="5" failures="1" errors="0" skipped="2"></testsuite>'
        self.assertEqual((5, 1, 2), self.totals(xml))

    def test_output_over_ten_megabytes_is_readable(self):
        # lxml (used by junitparser) refuses CDATA sections over 10 MB.
        output = "x" * (11 * 1024 * 1024)
        xml = ('<testsuite name="org.example.SomeTest" tests="1" failures="0" errors="0" skipped="0">'
               + case("a()") + f"<system-out><![CDATA[{output}]]></system-out></testsuite>")
        self.assertEqual((1, 0, 0), self.totals(xml))


if __name__ == "__main__":
    unittest.main()
