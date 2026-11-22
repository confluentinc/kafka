import io
import os
import sys
import types
import unittest
from xml.etree import ElementTree

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if "junitparser" not in sys.modules:
    try:
        import junitparser  # noqa: F401
    except ImportError:
        sys.modules["junitparser"] = types.SimpleNamespace(JUnitXml=None)
from check_test_failures import parse_failures_and_rerun_tests


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

    def test_same_display_name_twice_counts_each_failure(self):
        self.assertEqual((1, 0), parse(suite(case("[1] x", FAILURE), case("[1] x"))))


if __name__ == "__main__":
    unittest.main()
