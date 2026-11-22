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
    return '<testsuite name="kafka.zk.ZkMigrationFailoverTest">' + "".join(testcases) + "</testsuite>"


def case(name, failed=False, rerun_failed=False, classname="kafka.zk.ZkMigrationFailoverTest"):
    body = ('<failure message="boom">trace</failure>' if failed else "") + \
           ('<rerunFailure message="boom">trace</rerunFailure>' if rerun_failed else "")
    return f'<testcase name="{name}" classname="{classname}" time="1.0">{body}</testcase>'


class ParseFailuresTest(unittest.TestCase):
    def test_all_pass(self):
        self.assertEqual((0, 0), parse(suite(case("a()"), case("b()"))))

    def test_single_failure_without_retry(self):
        self.assertEqual((1, 0), parse(suite(case("a()", failed=True), case("b()"))))

    def test_gradle_retry_that_passes_is_not_a_failure(self):
        # Shape written by the Gradle test-retry plugin: the failed attempt, then the passing retry.
        self.assertEqual((0, 0), parse(suite(case("a()", failed=True), case("a()"))))

    def test_gradle_retry_that_fails_again_is_one_failure(self):
        self.assertEqual((1, 0), parse(suite(case("a()", failed=True), case("a()", failed=True))))

    def test_same_name_in_another_class_is_a_different_test(self):
        xml = suite(case("a()", failed=True, classname="X"), case("a()", classname="Y"))
        self.assertEqual((1, 0), parse(xml))

    def test_surefire_rerun_failure(self):
        self.assertEqual((0, 1), parse(suite(case("a()", failed=True, rerun_failed=True))))


if __name__ == "__main__":
    unittest.main()
