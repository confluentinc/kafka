import sys
import glob
from xml.etree import ElementTree
from junitparser import JUnitXml


def count_test_results(xml_files):
    total_passed = 0
    total_failures = 0
    total_rerun_failures = 0
    total_skipped = 0

    for junit_xml_file in xml_files:
        print(f"Reading file - {junit_xml_file}")
        passed = 0
        skipped = 0
        failures = 0
        rerun_failures = 0
        try:
            xml = JUnitXml.fromfile(junit_xml_file)
            passed = xml.tests - xml.failures - xml.skipped
            skipped = xml.skipped

            context = ElementTree.iterparse(junit_xml_file, events=("start", "end"))
            failures, rerun_failures = parse_failures_and_rerun_tests(context)

            print(f"Counts from file: {junit_xml_file}. Passed={passed}, "
                  f"Failures={failures}, rerun failures={rerun_failures}, skipped={skipped}")

        except Exception as e:
            print(f"Unexpected error while processing test xml for {junit_xml_file}: {e}")
            failures += failures

        total_passed += passed
        total_failures += failures
        total_rerun_failures += rerun_failures
        total_skipped += skipped

    return total_passed, total_failures, total_rerun_failures, total_skipped

def parse_failures_and_rerun_tests(context):
    failures = 0
    rerun_failures = 0
    for event, elem in context:
        if event == "start" and elem.tag == "testcase":
            testcase_has_failure = False
            testcase_has_rerun_failure = False
        elif event == "end" and elem.tag == "testcase":
            if testcase_has_failure and not testcase_has_rerun_failure:
                failures += 1
            elif testcase_has_rerun_failure:
                rerun_failures += 1
        elif event == "end" and elem.tag == "failure":
            testcase_has_failure = True
        elif event == "end" and (elem.tag == "rerunFailure" or elem.tag == "rerunError"):
            testcase_has_rerun_failure = True
    return failures, rerun_failures

def main():
    xml_files = []
    for path in sys.argv[1:]:
        xml_files.extend(glob.glob(path, recursive=True))

    passed, failures, rerun_failures, skipped = count_test_results(xml_files)

    print(f"Test results:\nPassed: {passed}, Failures: {failures}, Rerun Failures: {rerun_failures}, "
          f"Skipped: {skipped}")

    if rerun_failures > 0 or failures > 0:
        sys.exit(rerun_failures + failures)



if __name__ == "__main__":
    main()
