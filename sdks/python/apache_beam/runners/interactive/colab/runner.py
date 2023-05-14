"""Executes notebook tests in current enviornment.

Example usage:
  Run all specs in ./base/:   python runner.py base
  Run single test spec:       python runner.py --single-test base/testing.spec
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# import testlib

from collections import OrderedDict

import argparse
import glob
import itertools
import os
import sys
import yaml

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

import apache_beam as beam
IS_BEAM_DEV = False
if "dev" in beam.__version__:
  IS_BEAM_DEV = True


def _skip_test(spec):
  if IS_BEAM_DEV and "sql" in str(spec).lower():
    return True
  return False


class Executor:
  """Loads a single test spec, executes and compares target notebook."""
  def __init__(self, spec_path, timeout_secs):
    with open(spec_path, "r") as spec_f:
      self.spec = yaml.safe_load(spec_f)
    # notebook in spec relative to spec's location
    self.dir = os.path.dirname(spec_path)
    self.notebook_path = os.path.join(self.dir, self.spec["notebook"])
    self.tests = self.spec["tests"]
    self.timeout_secs = timeout_secs

  def execute_tests(self):
    """Executes notebook and compares to test spec.

    Returns:
      # of tests, # of errors, error_dict
      where error_dict maps (cell number, test class, expected output) to string
    """
    # load and execute notebook using nbconvert
    with open(self.notebook_path, "r") as nb_f:
      nb = nbformat.read(nb_f, as_version=4)
    ExecutePreprocessor.timeout = self.timeout_secs
    ep = ExecutePreprocessor(allow_errors=True)
    # Executes full notebook, result is classic JSON representation
    exec_nb, _ = ep.preprocess(nb, {"metadata": {"path": self.dir + "/"}})
    test_count = 0
    error_count = 0
    errors = OrderedDict()
    code_cells = {}
    for cell in exec_nb["cells"]:
      if cell["cell_type"] == "code":
        code_cells[cell["execution_count"]] = cell
    # generate test cases for each line, substituting in the output
    for cell_num in sorted(self.tests.keys()):
      if cell_num not in code_cells:
        test_count += 1
        error_count += 1
        errors[(cell_num, "", "")] = "Given cell does not exist."
      else:
        cell = code_cells[cell_num]
        for test in self.tests[cell_num]:
          cls, setup = list(test.items())[0]
          test_count += 1
          try:
            getattr(sys.modules["testlib"], cls)(setup).check(cell)
          except Exception as e:
            error_count += 1
            errors[(cell_num, cls, setup)] = str(e)
    return test_count, error_count, errors


def main():
  """Executes tests.

  Return codes:
    0 if all tests passed, 1 otherwise
    2 if no tests were found
  """
  parser = argparse.ArgumentParser("Notebook test runner")
  # TODO: for environments with multiple kernels, add kernel name
  parser.add_argument("suites", nargs="*")
  parser.add_argument("--single-test")
  parser.add_argument(
      "--timeout-secs",
      type=int,
      default=30,
      help="The time to wait (in seconds) for output from the execution of a "
      "cell.")
  args = parser.parse_args()

  if args.single_test:
    specs = [args.single_test]
  else:
    specs = itertools.chain.from_iterable([
        glob.iglob("{}/**.spec".format(suite), recursive=True)
        for suite in args.suites
    ])

  total_test_count = 0
  total_error_count = 0
  total_errors = []
  for spec in specs:
    if _skip_test(spec):
      continue
    test_count, error_count, errors = Executor(
        spec, args.timeout_secs).execute_tests()
    total_test_count += test_count
    total_error_count += error_count
    if errors:
      total_errors.append((spec, errors))

  if total_test_count == 0:
    print("No tests were found.")
    sys.exit(2)
  print("{} errors in {} tests.\n".format(total_error_count, total_test_count))
  if total_error_count:
    for spec, errors in total_errors:
      print("In spec {}:".format(spec))
      for info, error in errors.items():
        cell_num, cls, setup = info
        print("\tCell {} [{}: {}]: {}".format(cell_num, cls, setup, error))
      print("\n")
  ret_code = 0 if total_error_count == 0 else 1
  sys.exit(ret_code)


if __name__ == "__main__":
  main()
