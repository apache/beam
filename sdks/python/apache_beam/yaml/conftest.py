# This global will be set by pytest_configure
# and can be imported by other modules.
# Initialize with a default that matches the default.
yaml_test_files_dir = "tests"


def pytest_addoption(parser):
  parser.addoption(
      "--test_files_dir",
      action="store",
      default="tests",
      help="Directory with YAML test files, relative to integration_tests.py")


def pytest_configure(config):
  global yaml_test_files_dir
  yaml_test_files_dir = config.getoption("test_files_dir")
