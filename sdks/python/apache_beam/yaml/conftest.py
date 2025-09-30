#
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
#

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
