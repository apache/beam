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
"""Pytest configuration and custom hooks."""

from __future__ import absolute_import

import sys

from apache_beam.testing.test_pipeline import TestPipeline

# See pytest.ini for main collection rules.
if sys.version_info < (3,):
  collect_ignore_glob = ['*_py3.py']


def pytest_addoption(parser):
  """This hook adds custom flags to the pytest command line."""
  # These flags are used by apache_beam.testing.test_pipeline.
  parser.addoption('--test-pipeline-options',
                   action='store',
                   type=str,
                   help='providing pipeline options to run tests on runner')
  parser.addoption('--not-use-test-runner-api',
                   action='store_true',
                   default=False,
                   help='whether not to use test-runner-api')


def pytest_configure(config):
  """Saves options added in pytest_addoption for later use.

  This is necessary since pytest-xdist workers do not have the same sys.argv as
  the main pytest invocation. xdist does seem to pickle TestPipeline
  """
  TestPipeline.pytest_test_pipeline_options = config.getoption(
      'test_pipeline_options', default='')
  TestPipeline.pytest_not_use_test_runner_api = config.getoption(
      'not_use_test_runner_api', default='')
