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

"""A local plugin for Pytest

Pytest will invoke all self-defined hooks which are build for Beam Python
testing framework.
"""

import pytest


def pytest_addoption(parser):
  """Register command line options to pytest before command line parsing"""
  parser.addoption('--test_options',
                   nargs='?',
                   type=str,
                   action='store',
                   help='providing pipeline options to run tests on service')
  parser.addoption('--sub_categories',
                   nargs='+',
                   type=str,
                   action='store',
                   help='providing supported features ')


def pytest_runtest_setup(item):
  """Set up ValidatesRunner marker and handle sub-categories before each
  collected functions run"""
  marker = item.get_marker('ValidatesRunner')
  category_args = item.config.getoption('sub_categories')
  if category_args is not None:
    if marker is None or len(marker.args) == 0:
      pytest.skip('Skip ValidatesRunner tests which has no category')

    func_category = marker.args[0]
    if func_category not in category_args:
      pytest.skip('Skip ValidatesRunner tests that is not in target categories')
