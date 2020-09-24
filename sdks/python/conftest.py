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

from apache_beam.options import pipeline_options

MAX_SUPPORTED_PYTHON_VERSION = (3, 8)

# See pytest.ini for main collection rules.
collect_ignore_glob = []
if sys.version_info < (3,):
  collect_ignore_glob.append('*_py3*.py')
else:
  for minor in range(sys.version_info.minor + 1,
                     MAX_SUPPORTED_PYTHON_VERSION[1] + 1):
    collect_ignore_glob.append('*_py3%d.py' % minor)


def pytest_configure(config):
  # Enable optional type checks on all tests.
  pipeline_options.enable_all_additional_type_checks()
