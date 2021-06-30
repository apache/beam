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

"""Tests for interactive_utils module."""

# pytype: skip-file

import unittest
from unittest.mock import patch

from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython
from apache_beam.utils.interactive_utils import is_in_ipython


def unavailable_ipython():
  # ModuleNotFoundError since Py3.6 is sub class of ImportError. An example,
  # 'import apache_beam.hahaha' raises a ModuleNotFoundError while
  # 'from apache_beam import hahaha' raises an ImportError.
  # For simplicity, we only use super class ImportError here.
  raise ImportError('Module IPython is not found.')


def corrupted_ipython():
  raise AttributeError('Module IPython does not contain get_ipython.')


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
class IPythonTest(unittest.TestCase):
  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_is_in_ipython_when_in_ipython_kernel(self, kernel):
    self.assertTrue(is_in_ipython())

  @patch('IPython.get_ipython', new_callable=lambda: unavailable_ipython)
  def test_is_not_in_ipython_when_no_ipython_dep(self, unavailable):
    self.assertFalse(is_in_ipython())

  @patch('IPython.get_ipython', new_callable=lambda: corrupted_ipython)
  def test_is_not_ipython_when_ipython_errors_out(self, corrupted):
    self.assertFalse(is_in_ipython())


if __name__ == '__main__':
  unittest.main()
