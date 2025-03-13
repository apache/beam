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

"""Unit tests for uncompiled implementation of coder impls."""
# pytype: skip-file

import logging
import unittest

# Run all the standard coder test cases.
from apache_beam.coders.coders_test_common import *


@unittest.skip(
    'Remove non-cython tests.'
    'https://github.com/apache/beam/issues/28307')
class SlowCoders(unittest.TestCase):
  def test_using_slow_impl(self):
    try:
      # pylint: disable=wrong-import-position
      # pylint: disable=unused-import
      from Cython.Build import cythonize
      self.skipTest('Found cython, cannot test non-compiled implementation.')
    except ImportError:
      # Assert that we are not using the compiled implementation.
      with self.assertRaises(ImportError):
        # pylint: disable=wrong-import-order, wrong-import-position
        # pylint: disable=unused-import
        import apache_beam.coders.stream


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
