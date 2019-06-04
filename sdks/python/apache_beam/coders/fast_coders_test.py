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

"""Unit tests for compiled implementation of coder impls."""
from __future__ import absolute_import

import logging
import unittest


# Run all the standard coder test cases.
from apache_beam.coders.coders_test_common import *
from apache_beam.tools import utils


class FastCoders(unittest.TestCase):

  def test_using_fast_impl(self):
    try:
      utils.check_compiled('apache_beam.coders')
    except RuntimeError:
      self.skipTest('Cython is not installed')
    # pylint: disable=wrong-import-order, wrong-import-position
    # pylint: disable=unused-variable
    import apache_beam.coders.stream


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
