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

"""Test for the estimate_pi example."""

# pytype: skip-file

import json
import logging
import os
import tempfile
import unittest

import pytest

from apache_beam.examples.complete import estimate_pi
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import open_shards


def in_between(lower, upper):
  def _in_between(actual):
    _, _, estimate = actual[0]
    if estimate < lower or estimate > upper:
      raise BeamAssertException(
          'Failed assert: %f not in [%f, %f]' % (estimate, lower, upper))

  return _in_between


@pytest.mark.examples_postcommit
class EstimatePiTest(unittest.TestCase):
  def test_basics(self):
    temp_folder = tempfile.mkdtemp()
    estimate_pi.run(
        ['--output', os.path.join(temp_folder, 'result'), '--tries', '5000'])
    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      result = json.loads(result_file.read().strip())[2]
    # Note: Probabilistically speaking this test can fail with a probability
    # that is very small (VERY) given that we run at least 500 thousand
    # trials.
    self.assertTrue(3.125 <= result <= 3.155)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
