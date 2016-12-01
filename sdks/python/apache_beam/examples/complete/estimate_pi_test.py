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

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.complete import estimate_pi
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import DataflowAssertException


def in_between(lower, upper):
  def _in_between(actual):
    _, _, estimate = actual[0]
    if estimate < lower or estimate > upper:
      raise DataflowAssertException(
          'Failed assert: %f not in [%f, %f]' % (estimate, lower, upper))
  return _in_between


class EstimatePiTest(unittest.TestCase):

  def test_basics(self):
    p = beam.Pipeline('DirectPipelineRunner')
    result = p | 'Estimate' >> estimate_pi.EstimatePiTransform()

    # Note: Probabilistically speaking this test can fail with a probability
    # that is very small (VERY) given that we run at least 10 million trials.
    assert_that(result, in_between(3.13, 3.15))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
