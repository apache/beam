# coding=utf-8
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
import math
import unittest

import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import approximateunique

# pytype: skip-file


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.'
    'approximateunique.print',
    lambda x: x)
class ApproximateUniqueTest(unittest.TestCase):
  def test_approximateunique(self):
    def check_result(approx_count):
      actual_count = 1000
      sample_size = 16
      error = 2 / math.sqrt(sample_size)
      assert_that(
          approx_count
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= error]),
          equal_to([True]))

    approximateunique.approximateunique(test=check_result)


if __name__ == '__main__':
  unittest.main()
