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

"""Test for the filters example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import filters


class FiltersTest(unittest.TestCase):
  # Note that 'removed' should be projected away by the pipeline
  input_data = [
      {'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2011, 'month': 1, 'day': 3, 'mean_temp': 5, 'removed': 'a'},
      {'year': 2013, 'month': 2, 'day': 1, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2011, 'month': 3, 'day': 3, 'mean_temp': 5, 'removed': 'a'},
      ]

  def _get_result_for_month(self, month):
    p = beam.Pipeline('DirectPipelineRunner')
    rows = (p | 'create' >> beam.Create(self.input_data))

    results = filters.filter_cold_days(rows, month)
    return results

  def test_basic(self):
    """Test that the correct result is returned for a simple dataset."""
    results = self._get_result_for_month(1)
    beam.assert_that(
        results,
        beam.equal_to([{'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3},
                       {'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3}]))
    results.pipeline.run()

  def test_basic_empty(self):
    """Test that the correct empty result is returned for a simple dataset."""
    results = self._get_result_for_month(3)
    beam.assert_that(results, beam.equal_to([]))
    results.pipeline.run()

  def test_basic_empty_missing(self):
    """Test that the correct empty result is returned for a missing month."""
    results = self._get_result_for_month(4)
    beam.assert_that(results, beam.equal_to([]))
    results.pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
