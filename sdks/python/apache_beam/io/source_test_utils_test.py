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

import logging
import tempfile
import unittest

from apache_beam.io.filebasedsource_test import LineSource
import apache_beam.io.source_test_utils as source_test_utils


class SourceTestUtilsTest(unittest.TestCase):

  def _create_file_with_data(self, lines):
    assert isinstance(lines, list)
    with tempfile.NamedTemporaryFile(delete=False) as f:
      for line in lines:
        f.write(line + '\n')

      return f.name

  def _create_data(self, num_lines):
    return ['line ' + str(i) for i in range(num_lines)]

  def _create_source(self, data):
    source = LineSource(self._create_file_with_data(data))
    # By performing initial splitting, we can get a source for a single file.
    # This source, that uses OffsetRangeTracker, is better for testing purposes,
    # than using the original source for a file-pattern.
    for bundle in source.split(float('inf')):
      return bundle.source

  def test_read_from_source(self):
    data = self._create_data(100)
    source = self._create_source(data)
    self.assertItemsEqual(
        data, source_test_utils.readFromSource(source, None, None))

  def test_source_equals_reference_source(self):
    data = self._create_data(100)
    reference_source = self._create_source(data)
    sources_info = [(split.source, split.start_position, split.stop_position)
                    for split in reference_source.split(desired_bundle_size=50)]
    if len(sources_info) < 2:
      raise ValueError('Test is too trivial since splitting only generated %d'
                       'bundles. Please adjust the test so that at least '
                       'two splits get generated.', len(sources_info))

    source_test_utils.assertSourcesEqualReferenceSource(
        (reference_source, None, None), sources_info)

  def test_split_at_fraction_successful(self):
    data = self._create_data(100)
    source = self._create_source(data)
    result1 = source_test_utils.assertSplitAtFractionBehavior(
        source, 10, 0.5,
        source_test_utils.ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT)
    result2 = source_test_utils.assertSplitAtFractionBehavior(
        source, 20, 0.5,
        source_test_utils.ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT)
    self.assertEquals(result1, result2)
    self.assertEquals(100, result1[0] + result1[1])

    result3 = source_test_utils.assertSplitAtFractionBehavior(
        source, 30, 0.8,
        source_test_utils.ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT)
    result4 = source_test_utils.assertSplitAtFractionBehavior(
        source, 50, 0.8,
        source_test_utils.ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT)
    self.assertEquals(result3, result4)
    self.assertEquals(100, result3[0] + result4[1])

    self.assertTrue(result1[0] < result3[0])
    self.assertTrue(result1[1] > result3[1])

  def test_split_at_fraction_fails(self):
    data = self._create_data(100)
    source = self._create_source(data)

    result = source_test_utils.assertSplitAtFractionBehavior(
        source, 90, 0.1, source_test_utils.ExpectedSplitOutcome.MUST_FAIL)
    self.assertEquals(result[0], 100)
    self.assertEquals(result[1], -1)

    with self.assertRaises(ValueError):
      source_test_utils.assertSplitAtFractionBehavior(
          source, 10, 0.5, source_test_utils.ExpectedSplitOutcome.MUST_FAIL)

  def test_split_at_fraction_binary(self):
    data = self._create_data(100)
    source = self._create_source(data)

    stats = source_test_utils.SplitFractionStatistics([], [])
    source_test_utils.assertSplitAtFractionBinary(
        source, data, 10, 0.5, None, 0.8, None, stats)

    # These lists should not be empty now.
    self.assertTrue(stats.successful_fractions)
    self.assertTrue(stats.non_trivial_fractions)

  def test_split_at_fraction_exhaustive(self):
    data = self._create_data(20)
    source = self._create_source(data)

    source_test_utils.assertSplitAtFractionExhaustive(source)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
