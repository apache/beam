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

# pytype: skip-file

import unittest

import mock

from apache_beam.io import OffsetRangeTracker
from apache_beam.io import source_test_utils
from apache_beam.io.utils import CountingSource


class CountingSourceTest(unittest.TestCase):
  def setUp(self):
    self.source = CountingSource(10)

  def test_estimate_size(self):
    self.assertEqual(10, self.source.estimate_size())

  @mock.patch('apache_beam.io.utils.OffsetRangeTracker')
  def test_get_range_tracker(self, mock_tracker):
    _ = self.source.get_range_tracker(None, None)
    mock_tracker.assert_called_with(0, 10)
    _ = self.source.get_range_tracker(3, 7)
    mock_tracker.assert_called_with(3, 7)

  def test_read(self):
    tracker = OffsetRangeTracker(3, 6)
    res = list(self.source.read(tracker))
    self.assertEqual([3, 4, 5], res)

  def test_split(self):
    for size in [1, 3, 10]:
      splits = list(self.source.split(desired_bundle_size=size))

      reference_info = (self.source, None, None)
      sources_info = ([
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ])
      source_test_utils.assert_sources_equal_reference_source(
          reference_info, sources_info)

  def test_dynamic_work_rebalancing(self):
    splits = list(self.source.split(desired_bundle_size=20))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)


if __name__ == '__main__':
  unittest.main()
