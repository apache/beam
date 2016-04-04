# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for MapTask behavior.
"""


import logging
import unittest


from google.cloud.dataflow.utils.counters import Counter
from google.cloud.dataflow.worker import maptask


class MapTaskTest(unittest.TestCase):

  def test_itercounters(self):
    map_task = maptask.MapTask([], 'test-name', [])
    map_task.counter_factory.get_counter('counter-abc', Counter.SUM)
    counters_found = 0
    for counter in map_task.itercounters():
      logging.info('iterator yielded %s', counter)
      self.assertEqual('counter-abc', counter.name)
      counters_found += 1
    self.assertEqual(1, counters_found)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
