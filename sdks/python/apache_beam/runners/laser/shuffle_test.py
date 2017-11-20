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

"""Unit tests for the range_trackers module."""

import logging
import unittest

from apache_beam.runners.laser.laser_runner import SimpleShuffleDataset
from apache_beam.runners.laser.laser_runner import LexicographicPosition
from apache_beam.runners.laser.laser_runner import LexicographicRange


class ShuffleTest(unittest.TestCase):
  def test_simple_shuffle_dataset(self):
    dataset = SimpleShuffleDataset()
    txn_id = 1
    for i in range(50):
      dataset.put(1, 'k%d' % i, 'value%d' % i)
    dataset.commit_transaction(txn_id)
    dataset.finalize()
    print dataset.summarize_key_range(LexicographicRange(LexicographicPosition.KEYSPACE_BEGINNING, LexicographicPosition.KEYSPACE_END))
    print dataset.summarize_key_range(LexicographicRange('b', LexicographicPosition.KEYSPACE_END))
    print dataset.summarize_key_range(LexicographicRange('k2', LexicographicPosition.KEYSPACE_END))
    print dataset.summarize_key_range(LexicographicRange('k0', 'k2'))
    print dataset.summarize_key_range(LexicographicRange('k0', 'k1'))
    print dataset.summarize_key_range(LexicographicRange('zz', LexicographicPosition.KEYSPACE_END))
    # TODO: add more reading tests
    print dataset.read(LexicographicRange('k1', LexicographicPosition.KEYSPACE_END), None, 8+10)
    print dataset.read(LexicographicRange('k1', LexicographicPosition.KEYSPACE_END), '{"start": 2}', 4)
    print dataset.read(LexicographicRange('k1', LexicographicPosition.KEYSPACE_END), '{"start": 2}', 40)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
