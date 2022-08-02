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

import unittest

import pandas as pd

from apache_beam.dataframe.partitionings import Arbitrary
from apache_beam.dataframe.partitionings import Index
from apache_beam.dataframe.partitionings import JoinIndex
from apache_beam.dataframe.partitionings import Singleton


class PartitioningsTest(unittest.TestCase):
  # pylint: disable=bad-option-value

  multi_index_df = pd.DataFrame({
      'shape': ['dodecahedron', 'icosahedron'] * 12,
      'color': ['red', 'yellow', 'blue'] * 8,
      'size': range(24),
      'value': range(24)
  }).set_index(['shape', 'color', 'size'])

  def test_index_is_subpartition(self):
    ordered_list = [
        Singleton(),
        Index([3]),
        Index([1, 3]),
        Index(),
        JoinIndex('ref'),
        JoinIndex(),
        Arbitrary()
    ]
    for loose, strict in zip(ordered_list[:-1], ordered_list[1:]):
      self.assertTrue(strict.is_subpartitioning_of(loose), (strict, loose))
      self.assertFalse(loose.is_subpartitioning_of(strict), (loose, strict))
    # Incomparable.
    self.assertFalse(Index([1, 2]).is_subpartitioning_of(Index([1, 3])))
    self.assertFalse(Index([1, 3]).is_subpartitioning_of(Index([1, 2])))
    self.assertFalse(JoinIndex('a').is_subpartitioning_of(JoinIndex('b')))
    self.assertFalse(JoinIndex('b').is_subpartitioning_of(JoinIndex('a')))

  def _check_partition(self, partitioning, min_non_empty, max_non_empty=None):
    num_partitions = 1000
    if max_non_empty is None:
      max_non_empty = min_non_empty
    parts = list(partitioning.partition_fn(self.multi_index_df, num_partitions))
    self.assertEqual(num_partitions, len(parts))
    self.assertGreaterEqual(len([p for _, p in parts if len(p)]), min_non_empty)
    self.assertLessEqual(len([p for _, p in parts if len(p)]), max_non_empty)
    self.assertEqual(
        sorted(self.multi_index_df.value),
        sorted(sum((list(p.value) for _, p in parts), [])))

  def test_index_partition(self):
    self._check_partition(Index([0]), 2)
    self._check_partition(Index([0, 1]), 6)
    self._check_partition(Index([1]), 3)
    self._check_partition(Index([2]), 7, 24)
    self._check_partition(Index([0, 2]), 7, 24)
    self._check_partition(Index(), 7, 24)

  def test_nothing_subpartition(self):
    for p in [Index([1]), Index([1, 2]), Index(), Singleton()]:
      self.assertTrue(Arbitrary().is_subpartitioning_of(p), p)

  def test_singleton_subpartition(self):
    self.assertTrue(Singleton().is_subpartitioning_of(Singleton()))
    for p in [Arbitrary(), Index([1]), Index([1, 2]), Index()]:
      self.assertFalse(Singleton().is_subpartitioning_of(p), p)

  def test_singleton_partition(self):
    parts = list(Singleton().partition_fn(pd.Series(range(10)), 1000))
    self.assertEqual(1, len(parts))


if __name__ == '__main__':
  unittest.main()
