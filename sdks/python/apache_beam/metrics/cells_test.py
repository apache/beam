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

import copy
import itertools
import random
import threading
import unittest

from apache_beam.metrics.cells import BoundedTrieData
from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.cells import GaugeData
from apache_beam.metrics.cells import StringSetCell
from apache_beam.metrics.cells import StringSetData
from apache_beam.metrics.cells import _BoundedTrieNode
from apache_beam.metrics.metricbase import MetricName


class TestCounterCell(unittest.TestCase):
  @classmethod
  def _modify_counter(cls, d):
    for i in range(cls.NUM_ITERATIONS):
      d.inc(i)

  NUM_THREADS = 5
  NUM_ITERATIONS = 100

  def test_parallel_access(self):
    # We create NUM_THREADS threads that concurrently modify the counter.
    threads = []
    c = CounterCell()
    for _ in range(TestCounterCell.NUM_THREADS):
      t = threading.Thread(target=TestCounterCell._modify_counter, args=(c, ))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (
        self.NUM_ITERATIONS * (self.NUM_ITERATIONS - 1) // 2 * self.NUM_THREADS)
    self.assertEqual(c.get_cumulative(), total)

  def test_basic_operations(self):
    c = CounterCell()
    c.inc(2)
    self.assertEqual(c.get_cumulative(), 2)

    c.dec(10)
    self.assertEqual(c.get_cumulative(), -8)

    c.dec()
    self.assertEqual(c.get_cumulative(), -9)

    c.inc()
    self.assertEqual(c.get_cumulative(), -8)

  def test_start_time_set(self):
    c = CounterCell()
    c.inc(2)

    name = MetricName('namespace', 'name1')
    mi = c.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


class TestDistributionCell(unittest.TestCase):
  @classmethod
  def _modify_distribution(cls, d):
    for i in range(cls.NUM_ITERATIONS):
      d.update(i)

  NUM_THREADS = 5
  NUM_ITERATIONS = 100

  def test_parallel_access(self):
    # We create NUM_THREADS threads that concurrently modify the distribution.
    threads = []
    d = DistributionCell()
    for _ in range(TestDistributionCell.NUM_THREADS):
      t = threading.Thread(
          target=TestDistributionCell._modify_distribution, args=(d, ))
      threads.append(t)
      t.start()

    for t in threads:
      t.join()

    total = (
        self.NUM_ITERATIONS * (self.NUM_ITERATIONS - 1) // 2 * self.NUM_THREADS)

    count = (self.NUM_ITERATIONS * self.NUM_THREADS)

    self.assertEqual(
        d.get_cumulative(),
        DistributionData(total, count, 0, self.NUM_ITERATIONS - 1))

  def test_basic_operations(self):
    d = DistributionCell()
    d.update(10)
    self.assertEqual(d.get_cumulative(), DistributionData(10, 1, 10, 10))

    d.update(2)
    self.assertEqual(d.get_cumulative(), DistributionData(12, 2, 2, 10))

    d.update(900)
    self.assertEqual(d.get_cumulative(), DistributionData(912, 3, 2, 900))

  def test_integer_only(self):
    d = DistributionCell()
    d.update(3.1)
    d.update(3.2)
    d.update(3.3)
    self.assertEqual(d.get_cumulative(), DistributionData(9, 3, 3, 3))

  def test_start_time_set(self):
    d = DistributionCell()
    d.update(3.1)

    name = MetricName('namespace', 'name1')
    mi = d.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


class TestGaugeCell(unittest.TestCase):
  def test_basic_operations(self):
    g = GaugeCell()
    g.set(10)
    self.assertEqual(g.get_cumulative().value, GaugeData(10).value)

    g.set(2)
    self.assertEqual(g.get_cumulative().value, 2)

  def test_integer_only(self):
    g = GaugeCell()
    g.set(3.3)
    self.assertEqual(g.get_cumulative().value, 3)

  def test_combine_appropriately(self):
    g1 = GaugeCell()
    g1.set(3)

    g2 = GaugeCell()
    g2.set(1)

    # THe second Gauge, with value 1 was the most recent, so it should be
    # the final result.
    result = g2.combine(g1)
    self.assertEqual(result.data.value, 1)

  def test_start_time_set(self):
    g1 = GaugeCell()
    g1.set(3)

    name = MetricName('namespace', 'name1')
    mi = g1.to_runner_api_monitoring_info(name, 'transform_id')
    self.assertGreater(mi.start_time.seconds, 0)


class TestStringSetCell(unittest.TestCase):
  def test_not_leak_mutable_set(self):
    c = StringSetCell()
    c.add('test')
    c.add('another')
    s = c.get_cumulative()
    self.assertEqual(s, StringSetData({'test', 'another'}, 11))
    s.add('yet another')
    self.assertEqual(c.get_cumulative(), StringSetData({'test', 'another'}, 11))

  def test_combine_appropriately(self):
    s1 = StringSetCell()
    s1.add('1')
    s1.add('2')

    s2 = StringSetCell()
    s2.add('1')
    s2.add('3')

    result = s2.combine(s1)
    self.assertEqual(result.data, StringSetData({'1', '2', '3'}))

  def test_add_size_tracked_correctly(self):
    s = StringSetCell()
    s.add('1')
    s.add('2')
    self.assertEqual(s.data.string_size, 2)
    s.add('2')
    s.add('3')
    self.assertEqual(s.data.string_size, 3)


class TestBoundedTrieNode(unittest.TestCase):
  @classmethod
  def random_segments_fixed_depth(cls, n, depth, overlap, rand):
    if depth == 0:
      yield from ((), ) * n
    else:
      seen = []
      to_string = lambda ix: chr(ord('a') + ix) if ix < 26 else f'z{ix}'
      for suffix in cls.random_segments_fixed_depth(n, depth - 1, overlap,
                                                    rand):
        if not seen or rand.random() > overlap:
          prefix = to_string(len(seen))
          seen.append(prefix)
        else:
          prefix = rand.choice(seen)
        yield (prefix, ) + suffix

  @classmethod
  def random_segments(cls, n, min_depth, max_depth, overlap, rand):
    for depth, segments in zip(
        itertools.cycle(range(min_depth, max_depth + 1)),
        cls.random_segments_fixed_depth(n, max_depth, overlap, rand)):
      yield segments[:depth]

  def assert_covers(self, node, expected, max_truncated=0):
    self.assert_covers_flattened(node.flattened(), expected, max_truncated)

  def assert_covers_flattened(self, flattened, expected, max_truncated=0):
    expected = set(expected)
    # Split node into the exact and truncated segments.
    partitioned = {True: set(), False: set()}
    for segments in flattened:
      partitioned[segments[-1]].add(segments[:-1])
    exact, truncated = partitioned[False], partitioned[True]
    # Check we cover both parts.
    self.assertLessEqual(len(truncated), max_truncated, truncated)
    self.assertTrue(exact.issubset(expected), exact - expected)
    seen_truncated = set()
    for segments in expected - exact:
      found = 0
      for ix in range(len(segments)):
        if segments[:ix] in truncated:
          seen_truncated.add(segments[:ix])
          found += 1
      if found != 1:
        self.fail(
            f"Expected exactly one prefix of {segments} "
            f"to occur in {truncated}, found {found}")
    self.assertEqual(seen_truncated, truncated, truncated - seen_truncated)

  def run_covers_test(self, flattened, expected, max_truncated):
    def parse(s):
      return tuple(s.strip('*')) + (s.endswith('*'), )

    self.assert_covers_flattened([parse(s) for s in flattened],
                                 [tuple(s) for s in expected],
                                 max_truncated)

  def test_covers_exact(self):
    self.run_covers_test(['ab', 'ac', 'cd'], ['ab', 'ac', 'cd'], 0)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['ab', 'ac', 'cd'], ['ac', 'cd'], 0)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['ab', 'ac'], ['ab', 'ac', 'cd'], 0)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['a*', 'cd'], ['ab', 'ac', 'cd'], 0)

  def test_covers_trunacted(self):
    self.run_covers_test(['a*', 'cd'], ['ab', 'ac', 'cd'], 1)
    self.run_covers_test(['a*', 'cd'], ['ab', 'ac', 'abcde', 'cd'], 1)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['ab', 'ac', 'cd'], ['ac', 'cd'], 1)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['ab', 'ac'], ['ab', 'ac', 'cd'], 1)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['a*', 'c*'], ['ab', 'ac', 'cd'], 1)
    with self.assertRaises(AssertionError):
      self.run_covers_test(['a*', 'c*'], ['ab', 'ac'], 1)

  def run_test(self, to_add):
    everything = list(set(to_add))
    all_prefixees = set(
        segments[:ix] for segments in everything for ix in range(len(segments)))
    everything_deduped = set(everything) - all_prefixees

    # Check basic addition.
    node = _BoundedTrieNode()
    total_size = node.size()
    self.assertEqual(total_size, 1)
    for segments in everything:
      total_size += node.add(segments)
    self.assertEqual(node.size(), len(everything_deduped), node)
    self.assertEqual(node.size(), total_size, node)
    self.assert_covers(node, everything_deduped)

    # Check merging
    node0 = _BoundedTrieNode()
    node0.add_all(everything[0::2])
    node1 = _BoundedTrieNode()
    node1.add_all(everything[1::2])
    pre_merge_size = node0.size()
    merge_delta = node0.merge(node1)
    self.assertEqual(node0.size(), pre_merge_size + merge_delta)
    self.assertEqual(node0, node)

    # Check trimming.
    if node.size() > 1:
      trim_delta = node.trim()
      self.assertLess(trim_delta, 0, node)
      self.assertEqual(node.size(), total_size + trim_delta)
      self.assert_covers(node, everything_deduped, max_truncated=1)

    if node.size() > 1:
      trim2_delta = node.trim()
      self.assertLess(trim2_delta, 0)
      self.assertEqual(node.size(), total_size + trim_delta + trim2_delta)
      self.assert_covers(node, everything_deduped, max_truncated=2)

    # Adding after trimming should be a no-op.
    node_copy = copy.deepcopy(node)
    for segments in everything:
      self.assertEqual(node.add(segments), 0)
    self.assertEqual(node, node_copy)

    # Merging after trimming should be a no-op.
    self.assertEqual(node.merge(node0), 0)
    self.assertEqual(node.merge(node1), 0)
    self.assertEqual(node, node_copy)

    if node._truncated:
      expected_delta = 0
    else:
      expected_delta = 2

    # Adding something new is not.
    new_values = [('new1', ), ('new2', 'new2.1')]
    self.assertEqual(node.add_all(new_values), expected_delta)
    self.assert_covers(
        node, list(everything_deduped) + new_values, max_truncated=2)

    # Nor is merging something new.
    new_values_node = _BoundedTrieNode()
    new_values_node.add_all(new_values)
    self.assertEqual(node_copy.merge(new_values_node), expected_delta)
    self.assert_covers(
        node_copy, list(everything_deduped) + new_values, max_truncated=2)

  def run_fuzz(self, iterations=10, **params):
    for _ in range(iterations):
      seed = random.getrandbits(64)
      segments = self.random_segments(**params, rand=random.Random(seed))
      try:
        self.run_test(segments)
      except:
        print("SEED", seed)
        raise

  def test_trivial(self):
    self.run_test([('a', 'b'), ('a', 'c')])

  def test_flat(self):
    self.run_test([('a', 'a'), ('b', 'b'), ('c', 'c')])

  def test_deep(self):
    self.run_test([('a', ) * 10, ('b', ) * 12])

  def test_small(self):
    self.run_fuzz(n=5, min_depth=2, max_depth=3, overlap=0.5)

  def test_medium(self):
    self.run_fuzz(n=20, min_depth=2, max_depth=4, overlap=0.5)

  def test_large_sparse(self):
    self.run_fuzz(n=120, min_depth=2, max_depth=4, overlap=0.2)

  def test_large_dense(self):
    self.run_fuzz(n=120, min_depth=2, max_depth=4, overlap=0.8)

  def test_bounded_trie_data_combine(self):
    empty = BoundedTrieData()
    # The merging here isn't complicated we're just ensuring that
    # BoundedTrieData invokes _BoundedTrieNode correctly.
    singletonA = BoundedTrieData(singleton=('a', 'a'))
    singletonB = BoundedTrieData(singleton=('b', 'b'))
    lots_root = _BoundedTrieNode()
    lots_root.add_all([('c', 'c'), ('d', 'd')])
    lots = BoundedTrieData(root=lots_root)
    self.assertEqual(empty.get_result(), set())
    self.assertEqual(
        empty.combine(singletonA).get_result(), set([('a', 'a', False)]))
    self.assertEqual(
        singletonA.combine(empty).get_result(), set([('a', 'a', False)]))
    self.assertEqual(
        singletonA.combine(singletonB).get_result(),
        set([('a', 'a', False), ('b', 'b', False)]))
    self.assertEqual(
        singletonA.combine(lots).get_result(),
        set([('a', 'a', False), ('c', 'c', False), ('d', 'd', False)]))
    self.assertEqual(
        lots.combine(singletonA).get_result(),
        set([('a', 'a', False), ('c', 'c', False), ('d', 'd', False)]))

  def test_bounded_trie_data_combine_trim(self):
    left = _BoundedTrieNode()
    left.add_all([('a', 'x'), ('b', 'd')])
    right = _BoundedTrieNode()
    right.add_all([('a', 'y'), ('c', 'd')])
    self.assertEqual(
        BoundedTrieData(root=left).combine(
            BoundedTrieData(root=right, bound=3)).get_result(),
        set([('a', True), ('b', 'd', False), ('c', 'd', False)]))


if __name__ == '__main__':
  unittest.main()
