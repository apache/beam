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

"""Unit tests for our libraries of combine PTransforms."""
from __future__ import absolute_import
from __future__ import division

import itertools
import random
import sys
import unittest

import hamcrest as hc
from future.builtins import range
from nose.plugins.attrib import attr

import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import window
from apache_beam.transforms.core import CombineGlobally
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import Map
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.typehints import TypeCheckError
from apache_beam.utils.timestamp import Timestamp


class CombineTest(unittest.TestCase):

  def test_builtin_combines(self):
    pipeline = TestPipeline()

    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    mean = sum(vals) / float(len(vals))
    size = len(vals)

    # First for global combines.
    pcoll = pipeline | 'start' >> Create(vals)
    result_mean = pcoll | 'mean' >> combine.Mean.Globally()
    result_count = pcoll | 'count' >> combine.Count.Globally()
    assert_that(result_mean, equal_to([mean]), label='assert:mean')
    assert_that(result_count, equal_to([size]), label='assert:size')

    # Again for per-key combines.
    pcoll = pipeline | 'start-perkey' >> Create([('a', x) for x in vals])
    result_key_mean = pcoll | 'mean-perkey' >> combine.Mean.PerKey()
    result_key_count = pcoll | 'count-perkey' >> combine.Count.PerKey()
    assert_that(result_key_mean, equal_to([('a', mean)]), label='key:mean')
    assert_that(result_key_count, equal_to([('a', size)]), label='key:size')
    pipeline.run()

  def test_top(self):
    pipeline = TestPipeline()

    # First for global combines.
    pcoll = pipeline | 'start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | 'top' >> combine.Top.Largest(5)
    result_bot = pcoll | 'bot' >> combine.Top.Smallest(4)
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')

    # Again for per-key combines.
    pcoll = pipeline | 'start-perkey' >> Create(
        [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_key_top = pcoll | 'top-perkey' >> combine.Top.LargestPerKey(5)
    result_key_bot = pcoll | 'bot-perkey' >> combine.Top.SmallestPerKey(4)
    assert_that(result_key_top, equal_to([('a', [9, 6, 6, 5, 3])]),
                label='key:top')
    assert_that(result_key_bot, equal_to([('a', [0, 1, 1, 1])]),
                label='key:bot')
    pipeline.run()

  @unittest.skipIf(sys.version_info[0] > 2, 'deprecated comparator')
  def test_top_py2(self):
    pipeline = TestPipeline()

    # A parameter we'll be sharing with a custom comparator.
    names = {0: 'zo',
             1: 'one',
             2: 'twoo',
             3: 'three',
             5: 'fiiive',
             6: 'sssssix',
             9: 'nniiinne'}

    # First for global combines.
    pcoll = pipeline | 'start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])

    result_cmp = pcoll | 'cmp' >> combine.Top.Of(
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    result_cmp_rev = pcoll | 'cmp_rev' >> combine.Top.Of(
        3,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names,  # Note parameter passed to comparator.
        reverse=True)
    assert_that(result_cmp, equal_to([[9, 6, 6, 5, 3, 2]]), label='assert:cmp')
    assert_that(result_cmp_rev, equal_to([[0, 1, 1]]), label='assert:cmp_rev')

    # Again for per-key combines.
    pcoll = pipeline | 'start-perkye' >> Create(
        [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_key_cmp = pcoll | 'cmp-perkey' >> combine.Top.PerKey(
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    assert_that(result_key_cmp, equal_to([('a', [9, 6, 6, 5, 3, 2])]),
                label='key:cmp')
    pipeline.run()

  def test_empty_global_top(self):
    with TestPipeline() as p:
      assert_that(p | beam.Create([]) | combine.Top.Largest(10),
                  equal_to([[]]))

  def test_sharded_top(self):
    elements = list(range(100))
    random.shuffle(elements)

    pipeline = TestPipeline()
    shards = [pipeline | 'Shard%s' % shard >> beam.Create(elements[shard::7])
              for shard in range(7)]
    assert_that(shards | beam.Flatten() | combine.Top.Largest(10),
                equal_to([[99, 98, 97, 96, 95, 94, 93, 92, 91, 90]]))
    pipeline.run()

  def test_top_key(self):
    self.assertEqual(
        ['aa', 'bbb', 'c', 'dddd'] | combine.Top.Of(3, key=len),
        [['dddd', 'bbb', 'aa']])
    self.assertEqual(
        ['aa', 'bbb', 'c', 'dddd'] | combine.Top.Of(3, key=len, reverse=True),
        [['c', 'aa', 'bbb']])

  @unittest.skipIf(sys.version_info[0] > 2, 'deprecated comparator')
  def test_top_key_py2(self):
    # The largest elements compared by their length mod 5.
    self.assertEqual(
        ['aa', 'bbbb', 'c', 'ddddd', 'eee', 'ffffff'] | combine.Top.Of(
            3,
            compare=lambda len_a, len_b, m: len_a % m > len_b % m,
            key=len,
            reverse=True,
            m=5),
        [['bbbb', 'eee', 'aa']])

  def test_sharded_top_combine_fn(self):
    def test_combine_fn(combine_fn, shards, expected):
      accumulators = [
          combine_fn.add_inputs(combine_fn.create_accumulator(), shard)
          for shard in shards]
      final_accumulator = combine_fn.merge_accumulators(accumulators)
      self.assertEqual(combine_fn.extract_output(final_accumulator), expected)

    test_combine_fn(combine.TopCombineFn(3), [range(10), range(10)], [9, 9, 8])
    test_combine_fn(combine.TopCombineFn(5),
                    [range(1000), range(100), range(1001)],
                    [1000, 999, 999, 998, 998])

  def test_combine_per_key_top_display_data(self):
    def individual_test_per_key_dd(combineFn):
      transform = beam.CombinePerKey(combineFn)
      dd = DisplayData.create_from(transform)
      expected_items = [
          DisplayDataItemMatcher('combine_fn', combineFn.__class__),
          DisplayDataItemMatcher('n', combineFn._n),
          DisplayDataItemMatcher('compare', combineFn._compare.__name__)]
      hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

    individual_test_per_key_dd(combine.Largest(5))
    individual_test_per_key_dd(combine.Smallest(3))
    individual_test_per_key_dd(combine.TopCombineFn(8))
    individual_test_per_key_dd(combine.Largest(5))

  def test_combine_sample_display_data(self):
    def individual_test_per_key_dd(sampleFn, n):
      trs = [sampleFn(n)]
      for transform in trs:
        dd = DisplayData.create_from(transform)
        hc.assert_that(
            dd.items,
            hc.contains_inanyorder(DisplayDataItemMatcher('n', transform._n)))

    individual_test_per_key_dd(combine.Sample.FixedSizePerKey, 5)
    individual_test_per_key_dd(combine.Sample.FixedSizeGlobally, 5)

  def test_combine_globally_display_data(self):
    transform = beam.CombineGlobally(combine.Smallest(5))
    dd = DisplayData.create_from(transform)
    expected_items = [
        DisplayDataItemMatcher('combine_fn', combine.Smallest),
        DisplayDataItemMatcher('n', 5),
        DisplayDataItemMatcher('compare', 'gt')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_basic_combiners_display_data(self):
    transform = beam.CombineGlobally(
        combine.TupleCombineFn(max, combine.MeanCombineFn(), sum))
    dd = DisplayData.create_from(transform)
    expected_items = [
        DisplayDataItemMatcher('combine_fn', combine.TupleCombineFn),
        DisplayDataItemMatcher('combiners',
                               "['max', 'MeanCombineFn', 'sum']")]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_top_shorthands(self):
    pipeline = TestPipeline()

    pcoll = pipeline | 'start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | 'top' >> beam.CombineGlobally(combine.Largest(5))
    result_bot = pcoll | 'bot' >> beam.CombineGlobally(combine.Smallest(4))
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')

    pcoll = pipeline | 'start-perkey' >> Create(
        [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_ktop = pcoll | 'top-perkey' >> beam.CombinePerKey(combine.Largest(5))
    result_kbot = pcoll | 'bot-perkey' >> beam.CombinePerKey(
        combine.Smallest(4))
    assert_that(result_ktop, equal_to([('a', [9, 6, 6, 5, 3])]), label='k:top')
    assert_that(result_kbot, equal_to([('a', [0, 1, 1, 1])]), label='k:bot')
    pipeline.run()

  def test_top_no_compact(self):

    class TopCombineFnNoCompact(combine.TopCombineFn):

      def compact(self, accumulator):
        return accumulator

    pipeline = TestPipeline()
    pcoll = pipeline | 'Start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | 'Top' >> beam.CombineGlobally(
        TopCombineFnNoCompact(5, key=lambda x: x))
    result_bot = pcoll | 'Bot' >> beam.CombineGlobally(
        TopCombineFnNoCompact(4, reverse=True))
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='Assert:Top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='Assert:Bot')

    pcoll = pipeline | 'Start-Perkey' >> Create(
        [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_ktop = pcoll | 'Top-PerKey' >> beam.CombinePerKey(
        TopCombineFnNoCompact(5, key=lambda x: x))
    result_kbot = pcoll | 'Bot-PerKey' >> beam.CombinePerKey(
        TopCombineFnNoCompact(4, reverse=True))
    assert_that(result_ktop, equal_to([('a', [9, 6, 6, 5, 3])]), label='K:Top')
    assert_that(result_kbot, equal_to([('a', [0, 1, 1, 1])]), label='K:Bot')
    pipeline.run()

  def test_global_sample(self):
    def is_good_sample(actual):
      assert len(actual) == 1
      assert sorted(actual[0]) in [[1, 1, 2], [1, 2, 2]], actual

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> Create([1, 1, 2, 2])
      for ix in range(9):
        assert_that(
            pcoll | 'sample-%d' % ix >> combine.Sample.FixedSizeGlobally(3),
            is_good_sample,
            label='check-%d' % ix)

  def test_per_key_sample(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'start-perkey' >> Create(
        sum(([(i, 1), (i, 1), (i, 2), (i, 2)] for i in range(9)), []))
    result = pcoll | 'sample' >> combine.Sample.FixedSizePerKey(3)

    def matcher():
      def match(actual):
        for _, samples in actual:
          equal_to([3])([len(samples)])
          num_ones = sum(1 for x in samples if x == 1)
          num_twos = sum(1 for x in samples if x == 2)
          equal_to([1, 2])([num_ones, num_twos])
      return match
    assert_that(result, matcher())
    pipeline.run()

  def test_tuple_combine_fn(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([('a', 100, 0.0), ('b', 10, -1), ('c', 1, 100)])
          | beam.CombineGlobally(combine.TupleCombineFn(
              max, combine.MeanCombineFn(), sum)).without_defaults())
      assert_that(result, equal_to([('c', 111.0 / 3, 99.0)]))

  def test_tuple_combine_fn_without_defaults(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([1, 1, 2, 3])
          | beam.CombineGlobally(
              combine.TupleCombineFn(min, combine.MeanCombineFn(), max)
              .with_common_input()).without_defaults())
      assert_that(result, equal_to([(1, 7.0 / 4, 3)]))

  def test_to_list_and_to_dict(self):
    pipeline = TestPipeline()
    the_list = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start' >> Create(the_list)
    result = pcoll | 'to list' >> combine.ToList()

    def matcher(expected):
      def match(actual):
        equal_to(expected[0])(actual[0])
      return match
    assert_that(result, matcher([the_list]))
    pipeline.run()

    pipeline = TestPipeline()
    pairs = [(1, 2), (3, 4), (5, 6)]
    pcoll = pipeline | 'start-pairs' >> Create(pairs)
    result = pcoll | 'to dict' >> combine.ToDict()

    def matcher():
      def match(actual):
        equal_to([1])([len(actual)])
        equal_to(pairs)(actual[0].items())
      return match
    assert_that(result, matcher())
    pipeline.run()

  def test_combine_globally_with_default(self):
    with TestPipeline() as p:
      assert_that(p | Create([]) | CombineGlobally(sum), equal_to([0]))

  def test_combine_globally_without_default(self):
    with TestPipeline() as p:
      result = p | Create([]) | CombineGlobally(sum).without_defaults()
      assert_that(result, equal_to([]))

  def test_combine_globally_with_default_side_input(self):
    class SideInputCombine(PTransform):
      def expand(self, pcoll):
        side = pcoll | CombineGlobally(sum).as_singleton_view()
        main = pcoll.pipeline | Create([None])
        return main | Map(lambda _, s: s, side)

    with TestPipeline() as p:
      result1 = p | 'i1' >> Create([]) | 'c1' >> SideInputCombine()
      result2 = p | 'i2' >> Create([1, 2, 3, 4]) | 'c2' >> SideInputCombine()
      assert_that(result1, equal_to([0]), label='r1')
      assert_that(result2, equal_to([10]), label='r2')

  def test_hot_key_fanout(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(itertools.product(['hot', 'cold'], range(10)))
          | beam.CombinePerKey(combine.MeanCombineFn()).with_hot_key_fanout(
              lambda key: (key == 'hot') * 5))
      assert_that(result, equal_to([('hot', 4.5), ('cold', 4.5)]))

  def test_hot_key_fanout_sharded(self):
    # Lots of elements with the same key with varying/no fanout.
    with TestPipeline() as p:
      elements = [(None, e) for e in range(1000)]
      random.shuffle(elements)
      shards = [p | "Shard%s" % shard >> beam.Create(elements[shard::20])
                for shard in range(20)]
      result = (
          shards
          | beam.Flatten()
          | beam.CombinePerKey(combine.MeanCombineFn()).with_hot_key_fanout(
              lambda key: random.randrange(0, 5)))
      assert_that(result, equal_to([(None, 499.5)]))

  def test_global_fanout(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(range(100))
          | beam.CombineGlobally(combine.MeanCombineFn()).with_fanout(11))
      assert_that(result, equal_to([49.5]))

  def test_MeanCombineFn_combine(self):
    with TestPipeline() as p:
      input = (p
               | beam.Create([('a', 1),
                              ('a', 1),
                              ('a', 4),
                              ('b', 1),
                              ('b', 13)]))
      # The mean of all values regardless of key.
      global_mean = (input
                     | beam.Values()
                     | beam.CombineGlobally(combine.MeanCombineFn()))

      # The (key, mean) pairs for all keys.
      mean_per_key = (input | beam.CombinePerKey(combine.MeanCombineFn()))

      expected_mean_per_key = [('a', 2), ('b', 7)]
      assert_that(global_mean, equal_to([4]), label='global mean')
      assert_that(mean_per_key, equal_to(expected_mean_per_key),
                  label='mean per key')

  def test_MeanCombineFn_combine_empty(self):
    # For each element in a PCollection, if it is float('NaN'), then emits
    # a string 'NaN', otherwise emits str(element).

    with TestPipeline() as p:
      input = (p | beam.Create([]))

      # Compute the mean of all values in the PCollection,
      # then format the mean. Since the Pcollection is empty,
      # the mean is float('NaN'), and is formatted to be a string 'NaN'.
      global_mean = (input
                     | beam.Values()
                     | beam.CombineGlobally(combine.MeanCombineFn())
                     | beam.Map(str))

      mean_per_key = (input | beam.CombinePerKey(combine.MeanCombineFn()))

      # We can't compare one float('NaN') with another float('NaN'),
      # but we can compare one 'nan' string with another string.
      assert_that(global_mean, equal_to(['nan']), label='global mean')
      assert_that(mean_per_key, equal_to([]), label='mean per key')

  def test_sessions_combine(self):
    with TestPipeline() as p:
      input = (
          p
          | beam.Create([('c', 1), ('c', 9), ('c', 12), ('d', 2), ('d', 4)])
          | beam.MapTuple(lambda k, v: window.TimestampedValue((k, v), v))
          | beam.WindowInto(window.Sessions(4)))

      global_sum = (input
                    | beam.Values()
                    | beam.CombineGlobally(sum).without_defaults())
      sum_per_key = input | beam.CombinePerKey(sum)

      # The first window has 3 elements: ('c', 1), ('d', 2), ('d', 4).
      # The second window has 2 elements: ('c', 9), ('c', 12).
      assert_that(global_sum, equal_to([7, 21]), label='global sum')
      assert_that(sum_per_key, equal_to([('c', 1), ('c', 21), ('d', 6)]),
                  label='sum per key')

  def test_fixed_windows_combine(self):
    with TestPipeline() as p:
      input = (
          p
          | beam.Create([('c', 1), ('c', 2), ('c', 10),
                         ('d', 5), ('d', 8), ('d', 9)])
          | beam.MapTuple(lambda k, v: window.TimestampedValue((k, v), v))
          | beam.WindowInto(window.FixedWindows(4)))

      global_sum = (input
                    | beam.Values()
                    | beam.CombineGlobally(sum).without_defaults())
      sum_per_key = input | beam.CombinePerKey(sum)

      # The first window has 2 elements: ('c', 1), ('c', 2).
      # The second window has 1 elements: ('d', 5).
      # The third window has 3 elements: ('c', 10), ('d', 8), ('d', 9).
      assert_that(global_sum, equal_to([3, 5, 27]), label='global sum')
      assert_that(sum_per_key,
                  equal_to([('c', 3), ('c', 10), ('d', 5), ('d', 17)]),
                  label='sum per key')


class LatestTest(unittest.TestCase):

  def test_globally(self):
    l = [window.TimestampedValue(3, 100),
         window.TimestampedValue(1, 200),
         window.TimestampedValue(2, 300)]
    with TestPipeline() as p:
      # Map(lambda x: x) PTransform is added after Create here, because when
      # a PCollection of TimestampedValues is created with Create PTransform,
      # the timestamps are not assigned to it. Adding a Map forces the
      # PCollection to go through a DoFn so that the PCollection consists of
      # the elements with timestamps assigned to them instead of a PCollection
      # of TimestampedValue(element, timestamp).
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.Globally()
      assert_that(latest, equal_to([2]))

  def test_globally_empty(self):
    l = []
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.Globally()
      assert_that(latest, equal_to([None]))

  def test_per_key(self):
    l = [window.TimestampedValue(('a', 1), 300),
         window.TimestampedValue(('b', 3), 100),
         window.TimestampedValue(('a', 2), 200)]
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.PerKey()
      assert_that(latest, equal_to([('a', 1), ('b', 3)]))

  def test_per_key_empty(self):
    l = []
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.PerKey()
      assert_that(latest, equal_to([]))


class LatestCombineFnTest(unittest.TestCase):

  def setUp(self):
    self.fn = combine.LatestCombineFn()

  def test_create_accumulator(self):
    accumulator = self.fn.create_accumulator()
    self.assertEqual(accumulator, (None, window.MIN_TIMESTAMP))

  def test_add_input(self):
    accumulator = self.fn.create_accumulator()
    element = (1, 100)
    new_accumulator = self.fn.add_input(accumulator, element)
    self.assertEqual(new_accumulator, (1, 100))

  def test_merge_accumulators(self):
    accumulators = [(2, 400), (5, 100), (9, 200)]
    merged_accumulator = self.fn.merge_accumulators(accumulators)
    self.assertEqual(merged_accumulator, (2, 400))

  def test_extract_output(self):
    accumulator = (1, 100)
    output = self.fn.extract_output(accumulator)
    self.assertEqual(output, 1)

  def test_with_input_types_decorator_violation(self):
    l_int = [1, 2, 3]
    l_dict = [{'a': 3}, {'g': 5}, {'r': 8}]
    l_3_tuple = [(12, 31, 41), (12, 34, 34), (84, 92, 74)]

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_int)
        _ = pc | beam.CombineGlobally(self.fn)

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_dict)
        _ = pc | beam.CombineGlobally(self.fn)

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_3_tuple)
        _ = pc | beam.CombineGlobally(self.fn)

#
# Test cases for streaming.
#
@attr('ValidatesRunner')
class TimestampCombinerTest(unittest.TestCase):

  def test_combiner_earliest(self):
    """Test TimestampCombiner with EARLIEST."""
    options = PipelineOptions(streaming=True)
    with TestPipeline(options=options) as p:
      result = (p
                | TestStream()
                .add_elements([window.TimestampedValue(('k', 100), 2)])
                .add_elements([window.TimestampedValue(('k', 400), 7)])
                .advance_watermark_to_infinity()
                | beam.WindowInto(
                    window.FixedWindows(10),
                    timestamp_combiner=TimestampCombiner.OUTPUT_AT_EARLIEST)
                | beam.CombinePerKey(sum))

      records = (result
                 | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))

      # All the KV pairs are applied GBK using EARLIEST timestamp for the same
      # key.
      expected_window_to_elements = {
          window.IntervalWindow(0, 10): [
              (('k', 500), Timestamp(2)),
          ],
      }

      assert_that(
          records,
          equal_to_per_window(expected_window_to_elements),
          use_global_window=False,
          label='assert per window')

  def test_combiner_latest(self):
    """Test TimestampCombiner with LATEST."""
    options = PipelineOptions(streaming=True)
    with TestPipeline(options=options) as p:
      result = (p
                | TestStream()
                .add_elements([window.TimestampedValue(('k', 100), 2)])
                .add_elements([window.TimestampedValue(('k', 400), 7)])
                .advance_watermark_to_infinity()
                | beam.WindowInto(
                    window.FixedWindows(10),
                    timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST)
                | beam.CombinePerKey(sum))

      records = (result
                 | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))

      # All the KV pairs are applied GBK using LATEST timestamp for
      # the same key.
      expected_window_to_elements = {
          window.IntervalWindow(0, 10): [
              (('k', 500), Timestamp(7)),
          ],
      }

      assert_that(
          records,
          equal_to_per_window(expected_window_to_elements),
          use_global_window=False,
          label='assert per window')

if __name__ == '__main__':
  unittest.main()
