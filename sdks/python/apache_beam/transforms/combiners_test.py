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

import unittest

import apache_beam as beam
from apache_beam.pipeline import Pipeline
import apache_beam.transforms.combiners as combine
from apache_beam.transforms.core import CombineGlobally
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import Map
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.util import assert_that, equal_to


class CombineTest(unittest.TestCase):

  def setUp(self):
    # Sort more often for more rigorous testing on small data sets.
    combine.TopCombineFn._MIN_BUFFER_OVERSIZE = 1

  def test_builtin_combines(self):
    pipeline = Pipeline('DirectPipelineRunner')

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
    pipeline = Pipeline('DirectPipelineRunner')

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
    result_top = pcoll | 'top' >> combine.Top.Largest(5)
    result_bot = pcoll | 'bot' >> combine.Top.Smallest(4)
    result_cmp = pcoll | 'cmp' >> combine.Top.Of(
        'cmp',
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    result_cmp_rev = pcoll | 'cmp_rev' >> combine.Top.Of(
        'cmp',
        3,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names,  # Note parameter passed to comparator.
        reverse=True)
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')
    assert_that(result_cmp, equal_to([[9, 6, 6, 5, 3, 2]]), label='assert:cmp')
    assert_that(result_cmp_rev, equal_to([[0, 1, 1]]), label='assert:cmp_rev')

    # Again for per-key combines.
    pcoll = pipeline | 'start-perkye' >> Create(
        [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_key_top = pcoll | 'top-perkey' >> combine.Top.LargestPerKey(5)
    result_key_bot = pcoll | 'bot-perkey' >> combine.Top.SmallestPerKey(4)
    result_key_cmp = pcoll | 'cmp-perkey' >> combine.Top.PerKey(
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    assert_that(result_key_top, equal_to([('a', [9, 6, 6, 5, 3])]),
                label='key:top')
    assert_that(result_key_bot, equal_to([('a', [0, 1, 1, 1])]),
                label='key:bot')
    assert_that(result_key_cmp, equal_to([('a', [9, 6, 6, 5, 3, 2])]),
                label='key:cmp')
    pipeline.run()

  def test_top_key(self):
    self.assertEqual(
        ['aa', 'bbb', 'c', 'dddd'] | combine.Top.Of(3, key=len),
        [['dddd', 'bbb', 'aa']])
    self.assertEqual(
        ['aa', 'bbb', 'c', 'dddd'] | combine.Top.Of(3, key=len, reverse=True),
        [['c', 'aa', 'bbb']])

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

  def test_top_shorthands(self):
    pipeline = Pipeline('DirectPipelineRunner')

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

  def test_sample(self):

    # First test global samples (lots of them).
    for ix in xrange(300):
      pipeline = Pipeline('DirectPipelineRunner')
      pcoll = pipeline | 'start' >> Create([1, 1, 2, 2])
      result = pcoll | combine.Sample.FixedSizeGlobally('sample-%d' % ix, 3)

      def matcher():
        def match(actual):
          # There is always exactly one result.
          equal_to([1])([len(actual)])
          # There are always exactly three samples in the result.
          equal_to([3])([len(actual[0])])
          # Sampling is without replacement.
          num_ones = sum(1 for x in actual[0] if x == 1)
          num_twos = sum(1 for x in actual[0] if x == 2)
          equal_to([1, 2])([num_ones, num_twos])
        return match
      assert_that(result, matcher())
      pipeline.run()

    # Now test per-key samples.
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start-perkey' >> Create(
        sum(([(i, 1), (i, 1), (i, 2), (i, 2)] for i in xrange(300)), []))
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
    p = Pipeline('DirectPipelineRunner')
    result = (
        p
        | Create([('a', 100, 0.0), ('b', 10, -1), ('c', 1, 100)])
        | beam.CombineGlobally(combine.TupleCombineFn(max,
                                                      combine.MeanCombineFn(),
                                                      sum)).without_defaults())
    assert_that(result, equal_to([('c', 111.0 / 3, 99.0)]))
    p.run()

  def test_tuple_combine_fn_without_defaults(self):
    p = Pipeline('DirectPipelineRunner')
    result = (
        p
        | Create([1, 1, 2, 3])
        | beam.CombineGlobally(
            combine.TupleCombineFn(min, combine.MeanCombineFn(), max)
            .with_common_input()).without_defaults())
    assert_that(result, equal_to([(1, 7.0 / 4, 3)]))
    p.run()

  def test_to_list_and_to_dict(self):
    pipeline = Pipeline('DirectPipelineRunner')
    the_list = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start' >> Create(the_list)
    result = pcoll | 'to list' >> combine.ToList()

    def matcher(expected):
      def match(actual):
        equal_to(expected[0])(actual[0])
      return match
    assert_that(result, matcher([the_list]))
    pipeline.run()

    pipeline = Pipeline('DirectPipelineRunner')
    pairs = [(1, 2), (3, 4), (5, 6)]
    pcoll = pipeline | 'start-pairs' >> Create(pairs)
    result = pcoll | 'to dict' >> combine.ToDict()

    def matcher():
      def match(actual):
        equal_to([1])([len(actual)])
        equal_to(pairs)(actual[0].iteritems())
      return match
    assert_that(result, matcher())
    pipeline.run()

  def test_combine_globally_with_default(self):
    p = Pipeline('DirectPipelineRunner')
    assert_that(p | Create([]) | CombineGlobally(sum), equal_to([0]))
    p.run()

  def test_combine_globally_without_default(self):
    p = Pipeline('DirectPipelineRunner')
    result = p | Create([]) | CombineGlobally(sum).without_defaults()
    assert_that(result, equal_to([]))
    p.run()

  def test_combine_globally_with_default_side_input(self):
    class CombineWithSideInput(PTransform):
      def apply(self, pcoll):
        side = pcoll | CombineGlobally(sum).as_singleton_view()
        main = pcoll.pipeline | Create([None])
        return main | Map(lambda _, s: s, side)

    p = Pipeline('DirectPipelineRunner')
    result1 = p | 'i1' >> Create([]) | 'c1' >> CombineWithSideInput()
    result2 = p | 'i2' >> Create([1, 2, 3, 4]) | 'c2' >> CombineWithSideInput()
    assert_that(result1, equal_to([0]), label='r1')
    assert_that(result2, equal_to([10]), label='r2')
    p.run()


if __name__ == '__main__':
  unittest.main()
