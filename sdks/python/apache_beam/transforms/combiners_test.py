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
from apache_beam.transforms import combiners
import apache_beam.transforms.combiners as combine
from apache_beam.transforms.core import CombineGlobally
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import Map
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.util import assert_that, equal_to


class CombineTest(unittest.TestCase):

  def test_builtin_combines(self):
    pipeline = Pipeline('DirectPipelineRunner')

    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    mean = sum(vals) / float(len(vals))
    size = len(vals)

    # First for global combines.
    pcoll = pipeline | Create('start', vals)
    result_mean = pcoll | combine.Mean.Globally('mean')
    result_count = pcoll | combine.Count.Globally('count')
    assert_that(result_mean, equal_to([mean]), label='assert:mean')
    assert_that(result_count, equal_to([size]), label='assert:size')

    # Again for per-key combines.
    pcoll = pipeline | Create('start-perkey', [('a', x) for x in vals])
    result_key_mean = pcoll | combine.Mean.PerKey('mean-perkey')
    result_key_count = pcoll | combine.Count.PerKey('count-perkey')
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
    pcoll = pipeline | Create('start', [6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | combine.Top.Largest('top', 5)
    result_bot = pcoll | combine.Top.Smallest('bot', 4)
    result_cmp = pcoll | combine.Top.Of(
        'cmp',
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')
    assert_that(result_cmp, equal_to([[9, 6, 6, 5, 3, 2]]), label='assert:cmp')

    # Again for per-key combines.
    pcoll = pipeline | Create(
        'start-perkey', [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_key_top = pcoll | combine.Top.LargestPerKey('top-perkey', 5)
    result_key_bot = pcoll | combine.Top.SmallestPerKey('bot-perkey', 4)
    result_key_cmp = pcoll | combine.Top.PerKey(
        'cmp-perkey',
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

  def test_top_shorthands(self):
    pipeline = Pipeline('DirectPipelineRunner')

    pcoll = pipeline | Create('start', [6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | beam.CombineGlobally('top', combiners.Largest(5))
    result_bot = pcoll | beam.CombineGlobally('bot', combiners.Smallest(4))
    assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
    assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')

    pcoll = pipeline | Create(
        'start-perkey', [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_ktop = pcoll | beam.CombinePerKey('top-perkey', combiners.Largest(5))
    result_kbot = pcoll | beam.CombinePerKey(
        'bot-perkey', combiners.Smallest(4))
    assert_that(result_ktop, equal_to([('a', [9, 6, 6, 5, 3])]), label='k:top')
    assert_that(result_kbot, equal_to([('a', [0, 1, 1, 1])]), label='k:bot')
    pipeline.run()

  def test_sample(self):

    # First test global samples (lots of them).
    for ix in xrange(300):
      pipeline = Pipeline('DirectPipelineRunner')
      pcoll = pipeline | Create('start', [1, 1, 2, 2])
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
    pcoll = pipeline | Create(
        'start-perkey',
        sum(([(i, 1), (i, 1), (i, 2), (i, 2)] for i in xrange(300)), []))
    result = pcoll | combine.Sample.FixedSizePerKey('sample', 3)

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
    pcoll = pipeline | Create('start', the_list)
    result = pcoll | combine.ToList('to list')

    def matcher(expected):
      def match(actual):
        equal_to(expected[0])(actual[0])
      return match
    assert_that(result, matcher([the_list]))
    pipeline.run()

    pipeline = Pipeline('DirectPipelineRunner')
    pairs = [(1, 2), (3, 4), (5, 6)]
    pcoll = pipeline | Create('start-pairs', pairs)
    result = pcoll | combine.ToDict('to dict')

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
    result1 = p | Create('label1', []) | CombineWithSideInput('L1')
    result2 = p | Create('label2', [1, 2, 3, 4]) | CombineWithSideInput('L2')
    assert_that(result1, equal_to([0]), label='r1')
    assert_that(result2, equal_to([10]), label='r2')
    p.run()


if __name__ == '__main__':
  unittest.main()
